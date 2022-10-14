use super::load_chunk::EntryBatch;
use super::{MineLoadChunk, SealAnswer, SealTask};
use crate::error::Error;
use crate::log_store::log_manager::{bytes_to_entries, COL_ENTRY_BATCH, COL_ENTRY_BATCH_ROOT};
use crate::log_store::{FlowRead, FlowSeal, FlowWrite};
use crate::{try_option, IonianKeyValueDB};
use anyhow::{anyhow, bail, Result};
use ionian_spec::{BYTES_PER_SECTOR, SEALS_PER_LOADING, SECTORS_PER_LOADING, SECTORS_PER_SEAL};
use itertools::Itertools;
use shared_types::{ChunkArray, DataRoot};
use ssz::{Decode, Encode};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use std::cmp;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, trace};

pub struct FlowStore {
    db: FlowDBStore,
    // TODO(kevin): This is an in-memory cache for recording which chunks are ready for sealing. It should be persisted on disk.
    to_seal_set: BTreeMap<usize, usize>,
    // Data sealing is an asynchronized process.
    // The sealing service uses the version number to distinguish if revert happens during sealing.
    to_seal_version: usize,
    config: FlowConfig,
}

impl FlowStore {
    pub fn new(db: Arc<dyn IonianKeyValueDB>, config: FlowConfig) -> Self {
        Self {
            db: FlowDBStore::new(db),
            to_seal_set: Default::default(),
            to_seal_version: 0,
            config,
        }
    }

    pub fn put_batch_root(&self, batch_index: u64, root: DataRoot, length: usize) -> Result<()> {
        self.db.put_batch_root(batch_index, root, length)
    }
}

#[derive(Clone, Debug)]
pub struct FlowConfig {
    pub batch_size: usize,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            batch_size: SECTORS_PER_LOADING,
        }
    }
}

impl FlowRead for FlowStore {
    /// Return `Ok(None)` if only partial data are available.
    fn get_entries(&self, index_start: u64, index_end: u64) -> Result<Option<ChunkArray>> {
        if index_end <= index_start {
            bail!(
                "invalid entry index: start={} end={}",
                index_start,
                index_end
            );
        }
        let mut data = Vec::with_capacity((index_end - index_start) as usize * BYTES_PER_SECTOR);
        for (start_entry_index, end_entry_index) in
            batch_iter(index_start, index_end, self.config.batch_size)
        {
            let chunk_index = start_entry_index / self.config.batch_size as u64;
            let mut offset = start_entry_index - chunk_index * self.config.batch_size as u64;
            let mut length = end_entry_index - start_entry_index;

            // Tempfix: for first chunk, its offset is always 1
            if chunk_index == 0 && offset == 0 {
                offset = 1;
                length -= 1;
            }

            let entry_batch = try_option!(self.db.get_entry_batch(chunk_index)?);
            let mut entry_batch_data =
                try_option!(entry_batch.get_unsealed_data(offset as usize, length as usize));
            data.append(&mut entry_batch_data);
        }
        Ok(Some(ChunkArray {
            data,
            start_index: index_start,
        }))
    }

    fn get_available_entries(&self, index_start: u64, index_end: u64) -> Result<Vec<ChunkArray>> {
        // Both `index_start` and `index_end` are at the batch boundaries, so we do not need
        // to check if the data is within range when we process each batch.
        if index_end <= index_start
            || index_start % self.config.batch_size as u64 != 0
            || index_end % self.config.batch_size as u64 != 0
        {
            bail!(
                "invalid entry index: start={} end={}",
                index_start,
                index_end
            );
        }
        let mut entry_list = Vec::<ChunkArray>::new();
        for (start_entry_index, _) in batch_iter(index_start, index_end, self.config.batch_size) {
            let chunk_index = start_entry_index / self.config.batch_size as u64;

            if let Some(mut data_list) = self
                .db
                .get_entry_batch(chunk_index)?
                .map(|b| b.into_data_list(start_entry_index))
            {
                if data_list.is_empty() {
                    continue;
                }
                if let Some(last) = entry_list.last_mut() {
                    if last.start_index + bytes_to_entries(last.data.len() as u64)
                        == data_list[0].start_index
                    {
                        // Merge the first element with the previous one.
                        last.data.append(&mut data_list.remove(0).data);
                    }
                }
                for data in data_list {
                    entry_list.push(data);
                }
            }
        }
        Ok(entry_list)
    }

    /// Return the list of all stored chunk roots.
    fn get_chunk_root_list(&self) -> Result<Vec<(usize, DataRoot)>> {
        let mut chunk_roots = Vec::new();
        let mut i = 0;
        while let Some(root) = self.db.get_batch_root(i)? {
            let subtree = match root {
                BatchRoot::Single(r) => (1, r),
                BatchRoot::Multiple(t) => t,
            };
            chunk_roots.push(subtree);
            i += 1;
        }
        Ok(chunk_roots)
    }

    fn load_sealed_data(&self, chunk_index: u64) -> Result<Option<MineLoadChunk>> {
        let batch = try_option!(self.db.get_entry_batch(chunk_index as u64)?);
        let mut mine_chunk = MineLoadChunk::default();
        for (seal_index, (sealed, validity)) in mine_chunk
            .loaded_chunk
            .iter_mut()
            .zip(mine_chunk.avalibilities.iter_mut())
            .enumerate()
        {
            if let Some(data) = batch.get_sealed_data(seal_index as u16) {
                *validity = true;
                *sealed = data;
            }
        }
        Ok(Some(mine_chunk))
    }
}

impl FlowWrite for FlowStore {
    /// Return the roots of completed chunks. The order is guaranteed to be increasing
    /// by chunk index.
    fn append_entries(&mut self, data: ChunkArray) -> Result<Vec<(u64, DataRoot)>> {
        trace!("append_entries: {} {}", data.start_index, data.data.len());
        if data.data.len() % BYTES_PER_SECTOR != 0 {
            bail!("append_entries: invalid data size, len={}", data.data.len());
        }
        let mut batch_list = Vec::new();
        for (start_entry_index, end_entry_index) in batch_iter(
            data.start_index,
            data.start_index + bytes_to_entries(data.data.len() as u64),
            self.config.batch_size,
        ) {
            // TODO: Avoid mem-copy if possible.
            let chunk = data
                .sub_array(start_entry_index, end_entry_index)
                .expect("in range");

            let chunk_index = chunk.start_index / self.config.batch_size as u64;

            // TODO: Try to avoid loading from db if possible.
            let mut batch = self
                .db
                .get_entry_batch(chunk_index)?
                .unwrap_or_else(|| EntryBatch::new(chunk_index));
            let completed_seals = batch.insert_data(
                (chunk.start_index % self.config.batch_size as u64) as usize,
                chunk.data,
            )?;
            completed_seals.into_iter().for_each(|x| {
                self.to_seal_set.insert(
                    chunk_index as usize * SEALS_PER_LOADING + x as usize,
                    self.to_seal_version,
                );
            });

            batch_list.push((chunk_index, batch));
        }
        self.db.put_entry_batch_list(batch_list)
    }

    fn truncate(&mut self, start_index: u64) -> crate::error::Result<()> {
        self.db.truncate(start_index, self.config.batch_size)?;
        self.to_seal_set
            .split_off(&(start_index as usize / SECTORS_PER_SEAL));
        self.to_seal_version += 1;
        Ok(())
    }
}

impl FlowSeal for FlowStore {
    fn pull_seal_chunk(&self, seal_index_max: usize) -> Result<Option<Vec<SealTask>>> {
        let mut to_seal_iter = self.to_seal_set.iter();
        let (&first_index, &first_version) = try_option!(to_seal_iter.next());
        if first_index >= seal_index_max {
            return Ok(None);
        }

        let mut tasks = Vec::with_capacity(SEALS_PER_LOADING);

        let batch_data = self
            .db
            .get_entry_batch((first_index / SEALS_PER_LOADING) as u64)?
            .expect("Lost data chunk in to_seal_set");

        for (&seal_index, &version) in
            std::iter::once((&first_index, &first_version)).chain(to_seal_iter.filter(|(&x, _)| {
                first_index / SEALS_PER_LOADING == x / SEALS_PER_LOADING && x < seal_index_max
            }))
        {
            let seal_index_local = seal_index % SEALS_PER_LOADING;
            let non_sealed_data = batch_data
                .get_non_sealed_data(seal_index_local as u16)
                .expect("Lost seal chunk in to_seal_set");
            tasks.push(SealTask {
                seal_index: seal_index as u64,
                version,
                non_sealed_data,
            })
        }

        Ok(Some(tasks))
    }

    fn submit_seal_result(&mut self, answers: Vec<SealAnswer>) -> Result<()> {
        let is_consistent = |answer: &SealAnswer| {
            self.to_seal_set
                .get(&(answer.seal_index as usize))
                .map_or(false, |cur_ver| cur_ver == &answer.version)
        };

        let mut updated_chunk = vec![];
        let mut removed_seal_index = Vec::new();
        for (load_index, answers_in_chunk) in &answers
            .into_iter()
            .filter(is_consistent)
            .group_by(|answer| answer.seal_index / SEALS_PER_LOADING as u64)
        {
            let mut batch_chunk = self
                .db
                .get_entry_batch(load_index)?
                .expect("Can not find chunk data");
            for answer in answers_in_chunk {
                removed_seal_index.push(answer.seal_index as usize);
                batch_chunk.submit_seal_result(answer)?;
            }
            updated_chunk.push((load_index, batch_chunk));
        }

        debug!("Seal chunks: indices = {:?}", removed_seal_index);

        for idx in removed_seal_index.into_iter() {
            self.to_seal_set.remove(&idx);
        }

        self.db.put_entry_raw(updated_chunk)?;

        Ok(())
    }
}

pub struct FlowDBStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
}

impl FlowDBStore {
    pub fn new(kvdb: Arc<dyn IonianKeyValueDB>) -> Self {
        Self { kvdb }
    }

    fn put_entry_batch_list(
        &self,
        batch_list: Vec<(u64, EntryBatch)>,
    ) -> Result<Vec<(u64, DataRoot)>> {
        let mut completed_batches = Vec::new();
        let mut tx = self.kvdb.transaction();
        for (batch_index, batch) in batch_list {
            tx.put(
                COL_ENTRY_BATCH,
                &batch_index.to_be_bytes(),
                &batch.as_ssz_bytes(),
            );
            if let Some(root) = batch.build_root(batch_index == 0)? {
                tx.put(
                    COL_ENTRY_BATCH_ROOT,
                    &batch_index.to_be_bytes(),
                    &BatchRoot::Single(root).as_ssz_bytes(),
                );
                completed_batches.push((batch_index, root));
            }
        }
        self.kvdb.write(tx)?;
        Ok(completed_batches)
    }

    fn put_entry_raw(&self, batch_list: Vec<(u64, EntryBatch)>) -> Result<()> {
        let mut tx = self.kvdb.transaction();
        for (batch_index, batch) in batch_list {
            tx.put(
                COL_ENTRY_BATCH,
                &batch_index.to_be_bytes(),
                &batch.as_ssz_bytes(),
            );
        }
        self.kvdb.write(tx)?;
        Ok(())
    }

    fn get_entry_batch(&self, batch_index: u64) -> Result<Option<EntryBatch>> {
        let raw = try_option!(self.kvdb.get(COL_ENTRY_BATCH, &batch_index.to_be_bytes())?);
        Ok(Some(EntryBatch::from_ssz_bytes(&raw).map_err(Error::from)?))
    }

    pub fn put_batch_root(&self, batch_index: u64, root: DataRoot, length: usize) -> Result<()> {
        let root = if length == 1 {
            BatchRoot::Single(root)
        } else {
            BatchRoot::Multiple((length, root))
        };
        Ok(self.kvdb.put(
            COL_ENTRY_BATCH_ROOT,
            &batch_index.to_be_bytes(),
            &root.as_ssz_bytes(),
        )?)
    }

    fn get_batch_root(&self, batch_index: u64) -> Result<Option<BatchRoot>> {
        let raw = try_option!(self
            .kvdb
            .get(COL_ENTRY_BATCH_ROOT, &batch_index.to_be_bytes())?);
        Ok(Some(BatchRoot::from_ssz_bytes(&raw).map_err(Error::from)?))
    }

    fn truncate(&self, start_index: u64, batch_size: usize) -> crate::error::Result<()> {
        let mut tx = self.kvdb.transaction();
        let mut start_batch_index = start_index / batch_size as u64;
        let first_batch_offset = start_index as usize % batch_size;
        if first_batch_offset != 0 {
            if let Some(mut first_batch) = self.get_entry_batch(start_batch_index)? {
                first_batch.truncate(first_batch_offset as usize);
                tx.put(
                    COL_ENTRY_BATCH,
                    &start_batch_index.to_be_bytes(),
                    &first_batch.as_ssz_bytes(),
                );
            }

            start_batch_index += 1;
        }
        // TODO: `kvdb` and `kvdb-rocksdb` does not support `seek_to_last` yet.
        // We'll need to fork it or use another wrapper for a better performance in this.
        let end = match self.kvdb.iter(COL_ENTRY_BATCH).last() {
            Some((k, _)) => decode_batch_index(k.as_ref())?,
            None => {
                // The db has no data, so we can just return;
                return Ok(());
            }
        };
        for batch_index in start_batch_index..=end {
            tx.delete(COL_ENTRY_BATCH, &batch_index.to_be_bytes());
            tx.delete(COL_ENTRY_BATCH_ROOT, &batch_index.to_be_bytes());
        }
        self.kvdb.write(tx)?;
        Ok(())
    }
}

#[derive(DeriveEncode, DeriveDecode)]
#[ssz(enum_behaviour = "union")]
pub enum BatchRoot {
    Single(DataRoot),
    Multiple((usize, DataRoot)),
}

/// Return the batch boundaries `(batch_start_index, batch_end_index)` given the index range.
pub fn batch_iter(start: u64, end: u64, batch_size: usize) -> Vec<(u64, u64)> {
    let mut list = Vec::new();
    for i in (start / batch_size as u64 * batch_size as u64..end).step_by(batch_size) {
        let batch_start = cmp::max(start, i);
        let batch_end = cmp::min(end, i + batch_size as u64);
        list.push((batch_start, batch_end));
    }
    list
}

fn decode_batch_index(data: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        data.try_into().map_err(|e| anyhow!("{:?}", e))?,
    ))
}
