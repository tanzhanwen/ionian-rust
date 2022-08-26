use crate::error::Error;
use crate::log_store::log_manager::{
    bytes_to_entries, data_to_merkle_leaves, sub_merkle_tree, COL_ENTRY_BATCH,
    COL_ENTRY_BATCH_ROOT, ENTRY_SIZE, PORA_CHUNK_SIZE,
};
use crate::log_store::{FlowRead, FlowWrite};
use crate::{try_option, IonianKeyValueDB};
use anyhow::{bail, Result};
use append_merkle::{AppendMerkleTree, Sha3Algorithm};
use ethereum_types::H256;
use shared_types::{bytes_to_chunks, ChunkArray, DataRoot};
use ssz::{Decode, DecodeError, Encode};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use std::sync::Arc;
use std::{cmp, mem};

pub struct FlowStore {
    db: FlowDBStore,
    config: FlowConfig,
}

impl FlowStore {
    pub fn new(db: Arc<dyn IonianKeyValueDB>, config: FlowConfig) -> Self {
        Self {
            db: FlowDBStore::new(db),
            config,
        }
    }

    /// Return the data from `index_start` until data missing or `index_end`.
    pub fn get_entries_to_end(&self, index_start: u64, index_end: u64) -> Result<ChunkArray> {
        if index_end <= index_start {
            bail!(
                "invalid entry index: start={} end={}",
                index_start,
                index_end
            );
        }
        let mut data = Vec::with_capacity((index_end - index_start) as usize * ENTRY_SIZE);
        for (start_entry_index, end_entry_index) in
            batch_iter(index_start, index_end, self.config.batch_size)
        {
            let chunk_index = start_entry_index / self.config.batch_size as u64;
            let mut batch_data = match self.db.get_entry_batch(chunk_index)? {
                Some(EntryBatch::Complete(d)) => d,
                Some(EntryBatch::Incomplete(mut batch_list)) => {
                    if batch_list.is_empty() {
                        bail!(
                            "Unexpected error, empty entry batch: batch_index={}",
                            chunk_index
                        );
                    }
                    if batch_list[0].start_offset == 0 {
                        batch_list.swap_remove(0).data
                    } else {
                        vec![]
                    }
                }
                None => vec![],
            };
            let batch_len = batch_data.len() / ENTRY_SIZE;
            if end_entry_index != index_end {
                data.append(&mut batch_data);
                // data supposed to be full
                if batch_len != PORA_CHUNK_SIZE {
                    break;
                }
            } else if start_entry_index + batch_len as u64 >= end_entry_index {
                let end_offset = (end_entry_index - start_entry_index) as usize * ENTRY_SIZE;
                data.extend_from_slice(&batch_data[..end_offset]);
            } else {
                // data missing.
                data.append(&mut batch_data);
            }
        }
        Ok(ChunkArray {
            data,
            start_index: index_start,
        })
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
            batch_size: PORA_CHUNK_SIZE,
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
        let mut data = Vec::with_capacity((index_end - index_start) as usize * ENTRY_SIZE);
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

            data.append(&mut try_option!(try_option!(self
                .db
                .get_entry_batch(chunk_index)?)
            .get_data(offset as usize, length as usize)));
        }
        Ok(Some(ChunkArray {
            data,
            start_index: index_start,
        }))
    }

    /// Return the list of all stored chunk roots.
    fn get_chunk_root_list(&self) -> Result<Vec<DataRoot>> {
        let mut chunk_roots = Vec::new();
        let mut i = 0;
        // FIXME(zz): Process the root of a range of chunks.
        while let Some(BatchRoot::Single(root)) = self.db.get_batch_root(i)? {
            chunk_roots.push(root);
            i += 1;
        }
        Ok(chunk_roots)
    }
}

impl FlowWrite for FlowStore {
    /// Return the roots of completed chunks. The order is guaranteed to be increasing
    /// by chunk index.
    fn append_entries(&self, data: ChunkArray) -> Result<Vec<(u64, DataRoot)>> {
        if data.data.len() % ENTRY_SIZE != 0 {
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
            let batch = if chunk.data.len() != self.config.batch_size * ENTRY_SIZE {
                // We are writing partial data.
                // TODO: Try to avoid loading from db if possible.
                match self.db.get_entry_batch(chunk_index)? {
                    None => {
                        // no data in db, so just store the new data.
                        EntryBatch::Incomplete(vec![PartialBatch {
                            start_offset: (chunk.start_index % self.config.batch_size as u64)
                                as usize,
                            data: chunk.data,
                        }])
                    }
                    Some(mut data_in_db) => {
                        data_in_db.insert_data(
                            (chunk.start_index % self.config.batch_size as u64) as usize,
                            chunk.data,
                        )?;
                        data_in_db
                    }
                }
            } else {
                EntryBatch::Complete(chunk.data)
            };
            batch_list.push((chunk_index, batch));
        }
        self.db.put_entry_batch_list(batch_list)
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
        for (batch_index, data) in batch_list {
            tx.put(
                COL_ENTRY_BATCH,
                &batch_index.to_be_bytes(),
                &data.as_ssz_bytes(),
            );
            if batch_index == 0 {
                // Special case because the first entry hash is initialized as 0.
                match data {
                    EntryBatch::Complete(_) => {
                        bail!("Unexpected first batch");
                    }
                    EntryBatch::Incomplete(p) => {
                        if p.len() == 1
                            && p[0].start_offset == 1
                            && p[0].data.len() == ENTRY_SIZE * (PORA_CHUNK_SIZE - 1)
                        {
                            let mut leaves = vec![H256::zero()];
                            leaves.append(&mut data_to_merkle_leaves(&p[0].data)?);
                            let root = *AppendMerkleTree::<H256, Sha3Algorithm>::new(leaves).root();
                            tx.put(
                                COL_ENTRY_BATCH_ROOT,
                                &batch_index.to_be_bytes(),
                                &BatchRoot::Single(root).as_ssz_bytes(),
                            );
                            completed_batches.push((batch_index, root));
                        }
                    }
                }
            } else if let EntryBatch::Complete(raw_data) = &data {
                // TODO(zz): Check if we want to insert here.
                assert_eq!(raw_data.len(), ENTRY_SIZE * PORA_CHUNK_SIZE);
                let root: DataRoot = sub_merkle_tree(raw_data.as_slice())?.root().into();
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

    fn get_entry_batch(&self, batch_index: u64) -> Result<Option<EntryBatch>> {
        let raw = try_option!(self.kvdb.get(COL_ENTRY_BATCH, &batch_index.to_be_bytes())?);
        Ok(Some(EntryBatch::from_ssz_bytes(&raw).map_err(Error::from)?))
    }

    pub fn put_batch_root(&self, batch_index: u64, root: DataRoot, length: usize) -> Result<()> {
        let root = if length == 1 {
            BatchRoot::Single(root)
        } else {
            BatchRoot::Multiple((root, length))
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
}

enum EntryBatch {
    Complete(Vec<u8>),
    /// All `PartialBatch`s are ordered based on `start_index`.
    Incomplete(Vec<PartialBatch>),
}

const COMPLETE_BATCH_TYPE: u8 = 0;
const INCOMPLETE_BATCH_TYPE: u8 = 1;

impl Encode for EntryBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        match &self {
            EntryBatch::Complete(data) => {
                buf.extend_from_slice(&[COMPLETE_BATCH_TYPE]);
                buf.extend_from_slice(data.as_slice());
            }
            EntryBatch::Incomplete(data_list) => {
                buf.extend_from_slice(&[INCOMPLETE_BATCH_TYPE]);
                buf.extend_from_slice(&data_list.as_ssz_bytes());
            }
        }
    }

    fn ssz_bytes_len(&self) -> usize {
        match &self {
            EntryBatch::Complete(data) => 1 + data.len(),
            EntryBatch::Incomplete(batch_list) => 1 + batch_list.ssz_bytes_len(),
        }
    }
}

impl Decode for EntryBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        match *bytes.first().ok_or(DecodeError::ZeroLengthItem)? {
            COMPLETE_BATCH_TYPE => Ok(EntryBatch::Complete(bytes[1..].to_vec())),
            INCOMPLETE_BATCH_TYPE => Ok(EntryBatch::Incomplete(
                <Vec<PartialBatch> as Decode>::from_ssz_bytes(&bytes[1..])?,
            )),
            _ => unreachable!(),
        }
    }
}

#[derive(DeriveEncode, DeriveDecode)]
#[ssz(enum_behaviour = "union")]
pub enum BatchRoot {
    Single(DataRoot),
    Multiple((DataRoot, usize)),
}

struct PartialBatch {
    /// Offset in this batch.
    start_offset: usize,
    data: Vec<u8>,
}

impl Encode for PartialBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.start_offset.to_be_bytes());
        buf.extend_from_slice(&self.data);
    }

    fn ssz_bytes_len(&self) -> usize {
        1 + self.data.len()
    }
}

impl Decode for PartialBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        Ok(Self {
            start_offset: usize::from_be_bytes(
                bytes[..mem::size_of::<usize>()].try_into().unwrap(),
            ),
            data: bytes[mem::size_of::<usize>()..].to_vec(),
        })
    }
}

impl PartialBatch {
    fn end_offset(&self) -> usize {
        self.start_offset + bytes_to_chunks(self.data.len())
    }
}

impl EntryBatch {
    fn get_data(&self, offset: usize, length: usize) -> Option<Vec<u8>> {
        match self {
            EntryBatch::Complete(data) => data
                .get(offset * ENTRY_SIZE..(offset + length) * ENTRY_SIZE)
                .map(|s| s.to_vec()),
            EntryBatch::Incomplete(data_list) => {
                for p in data_list {
                    if offset >= p.start_offset
                        && offset + length <= p.start_offset + bytes_to_chunks(p.data.len())
                    {
                        return p
                            .data
                            .get(
                                (offset - p.start_offset) * ENTRY_SIZE
                                    ..(offset - p.start_offset + length) * ENTRY_SIZE,
                            )
                            .map(|s| s.to_vec());
                    }
                }
                None
            }
        }
    }

    /// Return `Error` if the new data overlaps with old data.
    /// Convert `Incomplete` to `Completed` if the chunk is completed after the insertion.
    fn insert_data(&mut self, offset: usize, mut data: Vec<u8>) -> Result<()> {
        match self {
            EntryBatch::Complete(_) => {
                bail!("overwriting a completed PoRA Chunk with partial data");
            }
            EntryBatch::Incomplete(list) => {
                let data_entry_len = bytes_to_chunks(data.len());
                match list.binary_search_by_key(&offset, |p| p.start_offset) {
                    Ok(i) => {
                        bail!(
                            "same offset with a PartialBatch at index {}: offset={}",
                            i,
                            offset
                        );
                    }
                    Err(position) => {
                        if position != 0 && offset < list[position - 1].end_offset() {
                            bail!(
                                "Overlap with index {}: end_offset={} new_offset={}",
                                position - 1,
                                list[position - 1].end_offset(),
                                offset
                            );
                        }
                        if position != list.len()
                            && offset + data_entry_len > list[position].start_offset
                        {
                            bail!(
                                "Overlap with index{}: start_offset={} new_end_offset={}",
                                position,
                                list[position].start_offset,
                                offset + data_entry_len
                            );
                        }
                        let merge_prev = position != 0 && offset == list[position - 1].end_offset();
                        let merge_next = position != list.len()
                            && offset + data_entry_len == list[position].start_offset;
                        match (merge_prev, merge_next) {
                            (false, false) => {
                                list.insert(
                                    position,
                                    PartialBatch {
                                        start_offset: offset,
                                        data,
                                    },
                                );
                            }
                            (true, false) => {
                                list[position - 1].data.append(&mut data);
                            }
                            (false, true) => {
                                data.append(&mut list[position].data);
                                list[position] = PartialBatch {
                                    start_offset: offset,
                                    data,
                                };
                            }
                            (true, true) => {
                                // Merge the new data with the two around partial batches to
                                // a single one.
                                list[position - 1].data.append(&mut data);
                                let mut next = list.remove(position);
                                list[position - 1].data.append(&mut next.data);
                            }
                        }
                        // TODO(zz): Use config here?
                        if list.len() == 1
                            && list[0].start_offset == 0
                            && bytes_to_chunks(list[0].data.len()) == PORA_CHUNK_SIZE
                        {
                            // All data in this batch have been filled.
                            *self = EntryBatch::Complete(list.remove(0).data);
                        }
                        Ok(())
                    }
                }
            }
        }
    }
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
