use anyhow::{bail, Result};
use ionian_spec::{BYTES_PER_LOAD, BYTES_PER_SECTOR, SECTORS_PER_LOAD, SECTORS_PER_SEAL};
use shared_types::bytes_to_chunks;
use std::fmt::{Debug, Formatter};
use std::mem;

pub enum EntryBatchData {
    Complete(Vec<u8>),
    /// All `PartialBatch`s are ordered based on `start_index`.
    Incomplete(Vec<PartialBatch>),
}
#[derive(PartialEq, Eq)]
pub struct PartialBatch {
    /// Offset in this batch.
    pub(super) start_sector: usize,
    pub(super) data: Vec<u8>,
}

impl Debug for PartialBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartialBatch: start_offset={} data_len={}",
            self.start_sector,
            self.data.len()
        )
    }
}

impl PartialBatch {
    fn end_sector(&self) -> usize {
        self.start_sector + bytes_to_chunks(self.data.len())
    }

    fn find(data_list: &[PartialBatch], sector_index: usize) -> Result<usize, usize> {
        let possible_index = match data_list.binary_search_by_key(&sector_index, |x| x.start_sector)
        {
            Ok(x) => x,
            Err(0) => {
                return Err(0);
            }
            Err(x) => x - 1,
        };
        if sector_index < data_list[possible_index].end_sector() {
            Ok(possible_index)
        } else {
            Err(possible_index + 1)
        }
    }
}

impl EntryBatchData {
    pub fn new() -> Self {
        EntryBatchData::Incomplete(vec![])
    }

    pub fn is_empty(&self) -> bool {
        matches!(self,EntryBatchData::Incomplete(x) if x.is_empty())
    }

    pub fn get(&self, mut start_byte: usize, length_byte: usize) -> Option<&[u8]> {
        assert!(start_byte + length_byte <= BYTES_PER_LOAD);

        match self {
            EntryBatchData::Complete(data) => data.get(start_byte..(start_byte + length_byte)),
            EntryBatchData::Incomplete(data_list) => {
                let p = &data_list
                    [PartialBatch::find(data_list, start_byte / BYTES_PER_SECTOR).ok()?];

                // Rebase the start_byte and end_byte w.r.t. to hit partial batch.
                start_byte -= p.start_sector * BYTES_PER_SECTOR;

                p.data.get(start_byte..(start_byte + length_byte))
            }
        }
    }

    pub fn get_mut(&mut self, mut start_byte: usize, length_byte: usize) -> Option<&mut [u8]> {
        assert!(start_byte + length_byte <= BYTES_PER_LOAD);

        match self {
            EntryBatchData::Complete(data) => data.get_mut(start_byte..(start_byte + length_byte)),
            EntryBatchData::Incomplete(data_list) => {
                let index = PartialBatch::find(&*data_list, start_byte / BYTES_PER_SECTOR).ok()?;
                let p = &mut data_list[index];

                // Rebase the start_byte and end_byte w.r.t. to hit partial batch.
                start_byte -= p.start_sector * BYTES_PER_SECTOR;

                p.data.get_mut(start_byte..(start_byte + length_byte))
            }
        }
    }

    pub fn truncate(&mut self, mut truncated_byte: usize) {
        assert!(truncated_byte % BYTES_PER_SECTOR == 0);
        *self = match self {
            EntryBatchData::Complete(data) => {
                data.truncate(truncated_byte);
                EntryBatchData::Incomplete(vec![PartialBatch {
                    start_sector: 0,
                    data: std::mem::take(data),
                }])
            }
            EntryBatchData::Incomplete(batch_list) => {
                let partial_batch_truncate =
                    match PartialBatch::find(batch_list, truncated_byte / BYTES_PER_SECTOR) {
                        Ok(x) => {
                            let p = &mut batch_list[x];
                            truncated_byte -= p.start_sector * BYTES_PER_SECTOR;
                            p.data.truncate(truncated_byte);
                            if p.data.is_empty() {
                                x
                            } else {
                                x + 1
                            }
                        }
                        Err(x) => x,
                    };
                batch_list.truncate(partial_batch_truncate);

                EntryBatchData::Incomplete(std::mem::take(batch_list))
            }
        };
    }

    pub fn insert_data(&mut self, start_byte: usize, mut data: Vec<u8>) -> Result<Vec<u16>> {
        assert!(start_byte % BYTES_PER_SECTOR == 0);
        assert!(data.len() % BYTES_PER_SECTOR == 0);

        if data.is_empty() {
            return Ok(vec![]);
        }

        // Check if the entry is completed
        let list = if let EntryBatchData::Incomplete(list) = self {
            list
        } else {
            bail!("overwriting a completed PoRA Chunk with partial data");
        };

        let start_sector = start_byte / BYTES_PER_SECTOR;
        let end_sector = start_sector + data.len() / BYTES_PER_SECTOR;
        let length_sector = data.len() / BYTES_PER_SECTOR;

        // Check if the entry is completed
        let start_insert_position = match PartialBatch::find(list, start_sector) {
            Ok(x) => {
                bail!(
                    "start position overlapped with existing batch: start {}, len {}",
                    list[x].start_sector,
                    list[x].data.len()
                );
            }
            Err(x) => x,
        };

        let end_insert_position = match PartialBatch::find(list, end_sector - 1) {
            Ok(x) => {
                bail!(
                    "end position overlapped with existing batch: start {}, len {}",
                    list[x].start_sector,
                    list[x].data.len()
                );
            }
            Err(x) => x,
        };

        let position = if start_insert_position != end_insert_position {
            bail!("data overlapped with existing batches");
        } else {
            start_insert_position
        };

        let merge_prev = position != 0 && start_sector == list[position - 1].end_sector();
        let merge_next = position != list.len() && end_sector == list[position].start_sector;

        let updated_segment = match (merge_prev, merge_next) {
            (false, false) => {
                list.insert(position, PartialBatch { start_sector, data });
                &list[position]
            }
            (true, false) => {
                list[position - 1].data.append(&mut data);
                &list[position - 1]
            }
            (false, true) => {
                data.append(&mut list[position].data);
                list[position] = PartialBatch { start_sector, data };
                &list[position]
            }
            (true, true) => {
                // Merge the new data with the two around partial batches to
                // a single one.
                list[position - 1].data.append(&mut data);
                let mut next = list.remove(position);
                list[position - 1].data.append(&mut next.data);
                &list[position - 1]
            }
        };

        // Find which seal chunks are made intact by this submission.
        // It will be notified to the sealer later.
        let intact_seal_idxs = get_intact_sealing_index(
            updated_segment.start_sector,
            updated_segment.data.len() / BYTES_PER_SECTOR,
        );

        // TODO(zz): Use config here?
        if list.len() == 1
            && list[0].start_sector == 0
            && bytes_to_chunks(list[0].data.len()) == SECTORS_PER_LOAD
        {
            // All data in this batch have been filled.
            *self = EntryBatchData::Complete(mem::take(&mut list[0].data));
        }

        let ready_for_seal_idxs: Vec<u16> = get_covered_sealing_index(start_sector, length_sector)
            .filter(|x| intact_seal_idxs.contains(x))
            .collect();

        Ok(ready_for_seal_idxs)
    }

    pub(super) fn available_range_entries(&self) -> Vec<(usize, usize)> {
        match self {
            EntryBatchData::Complete(data) => {
                vec![(0, data.len() / BYTES_PER_SECTOR)]
            }
            EntryBatchData::Incomplete(batch_list) => batch_list
                .iter()
                .map(|b| (b.start_sector, b.data.len() / BYTES_PER_SECTOR))
                .collect(),
        }
    }
}

fn get_intact_sealing_index(start_sector: usize, length_sector: usize) -> std::ops::Range<u16> {
    // Inclusive
    let start_index = ((start_sector + SECTORS_PER_SEAL - 1) / SECTORS_PER_SEAL) as u16;
    // Exclusive
    let end_index = ((start_sector + length_sector) / SECTORS_PER_SEAL) as u16;
    start_index..end_index
}

fn get_covered_sealing_index(start_sector: usize, length_sector: usize) -> std::ops::Range<u16> {
    // Inclusive
    let start_index = (start_sector / SECTORS_PER_SEAL) as u16;
    // Exclusive
    let end_index =
        ((start_sector + length_sector + SECTORS_PER_SEAL - 1) / SECTORS_PER_SEAL) as u16;
    start_index..end_index
}

#[cfg(test)]
mod tests {
    use crate::log_store::load_chunk::chunk_data::PartialBatch;

    use super::EntryBatchData;
    use ionian_spec::{BYTES_PER_LOAD, BYTES_PER_SECTOR, SECTORS_PER_LOAD};
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    fn test_data() -> Vec<u8> {
        let mut data = vec![0u8; BYTES_PER_LOAD];
        let mut random = StdRng::seed_from_u64(73);
        random.fill_bytes(&mut data);
        data
    }

    #[test]
    fn test_data_chunk_insert() {
        let data = test_data();
        let mut chunk_batch = EntryBatchData::new();

        for i in [2usize, 0, 1, 3].into_iter() {
            chunk_batch
                .insert_data(
                    BYTES_PER_LOAD / 4 * i,
                    data[(BYTES_PER_LOAD / 4) * i..(BYTES_PER_LOAD / 4) * (i + 1)].to_vec(),
                )
                .unwrap();
        }

        assert!(matches!(chunk_batch, EntryBatchData::Complete(_)));
    }

    #[test]
    fn test_data_chunk_truncate() {
        let data = test_data();
        let mut chunk_batch = EntryBatchData::new();

        for i in [3, 1].into_iter() {
            chunk_batch
                .insert_data(
                    BYTES_PER_LOAD / 4 * i,
                    data[(BYTES_PER_LOAD / 4) * i..(BYTES_PER_LOAD / 4) * (i + 1)].to_vec(),
                )
                .unwrap();
        }

        chunk_batch.truncate(BYTES_PER_LOAD / 4 * 3 + BYTES_PER_SECTOR);

        let chunks = if let EntryBatchData::Incomplete(chunks) = chunk_batch {
            chunks
        } else {
            unreachable!();
        };

        assert!(chunks.len() == 2);
        assert!(
            chunks[0]
                == PartialBatch {
                    start_sector: SECTORS_PER_LOAD / 4,
                    data: data[(BYTES_PER_LOAD / 4) * 1..(BYTES_PER_LOAD / 4) * 2].to_vec()
                }
        );
        assert!(
            chunks[1]
                == PartialBatch {
                    start_sector: SECTORS_PER_LOAD / 4 * 3,
                    data: data[BYTES_PER_LOAD / 4 * 3..BYTES_PER_LOAD / 4 * 3 + BYTES_PER_SECTOR]
                        .to_vec()
                }
        );
    }

    #[test]
    fn test_data_chunk_get_slice() {
        let data = test_data();
        let mut chunk_batch = EntryBatchData::new();

        const N: usize = BYTES_PER_LOAD;
        const B: usize = N / 16;

        // Skip batch 5,7,10,11
        for i in [3, 8, 12, 15, 6, 1, 4, 13, 0, 2, 9, 14].into_iter() {
            chunk_batch
                .insert_data(B * i, data[B * i..B * (i + 1)].to_vec())
                .unwrap();
            assert_eq!(
                chunk_batch.get(B * i, B).unwrap(),
                &data[B * i..B * (i + 1)]
            );
            assert_eq!(
                chunk_batch.get_mut(B * i, B).unwrap(),
                &data[B * i..B * (i + 1)]
            );
        }

        const S: usize = B / BYTES_PER_SECTOR;
        assert_eq!(
            chunk_batch.available_range_entries(),
            vec![(0, 5 * S), (6 * S, S), (8 * S, 2 * S), (12 * S, 4 * S)]
        );

        assert_eq!(chunk_batch.get(B * 8, B * 2).unwrap(), &data[B * 8..B * 10]);
        assert_eq!(
            chunk_batch.get_mut(B * 8, B * 2).unwrap(),
            &data[B * 8..B * 10]
        );

        assert_eq!(chunk_batch.get(0, B * 4).unwrap(), &data[0..B * 4]);
        assert_eq!(chunk_batch.get_mut(0, B * 4).unwrap(), &data[0..B * 4]);

        assert!(chunk_batch.get(0, B * 5 + 32).is_none());
        assert!(chunk_batch.get_mut(0, B * 5 + 32).is_none());

        assert!(chunk_batch.get(B * 7 - 32, B + 32).is_none());
        assert!(chunk_batch.get_mut(B * 7 - 32, B + 32).is_none());

        assert!(chunk_batch.get(B * 7, B + 32).is_none());
        assert!(chunk_batch.get_mut(B * 7, B + 32).is_none());

        assert!(chunk_batch.get(B * 12 - 32, B + 32).is_none());
        assert!(chunk_batch.get_mut(B * 12 - 32, B + 32).is_none());
    }
}
