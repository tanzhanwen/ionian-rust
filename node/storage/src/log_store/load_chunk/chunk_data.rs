use anyhow::{bail, Result};
use ionian_spec::{BYTES_PER_LOADING, BYTES_PER_SECTOR, SECTORS_PER_LOADING, SECTORS_PER_SEAL};
use shared_types::bytes_to_chunks;
use ssz::{Decode, DecodeError, Encode};
use std::fmt::{Debug, Formatter};
use std::mem;

pub enum EntryBatchData {
    Complete(Vec<u8>),
    /// All `PartialBatch`s are ordered based on `start_index`.
    Incomplete(Vec<PartialBatch>),
}

const COMPLETE_BATCH_TYPE: u8 = 0;
const INCOMPLETE_BATCH_TYPE: u8 = 1;

impl Encode for EntryBatchData {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        match &self {
            EntryBatchData::Complete(data) => {
                buf.extend_from_slice(&[COMPLETE_BATCH_TYPE]);
                buf.extend_from_slice(data.as_slice());
            }
            EntryBatchData::Incomplete(data_list) => {
                buf.extend_from_slice(&[INCOMPLETE_BATCH_TYPE]);
                buf.extend_from_slice(&data_list.as_ssz_bytes());
            }
        }
    }

    fn ssz_bytes_len(&self) -> usize {
        match &self {
            EntryBatchData::Complete(data) => 1 + data.len(),
            EntryBatchData::Incomplete(batch_list) => 1 + batch_list.ssz_bytes_len(),
        }
    }
}

impl Decode for EntryBatchData {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> std::result::Result<Self, DecodeError> {
        match *bytes.first().ok_or(DecodeError::ZeroLengthItem)? {
            COMPLETE_BATCH_TYPE => Ok(EntryBatchData::Complete(bytes[1..].to_vec())),
            INCOMPLETE_BATCH_TYPE => Ok(EntryBatchData::Incomplete(
                <Vec<PartialBatch> as Decode>::from_ssz_bytes(&bytes[1..])?,
            )),
            unknown => Err(DecodeError::BytesInvalid(format!(
                "Unrecognized EntryBatchData indentifier {}",
                unknown
            ))),
        }
    }
}

#[derive(PartialEq, Eq)]
pub struct PartialBatch {
    /// Offset in this batch.
    start_sector: usize,
    data: Vec<u8>,
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

impl Encode for PartialBatch {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.start_sector.to_be_bytes());
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
            start_sector: usize::from_be_bytes(
                bytes[..mem::size_of::<usize>()].try_into().unwrap(),
            ),
            data: bytes[mem::size_of::<usize>()..].to_vec(),
        })
    }
}

impl PartialBatch {
    fn end_offset(&self) -> usize {
        self.start_sector + bytes_to_chunks(self.data.len())
    }
}

impl EntryBatchData {
    pub fn new() -> Self {
        EntryBatchData::Incomplete(vec![])
    }

    pub fn get(&self, mut start_byte: usize, length_byte: usize) -> Option<&[u8]> {
        assert!(start_byte + length_byte <= BYTES_PER_LOADING);

        match self {
            EntryBatchData::Complete(data) => data.get(start_byte..(start_byte + length_byte)),
            EntryBatchData::Incomplete(data_list) => {
                let p = match data_list
                    .binary_search_by_key(&(start_byte / BYTES_PER_SECTOR), |x| x.start_sector)
                {
                    Ok(x) => &data_list[x],
                    Err(x) if x >= 1 => &data_list[x - 1],
                    _ => {
                        return None;
                    }
                };

                // Rebase the start_byte and end_byte w.r.t. to hit partial batch.
                start_byte -= p.start_sector * BYTES_PER_SECTOR;

                p.data.get(start_byte..(start_byte + length_byte))
            }
        }
    }

    pub fn get_mut(&mut self, mut start_byte: usize, length_byte: usize) -> Option<&mut [u8]> {
        assert!(start_byte + length_byte <= BYTES_PER_LOADING);

        match self {
            EntryBatchData::Complete(data) => data.get_mut(start_byte..(start_byte + length_byte)),
            EntryBatchData::Incomplete(data_list) => {
                let p = match data_list
                    .binary_search_by_key(&(start_byte / BYTES_PER_SECTOR), |x| x.start_sector)
                {
                    Ok(x) => &mut data_list[x],
                    Err(x) if x >= 1 => &mut data_list[x - 1],
                    _ => {
                        return None;
                    }
                };

                // Rebase the start_byte and end_byte w.r.t. to hit partial batch.
                start_byte -= p.start_sector * BYTES_PER_SECTOR;

                p.data.get_mut(start_byte..(start_byte + length_byte))
            }
        }
    }

    pub fn truncate(&mut self, start: usize) {
        assert!(start % BYTES_PER_SECTOR == 0);
        *self = match self {
            EntryBatchData::Complete(data) => {
                data.truncate(start);
                EntryBatchData::Incomplete(vec![PartialBatch {
                    start_sector: 0,
                    data: std::mem::take(data),
                }])
            }
            EntryBatchData::Incomplete(batch_list) => {
                let mut start_partial_batch_index = None;
                for (i, b) in batch_list.iter_mut().enumerate() {
                    if b.start_sector * BYTES_PER_SECTOR >= start {
                        // All partial chunks after (including) i should be removed;
                        start_partial_batch_index = Some(i);
                        break;
                    } else if b.start_sector + bytes_to_chunks(b.data.len())
                        > start / BYTES_PER_SECTOR
                    {
                        start_partial_batch_index = Some(i + 1);
                        b.data.truncate(start - b.start_sector * BYTES_PER_SECTOR);
                        break;
                    }
                }
                if let Some(start_index) = start_partial_batch_index {
                    batch_list.truncate(start_index);
                }

                EntryBatchData::Incomplete(std::mem::take(batch_list))
            }
        };
    }

    pub fn insert_data(&mut self, start_byte: usize, mut data: Vec<u8>) -> Result<Vec<u16>> {
        assert!(start_byte % BYTES_PER_SECTOR == 0);
        let start_entry = start_byte / BYTES_PER_SECTOR;

        let insert_length_in_sectors = data.len() / SECTORS_PER_SEAL;
        // Check if the entry is completed
        let list = if let EntryBatchData::Incomplete(list) = self {
            list
        } else {
            bail!("overwriting a completed PoRA Chunk with partial data");
        };

        // Check if the entry is completed
        let position = match list.binary_search_by_key(&start_entry, |p| p.start_sector) {
            Ok(i) => {
                bail!(
                    "same offset with a PartialBatch at index {}: offset={}",
                    i,
                    start_entry
                );
            }
            Err(position) => position,
        };

        // Check if overlapped with the previous item
        if position != 0 && start_entry < list[position - 1].end_offset() {
            bail!(
                "Overlap with index {}: end_offset={} new_offset={}",
                position - 1,
                list[position - 1].end_offset(),
                start_entry
            );
        }

        let data_entry_len = bytes_to_chunks(data.len());

        // Check if overlapped with the next item
        if position != list.len() && start_entry + data_entry_len > list[position].start_sector {
            bail!(
                "Overlap with index{}: start_offset={} new_end_offset={}",
                position,
                list[position].start_sector,
                start_entry + data_entry_len
            );
        }

        let merge_prev = position != 0 && start_entry == list[position - 1].end_offset();
        let merge_next =
            position != list.len() && start_entry + data_entry_len == list[position].start_sector;

        let updated_segment = match (merge_prev, merge_next) {
            (false, false) => {
                list.insert(
                    position,
                    PartialBatch {
                        start_sector: start_entry,
                        data,
                    },
                );
                &list[position]
            }
            (true, false) => {
                list[position - 1].data.append(&mut data);
                &list[position - 1]
            }
            (false, true) => {
                data.append(&mut list[position].data);
                list[position] = PartialBatch {
                    start_sector: start_entry,
                    data,
                };
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

        let ready_for_seal_idxs: Vec<u16> =
            get_covered_sealing_index(start_entry, insert_length_in_sectors)
                .filter(|x| intact_seal_idxs.contains(x))
                .collect();

        // TODO(zz): Use config here?
        if list.len() == 1
            && list[0].start_sector == 0
            && bytes_to_chunks(list[0].data.len()) == SECTORS_PER_LOADING
        {
            // All data in this batch have been filled.
            *self = EntryBatchData::Complete(list.remove(0).data);
        }
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

fn get_intact_sealing_index(start_sector: usize, length: usize) -> std::ops::Range<u16> {
    // Inclusive
    let start_index = ((start_sector + SECTORS_PER_SEAL - 1) / SECTORS_PER_SEAL) as u16;
    // Exclusive
    let end_index = ((start_sector + length) / SECTORS_PER_SEAL) as u16;
    start_index..end_index
}

fn get_covered_sealing_index(start_sector: usize, length: usize) -> std::ops::Range<u16> {
    // Inclusive
    let start_index = (start_sector / SECTORS_PER_SEAL) as u16;
    // Exclusive
    let end_index = ((start_sector + length + SECTORS_PER_SEAL - 1) / SECTORS_PER_SEAL) as u16;
    start_index..end_index
}

#[cfg(test)]
mod tests {
    use crate::log_store::load_chunk::chunk_data::PartialBatch;

    use super::EntryBatchData;
    use ionian_spec::{BYTES_PER_LOADING, BYTES_PER_SECTOR, SECTORS_PER_LOADING};
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    fn test_data() -> Vec<u8> {
        let mut data = vec![0u8; BYTES_PER_LOADING];
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
                    BYTES_PER_LOADING / 4 * i,
                    data[(BYTES_PER_LOADING / 4) * i..(BYTES_PER_LOADING / 4) * (i + 1)].to_vec(),
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
                    BYTES_PER_LOADING / 4 * i,
                    data[(BYTES_PER_LOADING / 4) * i..(BYTES_PER_LOADING / 4) * (i + 1)].to_vec(),
                )
                .unwrap();
        }

        chunk_batch.truncate(BYTES_PER_LOADING / 4 * 3 + BYTES_PER_SECTOR);

        let chunks = if let EntryBatchData::Incomplete(chunks) = chunk_batch {
            chunks
        } else {
            unreachable!();
        };

        assert!(chunks.len() == 2);
        assert!(
            chunks[0]
                == PartialBatch {
                    start_sector: SECTORS_PER_LOADING / 4,
                    data: data[(BYTES_PER_LOADING / 4) * 1..(BYTES_PER_LOADING / 4) * 2].to_vec()
                }
        );
        assert!(
            chunks[1]
                == PartialBatch {
                    start_sector: SECTORS_PER_LOADING / 4 * 3,
                    data: data
                        [BYTES_PER_LOADING / 4 * 3..BYTES_PER_LOADING / 4 * 3 + BYTES_PER_SECTOR]
                        .to_vec()
                }
        );
    }

    #[test]
    fn test_data_chunk_get_slice() {
        let data = test_data();
        let mut chunk_batch = EntryBatchData::new();

        const N: usize = BYTES_PER_LOADING;
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
