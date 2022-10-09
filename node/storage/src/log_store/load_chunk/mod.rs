mod bitmap;
mod chunk_data;
mod seal;

use std::cmp::min;

use anyhow::Result;
use ethereum_types::H256;
use ssz_derive::{Decode, Encode};

use ionian_spec::{BYTES_PER_SEAL, BYTES_PER_SECTOR, SECTORS_PER_SEAL};
use shared_types::ChunkArray;

use crate::log_store::{load_chunk::seal::keccak, log_manager::PORA_CHUNK_SIZE};
use chunk_data::EntryBatchData;
use seal::SealInfo;

#[derive(Encode, Decode)]
pub struct EntryBatch {
    seal_info: SealInfo,
    // the inner data
    data: EntryBatchData,
}

impl EntryBatch {
    pub fn new_with_chunk_array(
        chunk: ChunkArray,
        chunk_index: u64,
        start_offset: usize,
        is_full_chunk: bool,
    ) -> Self {
        Self {
            seal_info: SealInfo::new(None, chunk_index),
            data: EntryBatchData::new_with_chunk_array(chunk, start_offset, is_full_chunk),
        }
    }
}

impl EntryBatch {
    /// Get unsealed data
    pub fn get_data(&self, start: usize, length: usize) -> Option<Vec<u8>> {
        // If the start position is not aligned and is sealed, we need to load one more word for unsealing
        let advanced_by_one = if start % SECTORS_PER_SEAL == 0 {
            // If the start position is not aligned, it is no need to load one more word
            false
        } else {
            // otherwise, it depends on if the given offset is seal
            self.seal_info.is_sealed((start / SECTORS_PER_SEAL) as u16)
        };

        let start_byte = start * BYTES_PER_SECTOR;
        let length_byte = length * BYTES_PER_SECTOR;

        // Load data slice with the word for unsealing
        let (mut loaded_data, unseal_hint) = if advanced_by_one {
            let loaded_data_with_hint = self.data.get(start_byte - 32, length_byte + 32)?;

            // TODO: use `split_array_ref` instead when this api is stable.
            let (unseal_hint, loaded_data) = loaded_data_with_hint.split_at(32);
            let unseal_hint = <[u8; 32]>::try_from(unseal_hint).unwrap();
            (loaded_data.to_vec(), Some(unseal_hint))
        } else {
            (self.data.get(start_byte, length_byte)?.to_vec(), None)
        };

        let first_chunk_length = BYTES_PER_SEAL - start_byte % BYTES_PER_SEAL;

        // Unseal the first incomplete sealing chunk (if exists)
        if let Some(unseal_hint) = unseal_hint {
            // We do not need to check if this sealing chunk exists, since we have checked it before loading unseal_hint
            assert!(first_chunk_length != 0);

            let first_mask = keccak(&unseal_hint);

            if loaded_data.len() < first_chunk_length {
                // The loaded data does not cross sealings
                SealInfo::unseal_partial(loaded_data.as_mut(), first_mask);
            } else {
                SealInfo::unseal_partial(loaded_data[..first_chunk_length].as_mut(), first_mask);
            };
        }

        if loaded_data.len() > first_chunk_length {
            let complete_chunks = &mut loaded_data[first_chunk_length..];
            assert!((start_byte + first_chunk_length) % BYTES_PER_SEAL == 0);
            let start_sealing_index = (start_byte + first_chunk_length) / BYTES_PER_SEAL;

            for (sealing_chunk, sealing_index) in complete_chunks
                .chunks_mut(BYTES_PER_SEAL)
                .enumerate()
                .map(|(idx, chunk)| (chunk, start_sealing_index + idx))
            {
                self.seal_info.unseal(sealing_chunk, sealing_index as u16);
            }
        }

        Some(loaded_data)
    }

    /// Return `Error` if the new data overlaps with old data.
    /// Convert `Incomplete` to `Completed` if the chunk is completed after the insertion.
    pub fn insert_data(&mut self, offset: usize, data: Vec<u8>) -> Result<Vec<u16>> {
        self.data.insert_data(offset * BYTES_PER_SECTOR, data)
    }

    pub fn truncate(&mut self, start_offset: usize) {
        assert!(start_offset > 0 && start_offset < PORA_CHUNK_SIZE);

        self.data.truncate(start_offset * BYTES_PER_SECTOR);
        self.truncate_seal(start_offset);
    }

    pub fn into_data_list(self, global_start_entry: u64) -> Vec<ChunkArray> {
        self.data
            .available_range_entries()
            .into_iter()
            .map(|(start_entry, length_entry)| ChunkArray {
                data: self.get_data(start_entry, length_entry).unwrap().to_vec(),
                start_index: global_start_entry + start_entry as u64,
            })
            .collect()
    }

    fn truncate_seal(&mut self, start_offset: usize) {
        let first_truncate_seal = (start_offset / SECTORS_PER_SEAL) as u16;
        let (first_seal_undo, _, _) =
            if let Some(x) = self.seal_info.get_seal_context(first_truncate_seal) {
                x
            } else {
                // The trucated data is not sealed
                return;
            };

        // last_seal_undo could be first_truncate_seal or first_truncate_seal+1
        let last_seal_undo = (start_offset + SECTORS_PER_SEAL - 1) / SECTORS_PER_SEAL;

        for seal_index in (first_seal_undo as usize)..last_seal_undo {
            if !self.seal_info.is_sealed(seal_index as u16) {
                continue;
            }

            let start_byte = seal_index * BYTES_PER_SEAL;
            let length = min(start_offset * BYTES_PER_SEAL - start_byte, BYTES_PER_SEAL);

            let chunk_to_unseal = self
                .data
                .get_mut(start_byte, length)
                .expect("Sealed chunk should be complete");
            self.seal_info.unseal(chunk_to_unseal, seal_index as u16);
        }

        // trucate the bitmap
        self.seal_info.truncate(first_seal_undo);
    }

    pub fn build_root(&self, is_first_chunk: bool) -> Result<Option<H256>> {
        self.data.build_root(is_first_chunk)
    }
}
