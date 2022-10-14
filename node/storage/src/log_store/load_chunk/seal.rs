use ethereum_types::H256;
use ionian_seal;
use ionian_spec::{SEALS_PER_LOADING, SECTORS_PER_LOADING, SECTORS_PER_SEAL};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use static_assertions::const_assert;

use super::bitmap::WrappedBitmap;

#[derive(DeriveEncode, DeriveDecode)]
pub struct SealContextInfo {
    /// The context digest for this seal group
    context_digest: H256,
    /// The end position (exclusive) indexed by sectors
    end_position: u16,
}

type ChunkSealBitmap = WrappedBitmap<SEALS_PER_LOADING>;
const_assert!(SEALS_PER_LOADING <= u128::BITS as usize);

#[derive(Default, DeriveEncode, DeriveDecode)]
pub struct SealInfo {
    // a bitmap specify which sealing chunks have been sealed
    bitmap: ChunkSealBitmap,
    // the batch_offset (seal chunks) of the EntryBatch this seal info belongs to
    load_chunk_index: u64,
    // the miner Id for sealing this chunk, zero representing doesn't exists
    miner_id: H256,
    // seal context information, indexed by u16. Get a position has never been set is undefined behaviour.
    seal_contexts: Vec<SealContextInfo>,
}

impl SealInfo {
    pub fn new(load_chunk_index: u64) -> Self {
        Self {
            load_chunk_index,
            ..Default::default()
        }
    }

    pub fn is_sealed(&self, index: u16) -> bool {
        self.bitmap.get(index as usize)
    }

    pub fn load_chunk_index(&self) -> u64 {
        self.load_chunk_index
    }

    pub fn truncate(&mut self, index: u16) {
        self.bitmap.truncate(index);
        if index == 0 {
            self.seal_contexts.clear();
        } else {
            let context_position = self
                .seal_contexts
                .binary_search_by_key(&index, |x| x.end_position)
                .expect("truncate position is not at the start of seal context");
            self.seal_contexts.truncate(context_position + 1);
        }
    }

    /// Return the start position (inclusive, in seals), the end position (exclusive, in seals) and context hash
    pub fn get_seal_context(&self, seal_index: u16) -> Option<H256> {
        let index = match self
            .seal_contexts
            .binary_search_by_key(&(seal_index + 1), |x| x.end_position)
        {
            Ok(x) | Err(x) => x,
        };

        if index == self.seal_contexts.len() {
            None
        } else {
            Some(self.seal_contexts[index].context_digest)
        }
    }

    /// Return the start position (inclusive, in seals), the end position (exclusive, in seals) and context hash
    pub fn trucate_seal_context(&self, seal_index: u16) -> u16 {
        let index = match self
            .seal_contexts
            .binary_search_by_key(&(seal_index + 1), |x| x.end_position)
        {
            Ok(x) | Err(x) => x,
        };

        if index == 0 {
            0
        } else {
            self.seal_contexts.get(index - 1).unwrap().end_position
        }
    }

    pub fn set_seal_context(
        &mut self,
        index: u16,
        context_digest: H256,
        end_position: u64,
        miner_id: H256,
    ) {
        self.bitmap.set(index as usize, true);
        if self.miner_id.is_zero() {
            self.miner_id = miner_id;
        } else {
            assert!(
                self.miner_id == miner_id,
                "miner_id setting is inconsistent with db"
            );
        }

        let local_end_position = std::cmp::min(
            end_position - self.load_chunk_index * SEALS_PER_LOADING as u64,
            SEALS_PER_LOADING as u64,
        ) as u16;

        if self.seal_contexts.is_empty() {
            self.seal_contexts.push(SealContextInfo {
                context_digest,
                end_position: local_end_position,
            });
            return;
        }

        let insert_position = match self
            .seal_contexts
            .binary_search_by_key(&(index + 1), |x| x.end_position)
        {
            Ok(x) | Err(x) => x,
        };

        if insert_position >= 1
            && self.seal_contexts[insert_position - 1].context_digest == context_digest
        {
            assert!(
                self.seal_contexts
                    .get(insert_position)
                    .map_or(true, |x| x.end_position > index + 1),
                "Seal context conflict"
            );
            let end_position = &mut self.seal_contexts[insert_position - 1].end_position;
            *end_position = std::cmp::max(*end_position, index + 1);
            return;
        }

        if insert_position < self.seal_contexts.len()
            && self.seal_contexts[insert_position].context_digest == context_digest
        {
            let end_position = &mut self.seal_contexts[insert_position].end_position;
            *end_position = std::cmp::max(*end_position, index + 1);
            return;
        }

        self.seal_contexts.insert(
            insert_position,
            SealContextInfo {
                context_digest,
                end_position: index + 1,
            },
        );
    }

    pub fn unseal(&self, data: &mut [u8], index: u16) {
        if !self.is_sealed(index) {
            return;
        }
        let seal_context = self
            .get_seal_context(index)
            .expect("cannot unseal non-sealed data");
        ionian_seal::unseal(
            data,
            &self.miner_id,
            &seal_context,
            self.global_seal_sector(index),
        );
    }

    #[cfg(test)]
    pub fn seal(&self, data: &mut [u8], index: u16) {
        if self.is_sealed(index) {
            return;
        }
        let seal_context = self
            .get_seal_context(index)
            .expect("cannot unseal non-sealed data");
        ionian_seal::seal(
            data,
            &self.miner_id,
            &seal_context,
            self.global_seal_sector(index),
        );
    }

    pub fn global_seal_sector(&self, index: u16) -> u64 {
        (self.load_chunk_index as usize * SECTORS_PER_LOADING + index as usize * SECTORS_PER_SEAL)
            as u64
    }
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;
    use hex_literal::hex;
    use ionian_seal;
    use ionian_spec::BYTES_PER_SEAL;
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use super::{SealContextInfo, SealInfo};

    const TEST_MINER_ID: H256 = H256(hex!(
        "003d82782c78262bada18a22f5f982d2b43934d5541e236ca3781ddc8c911cb8"
    ));

    #[test]
    fn get_seal_context() {
        let mut random = StdRng::seed_from_u64(149);

        let mut context1 = H256::default();
        let mut context2 = H256::default();
        let mut context3 = H256::default();
        random.fill_bytes(&mut context1.0);
        random.fill_bytes(&mut context2.0);
        random.fill_bytes(&mut context3.0);

        let mut sealer = SealInfo::new(0);
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context1,
            end_position: 2,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context2,
            end_position: 3,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context3,
            end_position: 6,
        });

        assert_eq!(sealer.get_seal_context(0), Some(context1));
        assert_eq!(sealer.get_seal_context(1), Some(context1));
        assert_eq!(sealer.get_seal_context(2), Some(context2));
        assert_eq!(sealer.get_seal_context(3), Some(context3));
        assert_eq!(sealer.get_seal_context(4), Some(context3));
        assert_eq!(sealer.get_seal_context(5), Some(context3));
        assert_eq!(sealer.get_seal_context(6), None);

        assert_eq!(sealer.trucate_seal_context(0), 0);
        assert_eq!(sealer.trucate_seal_context(1), 0);
        assert_eq!(sealer.trucate_seal_context(2), 2);
        assert_eq!(sealer.trucate_seal_context(3), 3);
        assert_eq!(sealer.trucate_seal_context(4), 3);
        assert_eq!(sealer.trucate_seal_context(5), 3);
        assert_eq!(sealer.trucate_seal_context(6), 6);
    }

    #[test]
    fn unseal_chunks() {
        let mut random = StdRng::seed_from_u64(137);
        let mut unsealed_data = vec![0u8; BYTES_PER_SEAL * 10];
        random.fill_bytes(&mut unsealed_data);
        let mut data = unsealed_data.clone();

        let mut context1 = H256::default();
        let mut context2 = H256::default();
        let mut context3 = H256::default();
        random.fill_bytes(&mut context1.0);
        random.fill_bytes(&mut context2.0);
        random.fill_bytes(&mut context3.0);

        let mut sealer = SealInfo::new(100);
        sealer.miner_id = TEST_MINER_ID;

        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context1,
            end_position: 2,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context2,
            end_position: 5,
        });
        sealer.seal_contexts.push(SealContextInfo {
            context_digest: context3,
            end_position: 10,
        });

        // skip seal 6, 3, 9
        for idx in [1, 7, 2, 5, 0, 8, 4].into_iter() {
            sealer.seal(
                &mut data[idx * BYTES_PER_SEAL..(idx + 1) * BYTES_PER_SEAL],
                idx as u16,
            );
            sealer.bitmap.set(idx, true);
        }

        let partial_hint = &data[BYTES_PER_SEAL * 5 + 64..BYTES_PER_SEAL * 5 + 96];
        let mut tmp_data = data.clone();
        ionian_seal::unseal_with_mask_seed(
            &mut tmp_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6],
            partial_hint,
        );
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6],
            &unsealed_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6]
        );

        let mut tmp_data = data.clone();
        sealer.unseal(&mut tmp_data[BYTES_PER_SEAL * 5..BYTES_PER_SEAL * 6], 5);
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 5..BYTES_PER_SEAL * 6],
            &unsealed_data[BYTES_PER_SEAL * 5..BYTES_PER_SEAL * 6]
        );

        let mut tmp_data = data.clone();
        sealer.unseal(&mut tmp_data[BYTES_PER_SEAL * 6..BYTES_PER_SEAL * 7], 6);
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 6..BYTES_PER_SEAL * 7],
            &unsealed_data[BYTES_PER_SEAL * 6..BYTES_PER_SEAL * 7]
        );

        let mut tmp_data = data.clone();
        sealer.unseal(
            &mut tmp_data[BYTES_PER_SEAL * 7..BYTES_PER_SEAL * 7 + 96],
            7,
        );
        assert_eq!(
            &tmp_data[BYTES_PER_SEAL * 7..BYTES_PER_SEAL * 7 + 96],
            &unsealed_data[BYTES_PER_SEAL * 7..BYTES_PER_SEAL * 7 + 96]
        );
    }
}
