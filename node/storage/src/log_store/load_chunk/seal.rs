use ethereum_types::H256;
use ionian_spec::{SEALS_PER_LOADING, SECTORS_PER_LOADING, SECTORS_PER_SEAL};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use static_assertions::const_assert;
use tiny_keccak::{Hasher, Keccak};

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
    // seal information
    seal_contexts: Vec<SealContextInfo>,
}

impl SealInfo {
    pub fn new(miner_id: Option<H256>, load_chunk_index: u64) -> Self {
        Self {
            load_chunk_index,
            miner_id: miner_id.unwrap_or_default(),
            ..Default::default()
        }
    }

    pub fn is_sealed(&self, index: u16) -> bool {
        self.bitmap.get(index as usize)
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
    pub fn get_seal_context(&self, index: u16) -> Option<(u16, u16, H256)> {
        let index = match self
            .seal_contexts
            .binary_search_by_key(&(index + 1), |x| x.end_position)
        {
            Ok(x) | Err(x) => x,
        };

        if index == self.seal_contexts.len() {
            return None;
        }

        let context = self.seal_contexts[index].context_digest;
        let end_position = self.seal_contexts[index].end_position;
        let start_position = if index == 0 {
            0
        } else {
            self.seal_contexts.get(index - 1).unwrap().end_position
        };
        Some((start_position, end_position, context))
    }

    pub fn unseal_partial(data: &mut [u8], first_mask: [u8; 32]) {
        assert!(data.len() % 32 == 0);

        let mut mask = first_mask;
        data.chunks_exact_mut(32).for_each(|x| {
            let next_mask = keccak(&*x);

            // Compiler will optimize this well
            x.iter_mut()
                .zip(mask.iter())
                .for_each(|(x, mask)| *x ^= *mask);
            mask = next_mask;
        })
    }

    pub fn unseal(&self, data: &mut [u8], index: u16) {
        if !self.is_sealed(index) {
            return;
        }
        let first_mask = self.compute_first_mask(index).unwrap();
        Self::unseal_partial(data, first_mask);
    }

    fn compute_first_mask(&self, index: u16) -> Result<[u8; 32], &'static str> {
        assert!(self.miner_id != H256::zero());

        let (_, _, seal_context) = self
            .get_seal_context(index)
            .ok_or("try to unseal non-sealed data")?;

        let start_sector_index = self.load_chunk_index as usize * SECTORS_PER_LOADING
            + index as usize * SECTORS_PER_SEAL;

        let mut hasher = Keccak::v256();
        hasher.update(&self.miner_id.0);
        hasher.update(&seal_context.0);
        hasher.update(&[0u8; 24]);
        hasher.update(&start_sector_index.to_be_bytes());

        let mut first_mask = [0u8; 32];
        hasher.finalize(&mut first_mask);
        Ok(first_mask)
    }
}

pub fn keccak(input: impl AsRef<[u8]>) -> [u8; 32] {
    let mut hasher = Keccak::v256();
    let mut output = [0u8; 32];
    hasher.update(input.as_ref());
    hasher.finalize(&mut output);
    output
}

#[cfg(test)]
mod tests {
    use ethereum_types::H256;
    use hex_literal::hex;
    use ionian_spec::BYTES_PER_SEAL;
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use super::{keccak, SealContextInfo, SealInfo};

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

        let mut sealer = SealInfo::new(None, 0);
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

        assert_eq!(sealer.get_seal_context(0), Some((0, 2, context1)));
        assert_eq!(sealer.get_seal_context(1), Some((0, 2, context1)));
        assert_eq!(sealer.get_seal_context(2), Some((2, 3, context2)));
        assert_eq!(sealer.get_seal_context(3), Some((3, 6, context3)));
        assert_eq!(sealer.get_seal_context(4), Some((3, 6, context3)));
        assert_eq!(sealer.get_seal_context(5), Some((3, 6, context3)));
        assert_eq!(sealer.get_seal_context(6), None);
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

        let mut sealer = SealInfo::new(Some(TEST_MINER_ID), 100);

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
            sealer.bitmap.set(idx, true);
            let mut mask = sealer.compute_first_mask(idx as u16).unwrap();
            let chunk = &mut data[idx * BYTES_PER_SEAL..(idx + 1) * BYTES_PER_SEAL];
            for word in chunk.chunks_mut(32) {
                word.iter_mut().zip(mask.iter()).for_each(|(x, y)| *x ^= *y);
                mask = keccak(&*word);
            }
        }

        let partial_hint = &data[BYTES_PER_SEAL * 5 + 64..BYTES_PER_SEAL * 5 + 96];
        let first_mask: [u8; 32] = keccak(partial_hint).try_into().unwrap();
        let mut tmp_data = data.clone();
        SealInfo::unseal_partial(
            &mut tmp_data[BYTES_PER_SEAL * 5 + 96..BYTES_PER_SEAL * 6],
            first_mask,
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
