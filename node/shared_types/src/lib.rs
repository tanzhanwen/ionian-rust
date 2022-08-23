use anyhow::bail;
use append_merkle::{Proof as RawProof, RangeProof as RawRangeProof};
use ethereum_types::{H256, U256};
use merkle_light::merkle::next_pow2;
use merkle_light::proof::Proof as RawFileProof;
use merkle_tree::RawLeafSha3Algorithm;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode as DeriveDecode, Encode as DeriveEncode};
use tracing::debug;

/// Application level requests sent to the network.
#[derive(Debug, Clone, Copy)]
pub enum RequestId {
    Router,
}

pub type DataRoot = H256;

pub type FlowProof = RawProof<H256>;
pub type FlowRangeProof = RawRangeProof<H256>;

// Each chunk is 32 bytes.
pub const CHUNK_SIZE: usize = 256;

pub fn bytes_to_chunks(size_bytes: usize) -> usize {
    if size_bytes % CHUNK_SIZE == 0 {
        size_bytes / CHUNK_SIZE
    } else {
        size_bytes / CHUNK_SIZE + 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Chunk(pub [u8; CHUNK_SIZE]);

#[derive(Clone, Debug, Eq, PartialEq, DeriveDecode, DeriveEncode, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub stream_ids: Vec<U256>,
    /// In-place data.
    pub data: Vec<u8>,
    pub data_merkle_root: DataRoot,
    /// `(subtree_depth, subtree_root)`
    pub merkle_nodes: Vec<(usize, DataRoot)>,

    pub start_entry_index: u64,
    pub size: u64,
    pub seq: u64,
}

pub struct ChunkWithProof {
    pub chunk: Chunk,
    pub proof: FlowProof,
}

#[derive(Debug, Clone, PartialEq, Eq, DeriveEncode, DeriveDecode)]
pub struct ChunkArrayWithProof {
    pub chunks: ChunkArray,
    // TODO: The top levels of the two proofs can be merged.
    pub proof: FlowRangeProof,
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveEncode, DeriveDecode)]
pub struct ChunkArray {
    // The length is exactly a multiple of `CHUNK_SIZE`
    pub data: Vec<u8>,
    pub start_index: u64,
}

impl ChunkArray {
    pub fn first_chunk(&self) -> Option<Chunk> {
        self.chunk_at(self.start_index as usize)
    }

    pub fn last_chunk(&self) -> Option<Chunk> {
        let last_index =
            (self.start_index as usize + self.data.len() / CHUNK_SIZE).checked_sub(1)?;
        self.chunk_at(last_index)
    }

    pub fn chunk_at(&self, index: usize) -> Option<Chunk> {
        if index >= self.data.len() / CHUNK_SIZE + self.start_index as usize
            || index < self.start_index as usize
        {
            return None;
        }
        let offset = (index - self.start_index as usize) * CHUNK_SIZE;
        Some(Chunk(
            self.data[offset..offset + CHUNK_SIZE]
                .try_into()
                .expect("length match"),
        ))
    }

    pub fn sub_array(&self, start: u64, end: u64) -> Option<ChunkArray> {
        if start >= (self.data.len() / CHUNK_SIZE) as u64 + self.start_index
            || start < self.start_index
            || end > (self.data.len() / CHUNK_SIZE) as u64 + self.start_index
            || end <= self.start_index
            || end <= start
        {
            return None;
        }
        let start_offset = (start - self.start_index) as usize * CHUNK_SIZE;
        let end_offset = (end - self.start_index) as usize * CHUNK_SIZE;
        Some(ChunkArray {
            data: self.data[start_offset..end_offset].to_vec(),
            start_index: start as u64,
        })
    }
}

impl std::fmt::Display for ChunkArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChunkArray {{ chunks = {}, start_index = {} }}",
            self.data.len() / CHUNK_SIZE,
            self.start_index
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, DeriveEncode, DeriveDecode, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileProof {
    pub lemma: Vec<H256>,
    pub path: Vec<bool>,
}
impl FileProof {
    pub fn validate(
        &self,
        leaf_hash: &[u8; 32],
        root: &DataRoot,
        position: usize,
        leaf_count: usize,
    ) -> anyhow::Result<bool> {
        let proof_position = self.position(leaf_count)?;
        if proof_position != position {
            bail!(
                "wrong position: proof_pos={} provided={}",
                proof_position,
                position
            );
        }

        let proof: RawFileProof<[u8; 32]> = self.try_into()?;

        if !proof.validate::<RawLeafSha3Algorithm>() {
            debug!("Proof validate fails");
            return Ok(false);
        }

        if proof.root() != root.0 {
            bail!(
                "root mismatch, proof_root={:?} provided={:?}",
                proof.root(),
                root.0
            );
        }

        if proof.item() != *leaf_hash {
            bail!(
                "data hash mismatch: leaf_hash={:?} proof_item={:?}",
                leaf_hash,
                proof.item(),
            );
        }

        Ok(true)
    }

    fn position(&self, total_chunk_count: usize) -> anyhow::Result<usize> {
        let mut left_chunk_count = total_chunk_count;
        let mut proof_position = 0;
        // TODO: After the first `is_left == true`, the tree depth is fixed.
        for is_left in self.path.iter().rev() {
            if left_chunk_count <= 1 {
                bail!(
                    "Proof path too long for a tree size: path={:?}, size={}",
                    self.path,
                    total_chunk_count
                );
            }
            let subtree_size = next_pow2(left_chunk_count) >> 1;
            if !is_left {
                proof_position += subtree_size;
                left_chunk_count -= subtree_size;
            } else {
                left_chunk_count = subtree_size;
            }
        }
        if left_chunk_count != 1 {
            bail!(
                "Proof path too short for a tree size: path={:?}, size={}",
                self.path,
                total_chunk_count
            );
        }
        Ok(proof_position)
    }
}

impl TryFrom<&FileProof> for RawFileProof<[u8; 32]> {
    type Error = anyhow::Error;

    fn try_from(value: &FileProof) -> std::result::Result<Self, Self::Error> {
        if (value.lemma.len() == 1 && value.path.is_empty())
            || (value.lemma.len() > 2 && value.lemma.len() == value.path.len() + 2)
        {
            Ok(RawFileProof::<[u8; 32]>::new(
                value.lemma.iter().map(|e| e.0).collect(),
                value.path.clone(),
            ))
        } else {
            bail!("Invalid proof: proof={:?}", value)
        }
    }
}

pub fn timestamp_now() -> u32 {
    let timestamp = chrono::Utc::now().timestamp();
    u32::try_from(timestamp).expect("The year is between 1970 and 2106")
}
