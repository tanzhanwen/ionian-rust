use super::mem_pool::{MemoryCachedFile, MemoryChunkPool};
use anyhow::Result;
use shared_types::{ChunkArray, DataRoot, CHUNK_SIZE};
use std::sync::Arc;
use storage::log_store::Store;
use tokio::sync::mpsc::UnboundedReceiver;

const SEGMENT_DATA_SIZE: usize = super::NUM_CHUNKS_PER_SEGMENT * CHUNK_SIZE;

/// Handle the cached file when uploaded completely and verified from blockchain.
/// Generally, the file will be persisted into log store.
pub struct ChunkPoolHandler {
    receiver: UnboundedReceiver<DataRoot>,
    mem_pool: Arc<MemoryChunkPool>,
    log_store: Arc<dyn Store>,
}

impl ChunkPoolHandler {
    pub(crate) fn new(
        receiver: UnboundedReceiver<DataRoot>,
        mem_pool: Arc<MemoryChunkPool>,
        log_store: Arc<dyn Store>,
    ) -> Self {
        ChunkPoolHandler {
            receiver,
            mem_pool,
            log_store,
        }
    }

    pub async fn handle(&mut self) -> Result<bool> {
        let root = match self.receiver.recv().await {
            Some(root) => root,
            None => return Ok(false),
        };

        let file = match self.mem_pool.remove_file(&root).await {
            Some(file) => file,
            None => return Ok(false),
        };

        // File will not be persisted if any error occurred.
        self.persist_file(root, file)?;

        Ok(true)
    }

    fn persist_file(&self, root: DataRoot, file: MemoryCachedFile) -> Result<()> {
        let mut data = file.data;
        let mut chunk_offset: usize = 0;

        loop {
            if data.len() <= SEGMENT_DATA_SIZE {
                self.persist_segment(&root, data, chunk_offset)?;
                break;
            }

            // TODO(qhz): avoid frequent memory reallocation.
            let remain = data.split_off(SEGMENT_DATA_SIZE);
            self.persist_segment(&root, data, chunk_offset)?;

            chunk_offset += super::NUM_CHUNKS_PER_SEGMENT;
            data = remain;
        }

        Ok(())
    }

    fn persist_segment(
        &self,
        _root: &DataRoot,
        segment: Vec<u8>,
        chunk_start_index: usize,
    ) -> Result<()> {
        // TODO(qhz): enhance LogStoreChunkWrite trait to put chunks by data root.
        self.log_store.put_chunks(
            1,
            ChunkArray {
                data: segment,
                start_index: chunk_start_index as u32,
            },
        )
    }
}