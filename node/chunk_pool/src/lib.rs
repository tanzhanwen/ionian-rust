#[macro_use]
extern crate tracing;

mod handler;
mod mem_pool;

pub use handler::ChunkPoolHandler;
pub use mem_pool::{FileID, MemoryChunkPool, SegmentInfo};
use shared_types::DataRoot;

use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub write_window_size: usize,
    pub max_cached_chunks_all: usize,
    pub max_writings: usize,
    pub expiration_time_secs: u64,
}

impl Config {
    pub fn expiration_time(&self) -> Duration {
        Duration::from_secs(self.expiration_time_secs)
    }
}

pub fn unbounded(
    config: Config,
    log_store: storage_async::Store,
    network_send: tokio::sync::mpsc::UnboundedSender<network::NetworkMessage>,
    tx_root_recv: tokio::sync::broadcast::Receiver<DataRoot>,
) -> (Arc<MemoryChunkPool>, ChunkPoolHandler) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let mem_pool = Arc::new(MemoryChunkPool::new(config, log_store.clone(), sender, tx_root_recv));
    let handler = ChunkPoolHandler::new(receiver, mem_pool.clone(), log_store, network_send);

    (mem_pool, handler)
}
