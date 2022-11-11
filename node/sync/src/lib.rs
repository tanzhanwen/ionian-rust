#[macro_use]
extern crate tracing;

mod auto_sync;
mod context;
mod controllers;
mod service;
pub mod test_util;

pub use controllers::FileSyncInfo;
pub use service::{SyncMessage, SyncReceiver, SyncRequest, SyncResponse, SyncSender, SyncService};
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub auto_sync_disabled: bool,
    pub max_sync_files: usize,

    pub find_peer_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auto_sync_disabled: false,
            max_sync_files: 100,
            find_peer_timeout: Duration::from_secs(30),
        }
    }
}

impl Config {
    pub fn disable_auto_sync(mut self) -> Self {
        self.auto_sync_disabled = true;
        self
    }
}
