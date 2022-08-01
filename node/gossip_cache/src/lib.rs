#[macro_use]
extern crate tracing;

mod gossip_cache;

pub use crate::gossip_cache::GossipCache;
use std::time::Duration;

pub struct Config {
    pub max_files: usize,
    pub max_entries_per_file: usize,
    pub entry_expiration_time: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_files: 1000,
            max_entries_per_file: 3,
            entry_expiration_time: Duration::from_secs(300),
        }
    }
}
