#[macro_use]
extern crate tracing;

mod peer_manager;
mod service;

pub use crate::service::RouterService;

#[derive(Debug, Clone)]
pub struct Config {
    pub heartbeat_interval_secs: u64,

    pub idle_time_secs: u64,
    pub max_idle_incoming_peers: usize,
    pub max_idle_outgoing_peers: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval_secs: 5,
            idle_time_secs: 180,
            max_idle_incoming_peers: 12,
            max_idle_outgoing_peers: 20,
        }
    }
}
