#[macro_use]
extern crate tracing;

mod context;
mod controllers;
mod manager;
mod service;
mod test_util;

pub use service::{SyncMessage, SyncRequest, SyncResponse, SyncSender, SyncService};

#[derive(Default)]
pub struct Config {
    pub auto_sync_disabled: bool,
}

impl Config {
    pub fn disable_auto_sync(mut self) -> Self {
        self.auto_sync_disabled = true;
        self
    }
}
