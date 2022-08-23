use crate::sync_manager::config::LogSyncConfig;
use crate::sync_manager::log_entry_fetcher::LogEntryFetcher;
use anyhow::Result;
use jsonrpsee::tracing::{debug, error};
use shared_types::Transaction;
use std::cmp::Ordering;
use std::future::Future;
use std::sync::Arc;
use storage::log_store::Store;
use task_executor::{ShutdownReason, TaskExecutor};
use tokio::sync::RwLock;

pub struct LogSyncManager {
    #[allow(unused)]
    config: LogSyncConfig,
    log_fetcher: LogEntryFetcher,
    store: Arc<RwLock<dyn Store>>,

    next_tx_seq: u64,
}

impl LogSyncManager {
    pub async fn spawn(
        config: LogSyncConfig,
        executor: TaskExecutor,
        store: Arc<RwLock<dyn Store>>,
    ) -> Result<()> {
        let next_tx_seq = store.read().await.next_tx_seq()?;

        let executor_clone = executor.clone();
        // Spawn the task to sync log entries from the blockchain.
        executor.spawn(
            run_and_log(executor.shutdown_sender(), async move {
                let log_fetcher =
                    LogEntryFetcher::new(&config.rpc_endpoint_url, config.contract_address).await?;
                let mut log_sync_manager = Self {
                    config,
                    log_fetcher,
                    next_tx_seq,
                    store,
                };

                // Start watching before recovery to ensure that no log is skipped.
                // TODO(zz): Rate limit to avoid OOM during recovery.
                let mut watch_rx = log_sync_manager.log_fetcher.start_watch(0, &executor_clone);
                let mut recover_rx = log_sync_manager
                    .log_fetcher
                    .start_recover(0, &executor_clone);
                while let Some(tx) = recover_rx.recv().await {
                    if !log_sync_manager.put_tx(tx).await {
                        // Unexpected error.
                        error!("log sync error");
                        break;
                    }
                }
                while let Some(tx) = watch_rx.recv().await {
                    if !log_sync_manager.put_tx(tx).await {
                        // Unexpected error.
                        error!("log watch error");
                        break;
                    }
                }
                Ok(())
            }),
            "log_sync",
        );
        Ok(())
    }

    async fn put_tx(&mut self, tx: Transaction) -> bool {
        match tx.seq.cmp(&self.next_tx_seq) {
            Ordering::Less => {
                // FIXME(zz): Handle reorg after restart.
                debug!("Ignore old tx: seq={}", tx.seq);
                true
            }
            Ordering::Equal => {
                debug!("log entry sync get entry: {:?}", tx);
                if let Err(e) = self.store.write().await.put_tx(tx) {
                    error!("put_tx error: e={:?}", e);
                    false
                } else {
                    self.next_tx_seq += 1;
                    true
                }
            }
            Ordering::Greater => {
                error!(
                    "Unexpected transaction skip: next={} get={}",
                    self.next_tx_seq, tx.seq
                );
                false
            }
        }
    }
}

async fn run_and_log(
    mut shutdown_sender: futures::channel::mpsc::Sender<ShutdownReason>,
    f: impl Future<Output = Result<()>> + Send + 'static,
) {
    if let Err(e) = f.await {
        error!("log sync failure: e={:?}", e);
        shutdown_sender
            .try_send(ShutdownReason::Failure("log sync failure"))
            .expect("shutdown send error");
    }
}

pub(crate) mod config;
mod log_entry_fetcher;
