use crate::{SyncRequest, SyncSender};
use anyhow::{bail, Result};
use log_entry_sync::LogSyncEvent;
use ssz::{Decode, Encode};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::{
    sync::broadcast::{error::RecvError, Receiver},
    time::sleep,
};

const INTERVAL_CATCHUP: Duration = Duration::from_millis(1);
const INTERVAL: Duration = Duration::from_secs(3);
const INTERVAL_ERROR: Duration = Duration::from_secs(10);

const KEY_NEXT_TX_SEQ: &[u8] = "sync.manager.next_tx_seq".as_bytes();

// TODO(qhz): auto sync may be blocked once confirmed transaction reverted.
// When confirmed transaction reverted, file may be deleted from storage.
// As a result, it is possible that no storage node has the file anymore.
// User will not aware such situation, and leads to auto sync blocked.

/// SyncManager is used to auto sync files in sequence among storage nodes.
pub struct SyncManager {
    next_tx_seq: u64,
    store: Store,
    sender: SyncSender,

    reverted_tx: Arc<AtomicU64>,
}

impl SyncManager {
    pub fn new(store: Store, sender: SyncSender) -> Self {
        Self {
            next_tx_seq: 0,
            store,
            sender,
            reverted_tx: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    pub async fn start(mut self) {
        match self.load_next_tx_seq_or_default().await {
            Ok(val) => self.next_tx_seq = val,
            Err(err) => error!(%err, "Failed to load sync tx seq"),
        }

        info!(%self.next_tx_seq, "Start to sync file periodically");

        loop {
            // handles reorg before file sync
            self.handle_on_reorg();

            // sync file
            match self.sync_once().await {
                Ok(true) => {
                    self.next_tx_seq += 1;
                    debug!(%self.next_tx_seq, "Completed to sync file");
                    sleep(INTERVAL_CATCHUP).await;
                }
                Ok(false) => {
                    trace!(%self.next_tx_seq, "File in sync or log entry unavailable");
                    sleep(INTERVAL).await;
                }
                Err(err) => {
                    warn!(%err, %self.next_tx_seq, "Failed to sync file");
                    sleep(INTERVAL_ERROR).await;
                }
            }
        }
    }

    async fn load_next_tx_seq_or_default(&mut self) -> Result<u64> {
        let val = match self
            .store
            .get_store()
            .read()
            .await
            .get_config(KEY_NEXT_TX_SEQ)?
        {
            Some(val) => val,
            None => return Ok(0),
        };

        match u64::from_ssz_bytes(&val) {
            Ok(val) => Ok(val),
            Err(e) => bail!("SSZ decode error: {:?}", e),
        }
    }

    async fn sync_once(&mut self) -> Result<bool> {
        // already finalized
        if self.store.check_tx_completed(self.next_tx_seq).await? {
            let val = self.next_tx_seq.as_ssz_bytes();
            self.store
                .get_store()
                .read()
                .await
                .set_config(KEY_NEXT_TX_SEQ, &val)?;
            return Ok(true);
        }

        // tx not available yet
        if self
            .store
            .get_tx_by_seq_number(self.next_tx_seq)
            .await?
            .is_none()
        {
            return Ok(false);
        }

        // notify `SyncService` to sync file
        self.sender
            .request(SyncRequest::SyncFile {
                tx_seq: self.next_tx_seq,
            })
            .await?;

        Ok(false)
    }

    /// Monitors chain reorg event. Once transaction reverted in storage, node
    /// needs to re-sync files according to the latest log entry information.
    pub fn monitor_reorg(
        &self,
        executor: &TaskExecutor,
        mut receiver: Receiver<LogSyncEvent>,
        name: &'static str,
    ) {
        let reverted_tx_cloned = self.reverted_tx.clone();

        let task = async move {
            loop {
                match receiver.recv().await {
                    Ok(LogSyncEvent::ReorgDetected { .. }) => {}
                    Ok(LogSyncEvent::Reverted { tx_seq }) => {
                        // requires to re-sync files since transaction and files removed in storage
                        let reverted_tx = reverted_tx_cloned.load(Ordering::Relaxed);
                        if reverted_tx > tx_seq {
                            reverted_tx_cloned.store(tx_seq, Ordering::Relaxed);
                        }
                    }
                    Err(RecvError::Closed) => {
                        error!("Failed to receive reverted tx (Closed)");
                        return;
                    }
                    Err(RecvError::Lagged(lagged)) => {
                        // Generally, such error should not happen since confirmed block
                        // reorg rarely happen, and the buffer size of broadcast channel
                        // is big enough.
                        error!(%lagged, "Failed to receive reverted tx (Lagged)");
                    }
                }
            }
        };

        executor.spawn(task, name);
    }

    fn handle_on_reorg(&mut self) {
        let reverted_tx = self.reverted_tx.load(Ordering::Relaxed);

        // no reorg
        if reverted_tx == u64::MAX {
            return;
        }

        self.reverted_tx.store(u64::MAX, Ordering::Relaxed);

        // reorg happened, but no impact on file sync
        if reverted_tx > self.next_tx_seq {
            return;
        }

        // handles on reorg
        info!(%reverted_tx, %self.next_tx_seq, "Confirmed transaction reverted");

        // request sync service to terminate the file sync immediately
        let _ = self.sender.request(SyncRequest::TerminateFileSync {
            tx_seq: self.next_tx_seq,
        });

        // re-sync files from the reverted tx seq
        self.next_tx_seq = reverted_tx;
    }
}
