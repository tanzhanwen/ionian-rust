use super::sync_store::SyncStore;
use crate::{controllers::SyncState, SyncRequest, SyncResponse, SyncSender};
use anyhow::{bail, Result};
use log_entry_sync::LogSyncEvent;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use storage_async::Store;
use task_executor::TaskExecutor;
use tokio::sync::broadcast::{error::RecvError, Receiver};
use tokio::time::sleep;

const INTERVAL_CATCHUP: Duration = Duration::from_millis(1);
const INTERVAL: Duration = Duration::from_secs(3);
const INTERVAL_ERROR: Duration = Duration::from_secs(10);

const FIND_PEERS_TIMEOUT: Duration = Duration::from_secs(60);

/// Manager to synchronize files among storage nodes automatically.
///
/// Generally, most files could be synchronized among storage nodes. However, a few
/// files may be unavailable on all storage nodes, e.g.
///
/// 1. File not uploaded by user in time.
/// 2. File removed due to blockchain reorg, and user do not upload again.
///
/// So, there are 2 workers to synchronize files in parallel:
///
/// 1. Synchronize announced files in sequence. If any file unavailable, store it into db.
/// 2. Synchronize the missed files in sequential synchronization.
#[derive(Clone)]
pub struct Manager {
    /// The next `tx_seq` to sync in sequence.
    next_tx_seq: Arc<AtomicU64>,

    /// The maximum `tx_seq` to sync in sequence.
    /// Generally, it is updated when file announcement received.
    max_tx_seq: Arc<AtomicU64>,

    /// The last reverted transaction seq, `u64::MAX` means no tx reverted.
    /// Generally, it is updated when transaction reverted.
    reverted_tx_seq: Arc<AtomicU64>,

    store: Store,
    sync_store: SyncStore,

    /// Used to interact with sync service for the current file in sync.
    sync_send: SyncSender,
}

impl Manager {
    pub async fn new(store: Store, sync_send: SyncSender) -> Result<Self> {
        let sync_store = SyncStore::new(store.clone());

        let (next_tx_seq, max_tx_seq) = sync_store.get_tx_seq_range().await?;

        Ok(Self {
            next_tx_seq: Arc::new(AtomicU64::new(next_tx_seq)),
            max_tx_seq: Arc::new(AtomicU64::new(max_tx_seq)),
            reverted_tx_seq: Arc::new(AtomicU64::new(u64::MAX)),
            store,
            sync_store,
            sync_send,
        })
    }

    pub fn spwn(&self, executor: &TaskExecutor, receiver: Receiver<LogSyncEvent>) {
        executor.spawn(
            monitor_reorg(self.clone(), receiver),
            "sync_manager_reorg_monitor",
        );

        executor.spawn(start_sync(self.clone()), "sync_manager_sequential_syncer");

        executor.spawn(
            start_sync_pending_txs(self.clone()),
            "sync_manager_pending_syncer",
        );
    }

    fn handle_on_reorg(&self) {
        let reverted_tx_seq = self.reverted_tx_seq.load(Ordering::Relaxed);

        // no reorg happened
        if reverted_tx_seq == u64::MAX {
            return;
        }

        self.reverted_tx_seq.store(u64::MAX, Ordering::Relaxed);

        // reorg happened, but no impact on file sync
        let next_tx_seq = self.next_tx_seq.load(Ordering::Relaxed);
        if reverted_tx_seq > next_tx_seq {
            return;
        }

        // handles on reorg
        info!(%reverted_tx_seq, %next_tx_seq, "Transaction reverted");

        // request sync service to terminate the file sync immediately
        let _ = self.sync_send.request(SyncRequest::TerminateFileSync {
            tx_seq: next_tx_seq,
        });

        // re-sync files from the reverted tx seq
        self.next_tx_seq.store(reverted_tx_seq, Ordering::Relaxed);
    }

    pub async fn update_on_announcement(&mut self, announced_tx_seq: u64) {
        // new file announced
        if announced_tx_seq > self.max_tx_seq.load(Ordering::Relaxed) {
            match self.sync_store.set_max_tx_seq(announced_tx_seq).await {
                Ok(()) => self.max_tx_seq.store(announced_tx_seq, Ordering::Relaxed),
                Err(e) => error!(%e, "Failed to set max_tx_seq in store"),
            };
            return;
        }

        // already wait for sequential sync
        if announced_tx_seq >= self.next_tx_seq.load(Ordering::Relaxed) {
            return;
        }

        // otherwise, mark tx as ready for sync
        if let Err(e) = self.sync_store.upgrade_tx_to_ready(announced_tx_seq).await {
            error!(%e, "Failed to promote announced tx to ready");
        }
    }

    /// Returns whether file sync in progress but no peers found
    async fn sync_tx(&self, tx_seq: u64) -> Result<bool> {
        // tx not available yet
        if self.store.get_tx_by_seq_number(tx_seq).await?.is_none() {
            return Ok(false);
        }

        // get sync state to handle in advance
        let state = match self
            .sync_send
            .request(SyncRequest::SyncStatus { tx_seq })
            .await?
        {
            SyncResponse::SyncStatus { status } => status,
            _ => bail!("invalid sync response type"),
        };

        // notify service to sync file if not started or failed
        if matches!(state, None | Some(SyncState::Failed { .. })) {
            self.sync_send
                .request(SyncRequest::SyncFile { tx_seq })
                .await?;

            return Ok(false);
        }

        if matches!(state, Some(SyncState::FindingPeers { since }) if since.elapsed() > FIND_PEERS_TIMEOUT)
        {
            // no peers found for a long time
            Ok(true)
        } else {
            // otherwise, continue to wait for file sync that already in progress
            Ok(false)
        }
    }
}

/// Starts to monitor reorg and handle on transaction reverted.
async fn monitor_reorg(manager: Manager, mut receiver: Receiver<LogSyncEvent>) {
    info!("Start to monitor reorg");

    loop {
        match receiver.recv().await {
            Ok(LogSyncEvent::ReorgDetected { .. }) => {}
            Ok(LogSyncEvent::Reverted { tx_seq }) => {
                // requires to re-sync files since transaction and files removed in storage
                if tx_seq < manager.reverted_tx_seq.load(Ordering::Relaxed) {
                    manager.reverted_tx_seq.store(tx_seq, Ordering::Relaxed);
                }
            }
            Err(RecvError::Closed) => {
                // program terminated
                info!("Completed to monitor reorg");
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
}

/// Starts to synchronize files in sequence.
async fn start_sync(manager: Manager) {
    info!(
        "Start to sync files periodically, next = {}, max = {}",
        manager.next_tx_seq.load(Ordering::Relaxed),
        manager.max_tx_seq.load(Ordering::Relaxed)
    );

    loop {
        // handles reorg before file sync
        manager.handle_on_reorg();

        // sync file
        let sync_result = sync_once(&manager).await;
        let next_tx_seq = manager.next_tx_seq.load(Ordering::Relaxed);
        match sync_result {
            Ok(true) => {
                debug!(%next_tx_seq, "Completed to sync file");
                sleep(INTERVAL_CATCHUP).await;
            }
            Ok(false) => {
                trace!(%next_tx_seq, "File in sync or log entry unavailable");
                sleep(INTERVAL).await;
            }
            Err(err) => {
                warn!(%err, %next_tx_seq, "Failed to sync file");
                sleep(INTERVAL_ERROR).await;
            }
        }
    }
}

async fn sync_once(manager: &Manager) -> Result<bool> {
    // already sync to the latest file
    let mut next_tx_seq = manager.next_tx_seq.load(Ordering::Relaxed);
    if next_tx_seq > manager.max_tx_seq.load(Ordering::Relaxed) {
        return Ok(false);
    }

    // already finalized
    if manager.store.check_tx_completed(next_tx_seq).await? {
        next_tx_seq += 1;
        manager.sync_store.set_next_tx_seq(next_tx_seq).await?;
        manager.next_tx_seq.store(next_tx_seq, Ordering::Relaxed);
        return Ok(true);
    }

    // try sync tx
    let no_peer_timeout = manager.sync_tx(next_tx_seq).await?;

    // put tx to pending list if no peers found for a long time
    if no_peer_timeout {
        manager.sync_store.add_pending_tx(next_tx_seq).await?;
        debug!(%next_tx_seq, "No peers found for a long time, put tx to pending list");
    }

    Ok(no_peer_timeout)
}

/// Starts to synchronize pending files that unavailable during sequential synchronization.
async fn start_sync_pending_txs(manager: Manager) {
    info!("Start to sync pending files");

    loop {
        let tx_seq = match manager.sync_store.random_tx().await {
            Ok(Some(seq)) => seq,
            Ok(None) => {
                trace!("No pending file to sync");
                sleep(INTERVAL).await;
                continue;
            }
            Err(err) => {
                warn!(%err, "Failed to pick pending file to sync");
                sleep(INTERVAL_ERROR).await;
                continue;
            }
        };

        match sync_pending_tx(&manager, tx_seq).await {
            Ok(true) => {
                debug!(%tx_seq, "Completed to sync pending file");
                sleep(INTERVAL_CATCHUP).await;
            }
            Ok(false) => {
                trace!(%tx_seq, "Pending file in sync or tx unavailable");
                sleep(INTERVAL).await;
            }
            Err(err) => {
                warn!(%err, %tx_seq, "Failed to sync pending file");
                sleep(INTERVAL_ERROR).await;
            }
        }
    }
}

async fn sync_pending_tx(manager: &Manager, tx_seq: u64) -> Result<bool> {
    // already finalized
    if manager.store.check_tx_completed(tx_seq).await? {
        manager.sync_store.remove_tx(tx_seq).await?;
        return Ok(true);
    }

    // try sync tx
    let no_peer_timeout = manager.sync_tx(tx_seq).await?;

    // downgrade if no peers found for a long time
    if no_peer_timeout && manager.sync_store.downgrade_tx_to_pending(tx_seq).await? {
        debug!(%tx_seq, "No peers found for pending file and downgraded");
    }

    Ok(no_peer_timeout)
}
