use crate::{SyncRequest, SyncSender};
use anyhow::{bail, Result};
use ssz::{Decode, Encode};
use std::time::Duration;
use storage_async::Store;
use tokio::time::sleep;

const INTERVAL_CATCHUP: Duration = Duration::from_millis(1);
const INTERVAL: Duration = Duration::from_secs(3);
const INTERVAL_ERROR: Duration = Duration::from_secs(10);

const KEY_NEXT_TX_SEQ: &[u8] = "sync.manager.next_tx_seq".as_bytes();

pub struct SyncManager {
    next_tx_seq: u64,
    store: Store,
    sender: SyncSender,
}

impl SyncManager {
    pub fn new(store: Store, sender: SyncSender) -> Self {
        Self {
            next_tx_seq: 0,
            store,
            sender,
        }
    }

    pub async fn start(&mut self) {
        match self.load_next_tx_seq_or_default().await {
            Ok(val) => self.next_tx_seq = val,
            Err(err) => error!(%err, "Failed to load sync tx seq"),
        }

        debug!(%self.next_tx_seq, "Start to sync file periodically");

        loop {
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
}
