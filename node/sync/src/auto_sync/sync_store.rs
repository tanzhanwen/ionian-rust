use super::tx_store::TxStore;
use anyhow::Result;
use std::ops::Deref;
use storage::log_store::config::{ConfigTx, ConfigurableExt};
use storage_async::Store;

const KEY_NEXT_TX_SEQ: &str = "sync.manager.next_tx_seq";
const KEY_MAX_TX_SEQ: &str = "sync.manager.max_tx_seq";

#[derive(Clone)]
pub struct SyncStore {
    store: Store,

    /// Pending transactions to sync with low priority.
    pending_txs: TxStore,

    /// Ready transactions to sync with high priority since announcement
    /// already received from other peers.
    ready_txs: TxStore,
}

impl SyncStore {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            pending_txs: TxStore::new("pending"),
            ready_txs: TxStore::new("ready"),
        }
    }

    pub async fn get_tx_seq_range(&self) -> Result<(u64, u64)> {
        let store = self.store.get_store().read().await;

        // load next_tx_seq
        let next_tx_seq = store.get_config_decoded(&KEY_NEXT_TX_SEQ)?.unwrap_or(0);

        // load max_tx_seq
        let max_tx_seq = match store.get_config_decoded(&KEY_MAX_TX_SEQ)? {
            Some(val) => val,
            // use the next_tx_seq for log sync by default
            // TODO(qhz) what if storage node fall behind a lot?
            None => store.next_tx_seq()?,
        };

        Ok((next_tx_seq, max_tx_seq))
    }

    pub async fn set_next_tx_seq(&self, tx_seq: u64) -> Result<()> {
        self.store
            .get_store()
            .write()
            .await
            .set_config_encoded(&KEY_NEXT_TX_SEQ, &tx_seq)
    }

    pub async fn set_max_tx_seq(&self, tx_seq: u64) -> Result<()> {
        self.store
            .get_store()
            .write()
            .await
            .set_config_encoded(&KEY_MAX_TX_SEQ, &tx_seq)
    }

    pub async fn add_pending_tx(&self, tx_seq: u64) -> Result<bool> {
        let store = self.store.get_store().write().await;

        // already in ready queue
        if self.ready_txs.has(store.deref(), tx_seq)? {
            return Ok(false);
        }

        // always add in pending queue
        self.pending_txs.add(store.deref(), None, tx_seq)
    }

    pub async fn upgrade_tx_to_ready(&self, tx_seq: u64) -> Result<bool> {
        let store = self.store.get_store().write().await;

        let mut tx = ConfigTx::default();

        // not in pending queue
        if !self
            .pending_txs
            .remove(store.deref(), Some(&mut tx), tx_seq)?
        {
            return Ok(false);
        }

        // move from pending to ready queue
        let added = self.ready_txs.add(store.deref(), Some(&mut tx), tx_seq)?;

        store.exec_configs(tx)?;

        Ok(added)
    }

    pub async fn downgrade_tx_to_pending(&self, tx_seq: u64) -> Result<bool> {
        let store = self.store.get_store().write().await;

        let mut tx = ConfigTx::default();

        // not in ready queue
        if !self
            .ready_txs
            .remove(store.deref(), Some(&mut tx), tx_seq)?
        {
            return Ok(false);
        }

        // move from ready to pending queue
        let added = self.pending_txs.add(store.deref(), Some(&mut tx), tx_seq)?;

        store.exec_configs(tx)?;

        Ok(added)
    }

    pub async fn random_tx(&self) -> Result<Option<u64>> {
        let store = self.store.get_store().read().await;

        // try to find a tx in ready queue with high priority
        if let Some(val) = self.ready_txs.random(store.deref())? {
            return Ok(Some(val));
        }

        // otherwise, find tx in pending queue
        self.pending_txs.random(store.deref())
    }

    pub async fn remove_tx(&self, tx_seq: u64) -> Result<bool> {
        let store = self.store.get_store().write().await;

        // removed in ready queue
        if self.ready_txs.remove(store.deref(), None, tx_seq)? {
            return Ok(true);
        }

        // otherwise, try to remove in pending queue
        self.pending_txs.remove(store.deref(), None, tx_seq)
    }
}
