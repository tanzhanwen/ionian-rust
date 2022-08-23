use crate::error::Error;
use crate::log_store::log_manager::{
    sub_merkle_tree, COL_TX, COL_TX_COMPLETED, COL_TX_DATA_ROOT_INDEX, ENTRY_SIZE,
};
use crate::{try_option, IonianKeyValueDB};
use anyhow::{anyhow, Result};
use shared_types::{DataRoot, Transaction};
use ssz::{Decode, Encode};
use std::sync::Arc;

pub struct TransactionStore {
    kvdb: Arc<dyn IonianKeyValueDB>,
}

impl TransactionStore {
    pub fn new(kvdb: Arc<dyn IonianKeyValueDB>) -> Self {
        Self { kvdb }
    }

    pub fn put_tx(&self, mut tx: Transaction) -> Result<()> {
        let mut db_tx = self.kvdb.transaction();

        if !tx.data.is_empty() {
            tx.size = tx.data.len() as u64;
            let mut padded_data = tx.data.clone();
            let extra = tx.data.len() % ENTRY_SIZE;
            if extra != 0 {
                padded_data.append(&mut vec![0u8; ENTRY_SIZE - extra]);
            }
            let data_root = sub_merkle_tree(&padded_data)?.root();
            tx.data_merkle_root = data_root.into();
        }

        db_tx.put(COL_TX, &tx.seq.to_be_bytes(), &tx.as_ssz_bytes());
        if self
            .get_tx_seq_by_data_root(&tx.data_merkle_root)?
            .is_none()
        {
            db_tx.put(
                COL_TX_DATA_ROOT_INDEX,
                tx.data_merkle_root.as_bytes(),
                &tx.seq.to_be_bytes(),
            );
        }

        self.kvdb.write(db_tx)?;
        Ok(())
    }

    pub fn get_tx_by_seq_number(&self, seq: u64) -> Result<Option<Transaction>> {
        let value = try_option!(self.kvdb.get(COL_TX, &seq.to_be_bytes())?);
        let tx = Transaction::from_ssz_bytes(&value).map_err(Error::from)?;
        Ok(Some(tx))
    }

    pub fn get_tx_seq_by_data_root(&self, data_root: &DataRoot) -> Result<Option<u64>> {
        let value = try_option!(self
            .kvdb
            .get(COL_TX_DATA_ROOT_INDEX, data_root.as_bytes())?);
        Ok(Some(decode_tx_seq(&value)?))
    }

    pub fn finalize_tx(&self, tx_seq: u64) -> Result<()> {
        Ok(self
            .kvdb
            .put(COL_TX_COMPLETED, &tx_seq.to_be_bytes(), &[0])?)
    }

    pub fn check_tx_completed(&self, tx_seq: u64) -> Result<bool> {
        Ok(self.kvdb.has_key(COL_TX_COMPLETED, &tx_seq.to_be_bytes())?)
    }

    pub fn next_tx_seq(&self) -> Result<u64> {
        // TODO: `kvdb` and `kvdb-rocksdb` does not support `seek_to_last` yet.
        // We'll need to fork it or use another wrapper for a better performance in this.
        self.kvdb
            .iter(COL_TX)
            .last()
            .map(|(k, _)| decode_tx_seq(k.as_ref()).map(|seq| seq + 1))
            .unwrap_or(Ok(0))
    }
}

fn decode_tx_seq(data: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        data.try_into().map_err(|e| anyhow!("{:?}", e))?,
    ))
}
