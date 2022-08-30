use crate::contracts::{IonianFlow, SubmissionFilter};
use crate::rpc_proxy::ContractAddress;
use anyhow::{anyhow, Result};
use append_merkle::{Algorithm, Sha3Algorithm};
use ethers::prelude::builders::Event;
use ethers::prelude::{BlockNumber, Http, Log, Middleware, Provider, U256};
use ethers::providers::FilterKind;
use ethers::types::H256;
use futures::StreamExt;
use jsonrpsee::tracing::{debug, error};
use shared_types::{DataRoot, Transaction};
use std::sync::Arc;
use std::time::Duration;
use task_executor::TaskExecutor;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

const LOG_PAGE_SIZE: u64 = 1000;

pub struct LogEntryFetcher {
    contract_address: ContractAddress,
    provider: Arc<Provider<Http>>,
}

impl LogEntryFetcher {
    pub async fn new(url: &str, contract_address: ContractAddress) -> Result<Self> {
        let provider = Arc::new(Provider::try_from(url)?);
        // TODO: `error` types are removed from the ABI json file.
        Ok(Self {
            contract_address,
            provider,
        })
    }

    pub fn start_recover(
        &self,
        start_block_number: u64,
        executor: &TaskExecutor,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let provider = self.provider.clone();
        let (recover_tx, recover_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = IonianFlow::new(self.contract_address, provider.clone());
        executor.spawn(
            async move {
                let events = contract.submission_filter().from_block(start_block_number);
                let mut stream = provider.get_logs_paginated(&events.filter, LOG_PAGE_SIZE);
                debug!("start_recover starts, start={}", start_block_number);
                while let Some(maybe_log) = stream.next().await {
                    match maybe_log {
                        Ok(log) => {
                            let sync_progress =
                                if log.block_hash.is_some() && log.block_number.is_some() {
                                    let synced_block = LogFetchProgress::SyncedBlock((
                                        log.block_number.unwrap().as_u64(),
                                        log.block_hash.unwrap(),
                                    ));
                                    Some(synced_block)
                                } else {
                                    None
                                };

                            match events.parse_log(log) {
                                Ok(event) => {
                                    if let Err(e) = recover_tx
                                        .send(submission_event_to_transaction(event))
                                        .and_then(|_| match sync_progress {
                                            Some(b) => recover_tx.send(b),
                                            None => Ok(()),
                                        })
                                    {
                                        error!("send error: e={:?}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("log decode error: e={:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("log query error: e={:?}", e);
                        }
                    }
                }
            },
            "log recover",
        );
        recover_rx
    }

    pub fn start_watch(
        &self,
        start_block_number: u64,
        executor: &TaskExecutor,
    ) -> UnboundedReceiver<LogFetchProgress> {
        let (watch_tx, watch_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = IonianFlow::new(self.contract_address, self.provider.clone());
        let provider = self.provider.clone();
        executor.spawn(
            async move {
                let events = contract.submission_filter().from_block(start_block_number);
                debug!("start_watch starts, start={}", start_block_number);
                let filter_id = match provider.new_filter(FilterKind::Logs(&events.filter)).await {
                    Ok(id) => id,
                    Err(e) => {
                        error!("start_watch error: e={:?}", e);
                        return;
                    }
                };

                loop {
                    if let Err(e) =
                        Self::watch_loop(provider.as_ref(), filter_id, &events, &watch_tx).await
                    {
                        error!("log sync watch error: e={:?}", e);
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            },
            "log watch",
        );
        watch_rx
    }

    async fn watch_loop(
        provider: &Provider<Http>,
        filter_id: U256,
        events: &Event<'_, Provider<Http>, SubmissionFilter>,
        watch_tx: &UnboundedSender<LogFetchProgress>,
    ) -> Result<()> {
        let latest_block = provider
            .get_block(BlockNumber::Latest)
            .await?
            .ok_or_else(|| anyhow!("None for latest block"))?;
        let logs: Vec<Log> = provider.get_filter_changes(filter_id).await?;
        for log in logs {
            // Reverted log should not be processed. Since we revert back to a previous tx_seq
            // directly, we also do not need to notify with reverted logs.
            if !log.removed.unwrap_or(false) {
                // TODO(zz): Log parse error means logs might be lost here.
                let tx = events.parse_log(log)?;
                watch_tx.send(submission_event_to_transaction(tx))?;
            }
        }
        if latest_block.hash.is_some() && latest_block.number.is_some() {
            watch_tx.send(LogFetchProgress::SyncedBlock((
                latest_block.number.unwrap().as_u64(),
                latest_block.hash.unwrap(),
            )))?;
        }
        Ok(())
    }

    pub fn provider(&self) -> &Provider<Http> {
        self.provider.as_ref()
    }
}

#[derive(Debug)]
pub enum LogFetchProgress {
    SyncedBlock((u64, H256)),
    Transaction(Transaction),
}

fn submission_event_to_transaction(e: SubmissionFilter) -> LogFetchProgress {
    LogFetchProgress::Transaction(Transaction {
        stream_ids: vec![],
        data: vec![],
        data_merkle_root: nodes_to_root(&e.submission.1),
        merkle_nodes: e
            .submission
            .1
            .iter()
            // the submission height is the height of the root node starting from height 0.
            .map(|(root, height)| (height.as_usize() + 1, root.into()))
            .collect(),
        start_entry_index: e.start_pos.as_u64(),
        size: e.submission.0.as_u64(),
        seq: e.submission_index.as_u64(),
    })
}

fn nodes_to_root(node_list: &Vec<([u8; 32], U256)>) -> DataRoot {
    let mut root: DataRoot = node_list.last().expect("not empty").0.into();
    for (next_node, _) in node_list[..node_list.len() - 1].iter().rev() {
        root = Sha3Algorithm::parent(&next_node.into(), &root);
    }
    root
}
