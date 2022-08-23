use crate::contracts::{IonianFlow, SubmissionFilter};
use crate::rpc_proxy::ContractAddress;
use anyhow::Result;
use append_merkle::{Algorithm, Sha3Algorithm};
use ethers::prelude::{Http, Middleware, Provider, U256};
use futures::StreamExt;
use jsonrpsee::tracing::{debug, error};
use shared_types::{DataRoot, Transaction};
use std::sync::Arc;
use task_executor::TaskExecutor;
use tokio::sync::mpsc::UnboundedReceiver;

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
    ) -> UnboundedReceiver<Transaction> {
        let provider = self.provider.clone();
        let (recover_tx, recover_rx) = tokio::sync::mpsc::unbounded_channel();
        // TODO(zz): Do not create two contracts.
        let contract = IonianFlow::new(self.contract_address, provider.clone());
        executor.spawn(
            async move {
                let events = contract.submission_filter().from_block(start_block_number);
                let mut stream = provider.get_logs_paginated(&events.filter, LOG_PAGE_SIZE);
                debug!("start_recover starts, start={}", start_block_number);
                while let Some(maybe_log) = stream.next().await {
                    match maybe_log {
                        Ok(log) => match events.parse_log(log) {
                            Ok(event) => {
                                if let Err(e) =
                                    recover_tx.send(submission_event_to_transaction(event))
                                {
                                    error!("send error: e={:?}", e);
                                }
                            }
                            Err(e) => {
                                error!("log decode error: e={:?}", e);
                            }
                        },
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
    ) -> UnboundedReceiver<Transaction> {
        let (watch_tx, watch_rx) = tokio::sync::mpsc::unbounded_channel();
        let contract = IonianFlow::new(self.contract_address, self.provider.clone());
        executor.spawn(
            async move {
                let events = contract.submission_filter().from_block(start_block_number);
                debug!("start_watch starts, start={}", start_block_number);
                let mut stream = loop {
                    if let Ok(stream) = events.stream().await {
                        break stream;
                    }
                };
                debug!("start_watch: stream get, start={}", start_block_number);
                while let Some(e) = stream.next().await {
                    match e {
                        Ok(e) => {
                            debug!("get submission events: e={:?}", e);
                            if let Err(e) = watch_tx.send(submission_event_to_transaction(e)) {
                                error!("send error: e={:?}", e);
                            }
                        }
                        Err(e) => {
                            // This should never happen for an honest provider?
                            error!("Unexpected log decoding error: e={:?}", e);
                        }
                    }
                }
            },
            "log watch",
        );
        watch_rx
    }
}

fn submission_event_to_transaction(e: SubmissionFilter) -> Transaction {
    Transaction {
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
    }
}

fn nodes_to_root(node_list: &Vec<([u8; 32], U256)>) -> DataRoot {
    let mut root: DataRoot = node_list.last().expect("not empty").0.into();
    for (next_node, _) in node_list[..node_list.len() - 1].iter().rev() {
        root = Sha3Algorithm::parent(&next_node.into(), &root);
    }
    root
}
