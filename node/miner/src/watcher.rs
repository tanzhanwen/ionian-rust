#![allow(unused)]

use contract_interface::{ionian_flow::MineContext, IonianFlow, IonianMine};
use ethereum_types::{Address, H256, U256};
use ethers::{
    contract::Contract,
    providers::{JsonRpcClient, Middleware, Provider, StreamExt},
    types::BlockId,
};
use task_executor::TaskExecutor;
use tokio::{
    sync::{broadcast, mpsc},
    try_join,
};

use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use crate::{MinerConfig, MinerMessage};

pub type MineContextMessage = Option<(MineContext, U256)>;

lazy_static! {
    pub static ref EMPTY_HASH: H256 =
        H256::from_str("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470").unwrap();
}

pub struct MineContextWatcher<P: JsonRpcClient + 'static> {
    provider: Arc<Provider<P>>,
    flow_contract: IonianFlow<Provider<P>>,
    mine_contract: IonianMine<Provider<P>>,

    mine_context_sender: mpsc::UnboundedSender<MineContextMessage>,

    msg_recv: broadcast::Receiver<MinerMessage>,
}

impl<P: JsonRpcClient + 'static> MineContextWatcher<P> {
    pub fn spawn(
        executor: TaskExecutor,
        msg_recv: broadcast::Receiver<MinerMessage>,
        provider: Arc<Provider<P>>,
        config: &MinerConfig,
    ) -> mpsc::UnboundedReceiver<MineContextMessage> {
        let provider = provider;

        let mine_contract = IonianMine::new(config.mine_address, provider.clone());
        let flow_contract = IonianFlow::new(config.flow_address, provider.clone());

        let (mine_context_sender, mine_context_receiver) =
            mpsc::unbounded_channel::<MineContextMessage>();
        let watcher = MineContextWatcher {
            provider,
            flow_contract,
            mine_contract,
            mine_context_sender,
            msg_recv,
        };
        executor.spawn(
            async move { Box::pin(watcher.start()).await },
            "mine_context_watcher",
        );
        mine_context_receiver
    }

    async fn start(mut self) {
        let mut mining_enabled = true;
        let mut channel_opened = true;

        loop {
            tokio::select! {
                biased;

                v = self.msg_recv.recv(), if channel_opened => {
                    match v {
                        Ok(MinerMessage::ToggleMining(enable)) => {
                            mining_enabled = enable;
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            channel_opened = false;
                        }
                        _ => {}
                    }
                }

                _ = async {}, if mining_enabled => {
                    if let Err(err) = self.query_recent_context().await{
                        warn!(err);
                    }
                }
            }
        }
    }

    async fn query_recent_context(&self) -> Result<(), String> {
        // let mut watcher = self
        //     .provider
        //     .watch_blocks()
        //     .await
        //     .expect("should success")
        //     .stream();
        // watcher.next().await
        let context_call = self.flow_contract.make_context_with_result();
        let epoch_call = self.mine_contract.last_mined_epoch();
        let quality_call = self.mine_contract.target_quality();

        let (context, epoch, quality) =
            try_join!(context_call.call(), epoch_call.call(), quality_call.call())
                .map_err(|e| format!("Failed to query mining context: {:?}", e))?;
        if context.epoch > epoch && context.digest != EMPTY_HASH.0 {
            self.mine_context_sender
                .send(Some((context, quality)))
                .map_err(|e| format!("Failed to send out the most recent mine context: {:?}", e))?;
        } else {
            self.mine_context_sender
                .send(None)
                .map_err(|e| format!("Failed to send out the most recent mine context: {:?}", e))?;
        }
        Ok(())
    }
}
