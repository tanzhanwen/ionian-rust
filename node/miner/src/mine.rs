use contract_interface::ionian_flow::MineContext;
use ethereum_types::{H256, U256};
use rand::{self, Rng};
use task_executor::TaskExecutor;
use tokio::sync::{broadcast, mpsc};

use crate::{
    pora::{
        AnswerWithoutProof, Miner, SECTORS_PER_LOADING, SECTORS_PER_MAX_MINING_RANGE,
        SECTORS_PER_PRICING,
    },
    watcher::MineContextMessage,
    MinerConfig, MinerMessage, PoraLoader,
};

use std::sync::Arc;

pub struct PoraService {
    mine_context_receiver: mpsc::UnboundedReceiver<MineContextMessage>,
    mine_answer_sender: mpsc::UnboundedSender<AnswerWithoutProof>,
    msg_recv: broadcast::Receiver<MinerMessage>,
    loader: Arc<dyn PoraLoader>,

    puzzle: Option<PoraPuzzle>,
    mine_range: Option<CustomMineRange>,
    miner_id: Option<H256>,
}

struct PoraPuzzle {
    context: MineContext,
    target_quality: U256,
}
#[derive(Clone, Copy, Debug)]
pub struct CustomMineRange {
    start_position: u64,
    mining_length: u64,
}

impl CustomMineRange {
    #[inline]
    fn to_valid_range(self, context: &MineContext) -> (u64, u64) {
        let minable_length = (context.flow_length.as_u64() / SECTORS_PER_LOADING as u64)
            * SECTORS_PER_LOADING as u64;

        let mining_length = std::cmp::min(minable_length, SECTORS_PER_MAX_MINING_RANGE as u64);

        let start_position = std::cmp::min(self.start_position, minable_length - mining_length);
        let start_position =
            (start_position / SECTORS_PER_PRICING as u64) * SECTORS_PER_PRICING as u64;
        (start_position, mining_length)
    }

    #[inline]
    pub(crate) fn is_covered(&self, recall_position: u64) -> bool {
        self.start_position <= recall_position + SECTORS_PER_LOADING as u64
            || self.start_position + self.mining_length > recall_position
    }
}

impl PoraService {
    pub fn spawn(
        executor: TaskExecutor,
        msg_recv: broadcast::Receiver<MinerMessage>,
        mine_context_receiver: mpsc::UnboundedReceiver<MineContextMessage>,
        loader: Arc<dyn PoraLoader>,
        config: &MinerConfig,
    ) -> mpsc::UnboundedReceiver<AnswerWithoutProof> {
        let (mine_answer_sender, mine_answer_receiver) =
            mpsc::unbounded_channel::<AnswerWithoutProof>();
        let pora = PoraService {
            mine_context_receiver,
            mine_answer_sender,
            msg_recv,
            puzzle: None,
            mine_range: None,
            miner_id: Some(config.miner_id),
            loader,
        };
        executor.spawn(async move { Box::pin(pora.start()).await }, "pora_master");
        mine_answer_receiver
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
                        Ok(MinerMessage::ChangeMiningRange(range)) => {
                            self.mine_range = Some(range);
                        }
                        Err(broadcast::error::RecvError::Closed)=>{
                            channel_opened = false;
                        }
                        Err(_)=>{

                        }
                    }
                }

                maybe_msg = self.mine_context_receiver.recv() => {
                    if let Some(msg) = maybe_msg {
                        self.puzzle = msg.map(|(context, target_quality)| PoraPuzzle {
                            context, target_quality
                        });
                    }
                }

                _ = async {}, if mining_enabled && self.as_miner().is_some() => {
                    let nonce = H256(rand::thread_rng().gen());
                    let miner = self.as_miner().unwrap();
                    if let Some(answer) = miner.iteration(nonce).await{
                        self.mine_answer_sender.send(answer).unwrap();
                    }
                }
            }
        }
    }

    #[inline]
    fn as_miner(&self) -> Option<Miner> {
        match (
            self.puzzle.as_ref(),
            self.mine_range.as_ref(),
            self.miner_id.as_ref(),
        ) {
            (Some(puzzle), Some(mine_range), Some(miner_id)) => {
                let (start_position, mining_length) = mine_range.to_valid_range(&puzzle.context);
                Some(Miner {
                    start_position,
                    mining_length,
                    miner_id,
                    custom_mine_range: mine_range,
                    context: &puzzle.context,
                    target_quality: &puzzle.target_quality,
                    loader: &*self.loader,
                })
            }
            _ => None,
        }
    }
}
