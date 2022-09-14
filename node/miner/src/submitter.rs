use contract_interface::PoraAnswer;
use contract_interface::{IonianFlow, IonianMine};
use ethers::providers::PendingTransaction;
use std::sync::Arc;
use task_executor::TaskExecutor;
use tokio::sync::mpsc;

use crate::config::{MineServiceMiddleware, MinerConfig};
use crate::pora::{AnswerWithoutProof, SECTORS_PER_SEAL};

const SUBMISSION_RETIES: usize = 3;

pub struct Submitter {
    mine_answer_receiver: mpsc::UnboundedReceiver<AnswerWithoutProof>,
    mine_contract: IonianMine<MineServiceMiddleware>,
    flow_contract: IonianFlow<MineServiceMiddleware>,
}

impl Submitter {
    pub fn spawn(
        executor: TaskExecutor,
        mine_answer_receiver: mpsc::UnboundedReceiver<AnswerWithoutProof>,
        provider: Arc<MineServiceMiddleware>,
        config: &MinerConfig,
    ) {
        let mine_contract = IonianMine::new(config.mine_address, provider.clone());
        let flow_contract = IonianFlow::new(config.flow_address, provider);

        let submitter = Submitter {
            mine_answer_receiver,
            mine_contract,
            flow_contract,
        };
        executor.spawn(
            async move { Box::pin(submitter.start()).await },
            "mine_answer_submitter",
        );
    }

    async fn start(mut self) {
        loop {
            match self.mine_answer_receiver.recv().await {
                Some(answer) => {
                    if let Err(e) = self.submit_answer(answer).await {
                        warn!(e)
                    }
                }
                None => {
                    warn!("Mine submitter stopped because mine answer channel is closed.");
                    break;
                }
            };
        }
    }

    async fn submit_answer(&mut self, mine_answer: AnswerWithoutProof) -> Result<(), String> {
        let sealed_context_digest = self
            .flow_contract
            .query_context_at_position(
                (mine_answer.recall_position + SECTORS_PER_SEAL as u64) as u128,
            )
            .call()
            .await
            .map_err(|e| format!("Failed to fetch sealed contest digest: {:?}", e))?;
        debug!("Fetch sealed context: {:?}", sealed_context_digest);

        let answer = PoraAnswer {
            context_digest: mine_answer.context_digest.0,
            nonce: mine_answer.nonce.0,
            miner_id: mine_answer.miner_id.0,
            start_position: mine_answer.start_position.into(),
            mine_length: mine_answer.mining_length.into(),
            recall_position: mine_answer.recall_position.into(),
            seal_offset: mine_answer.seal_offset.into(),
            sealed_context_digest: sealed_context_digest.digest, // TODO(kevin): wait for implementation of data sealing and proof.
            sealed_data: unsafe { std::mem::transmute(mine_answer.sealed_data) },
            merkle_proof: vec![],
        };

        let submission_call = self.mine_contract.submit(answer).legacy();
        let pending_transaction: PendingTransaction<'_, _> = submission_call
            .send()
            .await
            .map_err(|e| format!("Fail to send mine answer transaction: {:?}", e))?;

        let receipt = pending_transaction
            .retries(SUBMISSION_RETIES)
            .await
            .map_err(|e| format!("Fail to execute mine answer transaction: {:?}", e))?
            .ok_or(format!(
                "Mine answer transaction dropped after {} retires",
                SUBMISSION_RETIES
            ))?;

        info!("Submit PoRA sucess");
        debug!("Receipt: {:?}", receipt);

        Ok(())
    }
}
