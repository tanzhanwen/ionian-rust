use crate::submitter::Submitter;
use crate::{config::MinerConfig, mine::PoraService, watcher::MineContextWatcher, PoraLoader};
use network::NetworkMessage;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum MinerMessage {
    /// Enable / Disable Mining
    ToggleMining(bool),

    /// Change mining range
    SetStartPosition(Option<u64>),
    SetEndPosition(Option<u64>),
}

pub struct MineService;

impl MineService {
    pub async fn spawn(
        executor: task_executor::TaskExecutor,
        _network_send: mpsc::UnboundedSender<NetworkMessage>,
        config: MinerConfig,
        loader: Arc<dyn PoraLoader>,
    ) -> Result<broadcast::Sender<MinerMessage>, String> {
        let provider = Arc::new(config.make_provider().await?);

        let (msg_send, msg_recv) = broadcast::channel(1024);

        let mine_context_receiver = MineContextWatcher::spawn(
            executor.clone(),
            msg_recv.resubscribe(),
            provider.clone(),
            &config,
        );

        let mine_answer_receiver = PoraService::spawn(
            executor.clone(),
            msg_recv.resubscribe(),
            mine_context_receiver,
            loader,
            &config,
        );

        Submitter::spawn(executor, mine_answer_receiver, provider, &config);

        debug!("Starting miner service");

        Ok(msg_send)
    }
}
