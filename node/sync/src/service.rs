use crate::context::SyncNetworkContext;
use crate::controllers::{SerialSyncController, SyncState};
use crate::manager::SyncManager;
use crate::Config;
use anyhow::{bail, Result};
use file_location_cache::FileLocationCache;
use network::{
    rpc::GetChunksRequest, rpc::RPCResponseErrorCode, Multiaddr, NetworkMessage, PeerAction,
    PeerId, PeerRequestId, SyncId as RequestId,
};
use shared_types::{bytes_to_chunks, ChunkArrayWithProof};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use storage::error::Result as StorageResult;
use storage::log_store::Store as LogStore;
use storage_async::Store;
use tokio::sync::{mpsc, RwLock};

const HEARTBEAT_INTERVAL_SEC: u64 = 5;

pub type SyncSender = channel::Sender<SyncMessage, SyncRequest, SyncResponse>;

#[derive(Debug)]
pub enum SyncMessage {
    DailFailed {
        peer_id: PeerId,
    },
    PeerConnected {
        peer_id: PeerId,
    },
    PeerDisconnected {
        peer_id: PeerId,
    },
    RequestChunks {
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    },
    ChunksResponse {
        peer_id: PeerId,
        request_id: RequestId,
        response: ChunkArrayWithProof,
    },
    RpcError {
        peer_id: PeerId,
        request_id: RequestId,
    },
    AnnounceFileGossip {
        tx_seq: u64,
        peer_id: PeerId,
        addr: Multiaddr,
    },
}

#[derive(Debug)]
pub enum SyncRequest {
    SyncStatus { tx_seq: u64 },
    SyncFile { tx_seq: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub enum SyncResponse {
    SyncStatus { status: String },
    SyncFile { err: String },
}

pub struct SyncService {
    /// A receiving channel sent by the message processor thread.
    msg_recv: channel::Receiver<SyncMessage, SyncRequest, SyncResponse>,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// Log and transaction storage.
    store: Store,

    /// Cache for storing and serving gossip messages.
    file_location_cache: Arc<FileLocationCache>,

    /// A collection of file sync controllers.
    controllers: HashMap<u64, SerialSyncController>,

    /// Heartbeat interval for executing periodic tasks.
    heartbeat: tokio::time::Interval,
}

impl SyncService {
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        store: Arc<RwLock<dyn LogStore>>,
        file_location_cache: Arc<FileLocationCache>,
    ) -> SyncSender {
        Self::spawn_with_config(
            Config::default(),
            executor,
            network_send,
            store,
            file_location_cache,
        )
    }

    pub fn spawn_with_config(
        config: Config,
        executor: task_executor::TaskExecutor,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        store: Arc<RwLock<dyn LogStore>>,
        file_location_cache: Arc<FileLocationCache>,
    ) -> SyncSender {
        let (sync_send, sync_recv) = channel::Channel::unbounded();

        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let store = Store::new(store, executor.clone());

        let mut sync = SyncService {
            msg_recv: sync_recv,
            ctx: Arc::new(SyncNetworkContext::new(network_send)),
            store: store.clone(),
            file_location_cache,
            controllers: Default::default(),
            heartbeat,
        };

        debug!("Starting sync service");
        executor.spawn(async move { Box::pin(sync.main()).await }, "sync");

        if !config.auto_sync_disabled {
            let mut manager = SyncManager::new(store, sync_send.clone());
            executor.spawn(
                async move { Box::pin(manager.start()).await },
                "auto_sync_file",
            );
        }

        sync_send
    }

    async fn main(&mut self) {
        loop {
            tokio::select! {
                // received sync message
                Some(msg) = self.msg_recv.recv() => {
                    match msg {
                        channel::Message::Notification(msg) => self.on_sync_msg(msg).await,
                        channel::Message::Request(req, sender) => self.on_sync_request(req, sender).await,
                    }
                }

                // heartbeat
                _ = self.heartbeat.tick() => self.on_heartbeat(),
            }
        }
    }

    async fn on_sync_msg(&mut self, msg: SyncMessage) {
        debug!("Sync received message {:?}", msg);

        match msg {
            SyncMessage::DailFailed { peer_id } => {
                self.on_dail_failed(peer_id);
            }
            SyncMessage::PeerConnected { peer_id } => {
                self.on_peer_connected(peer_id);
            }

            SyncMessage::PeerDisconnected { peer_id } => {
                self.on_peer_disconnected(peer_id);
            }

            SyncMessage::RequestChunks {
                request_id,
                peer_id,
                request,
            } => {
                self.on_get_chunks_request(peer_id, request_id, request)
                    .await;
            }

            SyncMessage::ChunksResponse {
                peer_id,
                request_id,
                response,
            } => {
                self.on_chunks_response(peer_id, request_id, response).await;
            }

            SyncMessage::RpcError {
                peer_id,
                request_id,
            } => {
                self.on_rpc_error(peer_id, request_id);
            }

            SyncMessage::AnnounceFileGossip {
                tx_seq,
                peer_id,
                addr,
            } => {
                self.on_announce_file_gossip(tx_seq, peer_id, addr).await;
            }
        }
    }

    async fn on_sync_request(
        &mut self,
        req: SyncRequest,
        sender: channel::ResponseSender<SyncResponse>,
    ) {
        match req {
            SyncRequest::SyncStatus { tx_seq } => {
                let status = match self.controllers.get_mut(&tx_seq) {
                    Some(controller) => format!("{:?}", controller.get_status()),
                    None => "unknown".to_string(),
                };

                let _ = sender.send(SyncResponse::SyncStatus { status });
            }

            SyncRequest::SyncFile { tx_seq } => {
                let err = match self.on_start_sync_file(tx_seq, None).await {
                    Ok(()) => "".into(),
                    Err(err) => err.to_string(),
                };

                let _ = sender.send(SyncResponse::SyncFile { err });
            }
        }
    }

    fn on_dail_failed(&mut self, peer_id: PeerId) {
        info!(%peer_id, "Dail to peer failed");

        for controller in self.controllers.values_mut() {
            controller.on_dail_failed(peer_id);
            controller.transition();
        }
    }

    fn on_peer_connected(&mut self, peer_id: PeerId) {
        info!(%peer_id, "Peer connected");

        for controller in self.controllers.values_mut() {
            controller.on_peer_connected(peer_id);
            controller.transition();
        }
    }

    fn on_peer_disconnected(&mut self, peer_id: PeerId) {
        info!(%peer_id, "Peer disconnected");

        for controller in self.controllers.values_mut() {
            controller.on_peer_disconnected(peer_id);
            controller.transition();
        }
    }

    async fn on_get_chunks_request(
        &mut self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    ) {
        info!(?request, %peer_id, ?request_id, "Received GetChunks request");

        if let Err(err) = self
            .handle_chunks_request_with_db_err(peer_id, request_id, request)
            .await
        {
            error!(%err, "Failed to handle chunks request due to db error");
            self.ctx.send(NetworkMessage::SendErrorResponse {
                peer_id,
                id: request_id,
                error: RPCResponseErrorCode::ServerError,
                reason: "DB error".into(),
            });
        }
    }

    async fn handle_chunks_request_with_db_err(
        &mut self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: GetChunksRequest,
    ) -> StorageResult<()> {
        // ban peer for invalid chunk index range
        if request.index_start >= request.index_end {
            self.ctx.ban_peer(peer_id, "Invalid chunk indices");
            return Ok(());
        }

        // ban peer if invalid tx requested
        // TODO(qhz): add cache to get tx, which will not be removed
        let tx = match self.store.get_tx_by_seq_number(request.tx_seq).await? {
            Some(tx) => tx,
            None => {
                self.ctx.ban_peer(peer_id, "Tx not found");
                return Ok(());
            }
        };

        // ban peer if chunk index out of bound
        let num_chunks = bytes_to_chunks(tx.size as usize);
        if request.index_end as usize > num_chunks {
            self.ctx.ban_peer(peer_id, "Chunk index out of bound");
            return Ok(());
        }

        // file may be removed, but remote peer still find one from the file location cache
        let finalized = self.store.check_tx_completed(request.tx_seq).await?;
        if !finalized {
            info!(%request.tx_seq, "Failed to handle chunks request due to tx not finalized");
            self.ctx
                .report_peer(peer_id, PeerAction::MidToleranceError, "Tx not finalized");
            self.ctx.send(NetworkMessage::SendErrorResponse {
                peer_id,
                error: RPCResponseErrorCode::InvalidRequest,
                reason: "Tx not finalized".into(),
                id: request_id,
            });
            return Ok(());
        }

        let result = self
            .store
            .get_chunks_with_proof_by_tx_and_index_range(
                request.tx_seq,
                request.index_start as usize,
                request.index_end as usize,
            )
            .await?;

        match result {
            Some(chunks) => {
                self.ctx.send(NetworkMessage::SendResponse {
                    peer_id,
                    id: request_id,
                    response: network::Response::Chunks(chunks),
                });
            }
            None => {
                // file may be removed during downloading
                warn!(%request.tx_seq, "Failed to handle chunks request due to chunks not found");
                self.ctx.send(NetworkMessage::SendErrorResponse {
                    peer_id,
                    error: RPCResponseErrorCode::InvalidRequest,
                    reason: "Chunks not found".into(),
                    id: request_id,
                });
            }
        }

        Ok(())
    }

    async fn on_chunks_response(
        &mut self,
        peer_id: PeerId,
        request_id: RequestId,
        response: ChunkArrayWithProof,
    ) {
        info!(%response.chunks, %peer_id, ?request_id, "Received chunks response");

        let tx_seq = match request_id {
            RequestId::SerialSync { tx_seq } => tx_seq,
        };

        match self.controllers.get_mut(&tx_seq) {
            Some(controller) => {
                controller.on_response(peer_id, response).await;
                controller.transition();
            }
            None => {
                warn!("Received chunks response for non-existent controller tx_seq={tx_seq}");
            }
        }
    }

    fn on_rpc_error(&mut self, peer_id: PeerId, request_id: RequestId) {
        info!(%peer_id, ?request_id, "Received RPC error");

        let tx_seq = match request_id {
            RequestId::SerialSync { tx_seq } => tx_seq,
        };

        match self.controllers.get_mut(&tx_seq) {
            Some(controller) => {
                controller.on_request_failed(peer_id);
                controller.transition();
            }
            None => {
                warn!("Received rpc error for non-existent controller tx_seq={tx_seq}");
            }
        }
    }

    async fn on_start_sync_file(
        &mut self,
        tx_seq: u64,
        maybe_peer: Option<(PeerId, Multiaddr)>,
    ) -> Result<()> {
        info!(%tx_seq, "Start to sync file");

        let controller = match self.controllers.entry(tx_seq) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let tx = match self.store.get_tx_by_seq_number(tx_seq).await? {
                    Some(tx) => tx,
                    None => bail!("transaction not found"),
                };

                let num_chunks = match usize::try_from(tx.size) {
                    Ok(size) => bytes_to_chunks(size),
                    Err(_) => {
                        error!(%tx_seq, "Unexpected transaction size: {}", tx.size);
                        bail!("Unexpected transaction size");
                    }
                };

                // file already exists
                if self.store.check_tx_completed(tx_seq).await? {
                    bail!("File already exists");
                }

                entry.insert(SerialSyncController::new(
                    tx_seq,
                    tx.data_merkle_root,
                    num_chunks as u64,
                    self.ctx.clone(),
                    self.store.clone(),
                    self.file_location_cache.clone(),
                ))
            }
        };

        // trigger retry after failure
        if let SyncState::Failed { .. } = controller.get_status() {
            controller.reset();
        }

        if let Some((peer_id, addr)) = maybe_peer {
            controller.on_peer_found(peer_id, addr);
        }

        controller.transition();

        Ok(())
    }

    async fn on_announce_file_gossip(&mut self, tx_seq: u64, peer_id: PeerId, addr: Multiaddr) {
        info!(%tx_seq, %peer_id, %addr, "Received AnnounceFile gossip");

        // File already in sync
        if let Some(controller) = self.controllers.get_mut(&tx_seq) {
            controller.on_peer_found(peer_id, addr);
            controller.transition();
            return;
        }

        // File already exists and ignore the AnnounceFile message
        match self.store.check_tx_completed(tx_seq).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(err) => {
                error!(%tx_seq, %err, "Failed to check if file finalized");
                return;
            }
        }

        // Now, always sync files among all nodes
        if let Err(err) = self.on_start_sync_file(tx_seq, Some((peer_id, addr))).await {
            error!(%tx_seq, %err, "Failed to sync file");
        }
    }

    fn on_heartbeat(&mut self) {
        let mut completed = vec![];

        for (&tx_seq, controller) in self.controllers.iter_mut() {
            controller.transition();

            if let SyncState::Completed = controller.get_status() {
                completed.push(tx_seq);
            }
        }

        for tx_seq in completed {
            self.controllers.remove(&tx_seq);
        }

        // TODO(qhz): serial controller removed, but the peers are not disconnected.
        // If there are enough peers, the outgoing connections limitation will be reached
        // over time. So, sync service requires to say goodbye to some peers after file sync
        // completed. On the other hand, a few peers should be retained pub-sub messages.
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use libp2p::identity;
    use network::discovery::ConnectionId;
    use network::rpc::SubstreamId;
    use network::ReportSource;
    use shared_types::ChunkArray;
    use std::time::Duration;
    use std::time::Instant;
    use storage::log_store::log_manager::LogConfig;
    use storage::log_store::log_manager::LogManager;
    use storage::log_store::LogStoreRead;
    use task_executor::test_utils::TestRuntime;
    use tokio::sync::mpsc::UnboundedReceiver;

    use crate::test_util::tests::{create_2_store, create_file_location_cache};

    use super::*;

    #[tokio::test]
    async fn test_peer_connected_not_in_controller() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _, _) = create_2_store(vec![chunk_count]);
        let store = Store::new(store, runtime.task_executor.clone());

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();
        let (_, sync_recv) = channel::Channel::unbounded();

        let heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let mut sync = SyncService {
            msg_recv: sync_recv,
            ctx: Arc::new(SyncNetworkContext::new(network_send)),
            store,
            file_location_cache,
            controllers: Default::default(),
            heartbeat,
        };

        sync.on_peer_connected(init_peer_id);
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_peer_disconnected_not_in_controller() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _, _) = create_2_store(vec![chunk_count]);
        let store = Store::new(store, runtime.task_executor.clone());

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();
        let (_, sync_recv) = channel::Channel::unbounded();

        let heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let mut sync = SyncService {
            msg_recv: sync_recv,
            ctx: Arc::new(SyncNetworkContext::new(network_send)),
            store,
            file_location_cache,
            controllers: Default::default(),
            heartbeat,
        };

        sync.on_peer_disconnected(init_peer_id);
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_request_chunks() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _txs, data) = create_2_store(vec![chunk_count]);
        let data = data[0].clone();

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let request = GetChunksRequest {
            tx_seq: 0,
            index_start: 0,
            index_end: chunk_count as u64,
        };

        sync_send
            .notify(SyncMessage::RequestChunks {
                request_id: (ConnectionId::new(0), SubstreamId(0)),
                peer_id: init_peer_id,
                request,
            })
            .unwrap();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::SendResponse {
                    peer_id,
                    response,
                    id,
                } => match response {
                    network::Response::Chunks(response) => {
                        assert_eq!(peer_id, init_peer_id);
                        assert_eq!(id.0, ConnectionId::new(0));
                        assert_eq!(id.1 .0, 0);

                        let chunk_array = ChunkArray {
                            data,
                            start_index: 0,
                        };

                        assert_eq!(
                            response.chunks,
                            chunk_array.sub_array(0, chunk_count as u64).unwrap()
                        );

                        store
                            .read()
                            .await
                            .validate_range_proof(0, &response)
                            .expect("validate proof");
                    }
                    _ => {
                        panic!("Not expected message: Response::Chunks");
                    }
                },
                _ => {
                    panic!("Not expected message: NetworkMessage::SendResponse");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_request_chunks_invalid_indices() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let request = GetChunksRequest {
            tx_seq: 0,
            index_start: 0,
            index_end: 0 as u64,
        };

        sync_send
            .notify(SyncMessage::RequestChunks {
                request_id: (ConnectionId::new(0), SubstreamId(0)),
                peer_id: init_peer_id,
                request,
            })
            .unwrap();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, init_peer_id);
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }
                    assert_eq!(msg, "Invalid chunk indices");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_request_chunks_tx_not_exist() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let request = GetChunksRequest {
            tx_seq: 1,
            index_start: 0,
            index_end: chunk_count as u64,
        };

        sync_send
            .notify(SyncMessage::RequestChunks {
                request_id: (ConnectionId::new(0), SubstreamId(0)),
                peer_id: init_peer_id,
                request,
            })
            .unwrap();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, init_peer_id);
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }
                    assert_eq!(msg, "Tx not found");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_request_chunks_index_out_bound() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let request = GetChunksRequest {
            tx_seq: 0,
            index_start: 0,
            index_end: chunk_count as u64 + 1,
        };

        sync_send
            .notify(SyncMessage::RequestChunks {
                request_id: (ConnectionId::new(0), SubstreamId(0)),
                peer_id: init_peer_id,
                request,
            })
            .unwrap();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, init_peer_id);
                    match action {
                        PeerAction::Fatal => {}
                        _ => {
                            panic!("PeerAction expect Fatal");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }
                    assert_eq!(msg, "Chunk index out of bound");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_request_chunks_tx_not_finalized() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (store, _, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let request = GetChunksRequest {
            tx_seq: 0,
            index_start: 0,
            index_end: chunk_count as u64,
        };

        sync_send
            .notify(SyncMessage::RequestChunks {
                request_id: (ConnectionId::new(0), SubstreamId(0)),
                peer_id: init_peer_id,
                request,
            })
            .unwrap();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, init_peer_id);
                    match action {
                        PeerAction::MidToleranceError => {}
                        _ => {
                            panic!("PeerAction expect MidToleranceError");
                        }
                    }

                    match source {
                        ReportSource::SyncService => {}
                        _ => {
                            panic!("ReportSource expect SyncService");
                        }
                    }
                    assert_eq!(msg, "Tx not finalized");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::SendErrorResponse {
                    peer_id,
                    id,
                    error,
                    reason,
                } => {
                    assert_eq!(peer_id, init_peer_id);
                    assert_eq!(id.1 .0, 0);
                    assert_eq!(error, RPCResponseErrorCode::InvalidRequest);
                    assert_eq!(reason, "Tx not finalized".to_string());
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::SendResponse");
                }
            }
        }
    }

    #[tokio::test]
    // #[traced_test]
    async fn test_sync_file_tx_not_exist() {
        let runtime = TestRuntime::default();

        let config = LogConfig::default();

        let store = Arc::new(RwLock::new(LogManager::memorydb(config.clone()).unwrap()));

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        thread::sleep(Duration::from_millis(1000));
        assert_eq!(
            store.read().await.get_tx_by_seq_number(tx_seq).unwrap(),
            None
        );
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_sync_file_exist_in_store() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            peer_store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        thread::sleep(Duration::from_millis(1000));
        assert_eq!(
            peer_store.read().await.check_tx_completed(tx_seq).unwrap(),
            true
        );
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    async fn wait_for_tx_finalized(store: Arc<RwLock<LogManager>>, tx_seq: u64) {
        let deadline = Instant::now() + Duration::from_millis(5000);
        while !store.read().await.check_tx_completed(tx_seq).unwrap() {
            if Instant::now() >= deadline {
                panic!("Failed to wait tx completed");
            }

            thread::sleep(Duration::from_millis(300));
        }
    }

    #[tokio::test]
    async fn test_sync_file_success() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (store, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        assert_eq!(
            store.read().await.check_tx_completed(tx_seq).unwrap(),
            false
        );

        assert_ne!(
            sync_send
                .request(SyncRequest::SyncStatus { tx_seq })
                .await
                .unwrap(),
            SyncResponse::SyncStatus {
                status: "Completed".to_string()
            }
        );

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            chunk_count as u64,
        )
        .await;

        wait_for_tx_finalized(store, tx_seq).await;

        // test heartbeat
        let deadline = Instant::now() + Duration::from_secs(HEARTBEAT_INTERVAL_SEC + 1);
        while sync_send
            .request(SyncRequest::SyncStatus { tx_seq })
            .await
            .unwrap()
            != (SyncResponse::SyncStatus {
                status: "unknown".to_string(),
            })
        {
            if Instant::now() >= deadline {
                panic!("Failed to wait heartbeat");
            }

            thread::sleep(Duration::from_millis(300));
        }
    }

    #[tokio::test]
    async fn test_sync_file_special_size() {
        test_sync_file(1).await;
        test_sync_file(511).await;
        test_sync_file(512).await;
        test_sync_file(513).await;
        test_sync_file(514).await;
        test_sync_file(1023).await;
        test_sync_file(1024).await;
        test_sync_file(1025).await;
        test_sync_file(2047).await;
        test_sync_file(2048).await;
    }

    #[tokio::test]
    async fn test_sync_file_exceed_max_chunks_to_request() {
        let runtime = TestRuntime::default();

        let chunk_count = 2049;
        let (store, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        assert_eq!(
            store.read().await.check_tx_completed(tx_seq).unwrap(),
            false
        );

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            2048,
        )
        .await;

        assert_ne!(
            sync_send
                .request(SyncRequest::SyncStatus { tx_seq })
                .await
                .unwrap(),
            SyncResponse::SyncStatus {
                status: "Completed".to_string()
            }
        );

        // next batch
        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            2048,
            chunk_count as u64,
        )
        .await;

        wait_for_tx_finalized(store, tx_seq).await;
    }

    #[tokio::test]
    async fn test_sync_file_multi_files() {
        let runtime = TestRuntime::default();

        let chunk_count = vec![1535, 1535, 1535];
        let (store, peer_store, _, _) = create_2_store(chunk_count.clone());

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 3);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        // second file
        let tx_seq = 1u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        assert_eq!(
            store.read().await.check_tx_completed(tx_seq).unwrap(),
            false
        );
        assert_eq!(store.read().await.check_tx_completed(0).unwrap(), false);

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            chunk_count[0] as u64,
        )
        .await;

        wait_for_tx_finalized(store.clone(), tx_seq).await;

        assert_eq!(store.read().await.check_tx_completed(0).unwrap(), false);

        // first file
        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            chunk_count[0] as u64,
        )
        .await;

        wait_for_tx_finalized(store, tx_seq).await;

        sync_send
            .notify(SyncMessage::PeerDisconnected {
                peer_id: init_peer_id,
            })
            .unwrap();

        thread::sleep(Duration::from_millis(1000));
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_rpc_error() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        sync_send
            .notify(SyncMessage::RpcError {
                request_id: network::SyncId::SerialSync { tx_seq: 0 },
                peer_id: init_peer_id,
            })
            .unwrap();

        thread::sleep(Duration::from_millis(1000));
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_announce_file() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (store, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> = Default::default();

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        let address: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        sync_send
            .notify(SyncMessage::AnnounceFileGossip {
                tx_seq,
                peer_id: init_peer_id,
                addr: address,
            })
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        assert_eq!(
            store.read().await.check_tx_completed(tx_seq).unwrap(),
            false
        );

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            chunk_count as u64,
        )
        .await;

        wait_for_tx_finalized(store, tx_seq).await;
    }

    #[tokio::test]
    async fn test_announce_file_in_sync() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (store, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        let address: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        sync_send
            .notify(SyncMessage::AnnounceFileGossip {
                tx_seq,
                peer_id: init_peer_id,
                addr: address,
            })
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        assert_eq!(
            store.read().await.check_tx_completed(tx_seq).unwrap(),
            false
        );

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            chunk_count as u64,
        )
        .await;

        wait_for_tx_finalized(store, tx_seq).await;
    }

    #[tokio::test]
    async fn test_announce_file_already_in_store() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (_, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            peer_store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        let address: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        sync_send
            .notify(SyncMessage::AnnounceFileGossip {
                tx_seq,
                peer_id: init_peer_id,
                addr: address,
            })
            .unwrap();

        thread::sleep(Duration::from_millis(1000));
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_sync_status_unknown() {
        let runtime = TestRuntime::default();

        let chunk_count = 1535;
        let (store, _, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, _) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn(
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        assert_eq!(
            sync_send
                .request(SyncRequest::SyncStatus { tx_seq: 0 })
                .await
                .unwrap(),
            SyncResponse::SyncStatus {
                status: "unknown".to_string()
            }
        );
    }

    async fn receive_dial(
        network_recv: &mut UnboundedReceiver<NetworkMessage>,
        sync_send: &SyncSender,
        init_peer_id: PeerId,
    ) {
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::DialPeer {
                    address: _,
                    peer_id,
                } => {
                    assert_eq!(peer_id, init_peer_id);

                    sync_send
                        .notify(SyncMessage::PeerConnected { peer_id })
                        .unwrap();
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::DialPeer");
                }
            }
        }
    }

    async fn test_sync_file(chunk_count: usize) {
        let runtime = TestRuntime::default();

        let (store, peer_store, _, _) = create_2_store(vec![chunk_count]);

        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let file_location_cache: Arc<FileLocationCache> =
            create_file_location_cache(init_peer_id, 1);

        let (network_send, mut network_recv) = mpsc::unbounded_channel::<NetworkMessage>();

        let sync_send = SyncService::spawn_with_config(
            Config::default().disable_auto_sync(),
            runtime.task_executor.clone(),
            network_send,
            store.clone(),
            file_location_cache,
        );

        let tx_seq = 0u64;
        sync_send
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .unwrap();

        receive_dial(&mut network_recv, &sync_send, init_peer_id).await;

        assert_eq!(
            store.read().await.check_tx_completed(tx_seq).unwrap(),
            false
        );

        assert_ne!(
            sync_send
                .request(SyncRequest::SyncStatus { tx_seq })
                .await
                .unwrap(),
            SyncResponse::SyncStatus {
                status: "Completed".to_string()
            }
        );

        receive_chunk_request(
            &mut network_recv,
            &sync_send,
            peer_store.clone(),
            init_peer_id,
            tx_seq,
            0,
            chunk_count as u64,
        )
        .await;

        wait_for_tx_finalized(store, tx_seq).await;
    }

    async fn receive_chunk_request(
        network_recv: &mut UnboundedReceiver<NetworkMessage>,
        sync_send: &SyncSender,
        peer_store: Arc<RwLock<LogManager>>,
        init_peer_id: PeerId,
        tx_seq: u64,
        index_start: u64,
        index_end: u64,
    ) {
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::SendRequest {
                    peer_id,
                    request,
                    request_id,
                } => {
                    assert_eq!(peer_id, init_peer_id);

                    let req = match request {
                        network::Request::GetChunks(req) => {
                            assert_eq!(req.tx_seq, tx_seq);
                            assert_eq!(req.index_start, index_start);
                            assert_eq!(req.index_end, index_end);

                            req
                        }
                        _ => {
                            panic!("Not expected message network::Request::GetChunks");
                        }
                    };

                    let sync_id = match request_id {
                        network::RequestId::Sync(sync_id) => sync_id,
                        _ => unreachable!("All Chunks responses belong to sync"),
                    };

                    let chunks = peer_store
                        .read()
                        .await
                        .get_chunks_with_proof_by_tx_and_index_range(
                            req.tx_seq,
                            req.index_start as usize,
                            req.index_end as usize,
                        )
                        .unwrap()
                        .unwrap();

                    sync_send
                        .notify(SyncMessage::ChunksResponse {
                            peer_id,
                            request_id: sync_id,
                            response: chunks,
                        })
                        .unwrap();
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::SendRequest");
                }
            }
        }
    }
}
