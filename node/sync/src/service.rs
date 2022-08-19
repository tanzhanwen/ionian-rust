use crate::context::SyncNetworkContext;
use crate::controllers::{SerialSyncController, SyncState};
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
use tokio::sync::mpsc;

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

#[derive(Debug)]
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
        store: Arc<dyn LogStore>,
        file_location_cache: Arc<FileLocationCache>,
    ) -> SyncSender {
        let (sync_send, sync_recv) = channel::Channel::unbounded();

        let heartbeat =
            tokio::time::interval(tokio::time::Duration::from_secs(HEARTBEAT_INTERVAL_SEC));

        let store = Store::new(store, executor.clone());

        let mut sync = SyncService {
            msg_recv: sync_recv,
            ctx: Arc::new(SyncNetworkContext::new(network_send)),
            store,
            file_location_cache,
            controllers: Default::default(),
            heartbeat,
        };

        debug!("Starting sync service");
        executor.spawn(async move { Box::pin(sync.main()).await }, "sync");

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
            // TODO(ionian-dev): only update controllers that need it?
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
                    num_chunks,
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
    }
}
