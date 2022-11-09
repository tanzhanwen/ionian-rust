use std::{ops::Neg, sync::Arc};

use file_location_cache::FileLocationCache;
use network::{
    rpc::StatusMessage,
    types::{AnnounceFile, FindFile, SignedAnnounceFile},
    Keypair, MessageAcceptance, MessageId, NetworkGlobals, NetworkMessage, PeerId, PeerRequestId,
    PublicKey, PubsubMessage, Request, RequestId, Response,
};
use shared_types::{timestamp_now, TxID};
use storage_async::Store;
use sync::{SyncMessage, SyncSender};
use tokio::sync::{mpsc, RwLock};

use crate::peer_manager::PeerManager;

lazy_static::lazy_static! {
    pub static ref FIND_FILE_TIMEOUT: chrono::Duration = chrono::Duration::minutes(2);
    pub static ref ANNOUNCE_FILE_TIMEOUT: chrono::Duration = chrono::Duration::minutes(2);
    pub static ref TOLERABLE_DRIFT: chrono::Duration = chrono::Duration::seconds(5);
}

fn duration_since(timestamp: u32) -> chrono::Duration {
    let timestamp = i64::try_from(timestamp).expect("Should fit");
    let timestamp = chrono::NaiveDateTime::from_timestamp(timestamp, 0);
    let now = chrono::Utc::now().naive_utc();
    now.signed_duration_since(timestamp)
}

fn peer_id_to_public_key(peer_id: &PeerId) -> Result<PublicKey, String> {
    // A libp2p peer id byte representation should be 2 length bytes + 4 protobuf bytes + compressed pk bytes
    // if generated from a PublicKey with Identity multihash.
    let pk_bytes = &peer_id.to_bytes()[2..];

    PublicKey::from_protobuf_encoding(pk_bytes).map_err(|e| {
        format!(
            " Cannot parse libp2p public key public key from peer id: {}",
            e
        )
    })
}

pub struct Libp2pEventHandler {
    /// A collection of global variables, accessible outside of the network service.
    network_globals: Arc<NetworkGlobals>,
    /// A channel to the router service.
    network_send: mpsc::UnboundedSender<NetworkMessage>,
    /// A channel to the syncing service.
    sync_send: SyncSender,
    /// Node keypair for signing messages.
    local_keypair: Keypair,
    /// Log and transaction storage.
    store: Store,
    /// Cache for storing and serving gossip messages.
    file_location_cache: Arc<FileLocationCache>,
    /// All connected peers.
    peers: Arc<RwLock<PeerManager>>,
}

impl Libp2pEventHandler {
    pub fn new(
        network_globals: Arc<NetworkGlobals>,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        sync_send: SyncSender,
        local_keypair: Keypair,
        store: Store,
        file_location_cache: Arc<FileLocationCache>,
        peers: Arc<RwLock<PeerManager>>,
    ) -> Self {
        Self {
            network_globals,
            network_send,
            sync_send,
            local_keypair,
            store,
            file_location_cache,
            peers,
        }
    }

    fn send_to_network(&self, message: NetworkMessage) {
        self.network_send.send(message).unwrap_or_else(|err| {
            warn!(%err, "Could not send message to the network service");
        });
    }

    pub fn send_to_sync(&self, message: SyncMessage) {
        self.sync_send.notify(message).unwrap_or_else(|err| {
            warn!(%err, "Could not send message to the sync service");
        });
    }

    pub fn publish(&self, msg: PubsubMessage) {
        self.send_to_network(NetworkMessage::Publish {
            messages: vec![msg],
        });
    }

    pub fn send_status(&self, peer_id: PeerId) {
        let status_message = StatusMessage { data: 123 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status request");

        self.send_to_network(NetworkMessage::SendRequest {
            peer_id,
            request_id: RequestId::Router,
            request: Request::Status(status_message),
        });
    }

    pub async fn on_peer_connected(&self, peer_id: PeerId, outgoing: bool) {
        self.peers.write().await.add(peer_id, outgoing);

        if outgoing {
            self.send_status(peer_id);
            self.send_to_sync(SyncMessage::PeerConnected { peer_id });
        }
    }

    pub async fn on_peer_disconnected(&self, peer_id: PeerId) {
        self.peers.write().await.remove(&peer_id);
        self.send_to_sync(SyncMessage::PeerDisconnected { peer_id });
    }

    pub async fn on_rpc_request(
        &self,
        peer_id: PeerId,
        request_id: PeerRequestId,
        request: Request,
    ) {
        self.peers.write().await.update(&peer_id);

        match request {
            Request::Status(status) => {
                self.on_status_request(peer_id, request_id, status);
            }
            Request::GetChunks(request) => {
                self.send_to_sync(SyncMessage::RequestChunks {
                    peer_id,
                    request_id,
                    request,
                });
            }
            Request::DataByHash(_) => {
                // ignore
            }
        }
    }

    fn on_status_request(&self, peer_id: PeerId, request_id: PeerRequestId, status: StatusMessage) {
        debug!(%peer_id, ?status, "Received Status request");

        let status_message = StatusMessage { data: 456 }; // dummy status message
        debug!(%peer_id, ?status_message, "Sending Status response");

        self.send_to_network(NetworkMessage::SendResponse {
            peer_id,
            id: request_id,
            response: Response::Status(status_message),
        });
    }

    pub async fn on_rpc_response(
        &self,
        peer_id: PeerId,
        request_id: RequestId,
        response: Response,
    ) {
        self.peers.write().await.update(&peer_id);

        match response {
            Response::Status(status_message) => {
                debug!(%peer_id, ?status_message, "Received Status response");
            }
            Response::Chunks(response) => {
                let request_id = match request_id {
                    RequestId::Sync(sync_id) => sync_id,
                    _ => unreachable!("All Chunks responses belong to sync"),
                };

                self.send_to_sync(SyncMessage::ChunksResponse {
                    peer_id,
                    request_id,
                    response,
                });
            }
            Response::DataByHash(_) => {
                // ignore
            }
        }
    }

    pub async fn on_rpc_error(&self, peer_id: PeerId, request_id: RequestId) {
        self.peers.write().await.update(&peer_id);

        // Check if the failed RPC belongs to sync
        if let RequestId::Sync(request_id) = request_id {
            self.send_to_sync(SyncMessage::RpcError {
                peer_id,
                request_id,
            });
        }
    }

    pub async fn on_pubsub_message(
        &self,
        propagation_source: PeerId,
        source: PeerId,
        id: &MessageId,
        message: PubsubMessage,
    ) -> MessageAcceptance {
        info!(?message, %propagation_source, %source, %id, "Received pubsub message");

        match message {
            PubsubMessage::ExampleMessage(_) => MessageAcceptance::Ignore,
            PubsubMessage::FindFile(msg) => self.on_find_file(msg).await,
            PubsubMessage::AnnounceFile(msg) => self.on_announce_file(propagation_source, msg),
        }
    }

    pub fn construct_announce_file_message(&self, tx_id: TxID) -> Option<PubsubMessage> {
        let peer_id = *self.network_globals.peer_id.read();

        let addr = match self.network_globals.listen_multiaddrs.read().first() {
            Some(addr) => addr.clone(),
            None => {
                error!("No listen address available");
                return None;
            }
        };

        let timestamp = timestamp_now();

        let msg = AnnounceFile {
            tx_id,
            peer_id: peer_id.into(),
            at: addr.into(),
            timestamp,
        };

        let mut signed = match msg.into_signed(&self.local_keypair) {
            Ok(signed) => signed,
            Err(e) => {
                error!(%tx_id.seq, %e, "Failed to sign AnnounceFile message");
                return None;
            }
        };

        signed.resend_timestamp = timestamp;

        Some(PubsubMessage::AnnounceFile(signed))
    }

    async fn on_find_file(&self, msg: FindFile) -> MessageAcceptance {
        let FindFile { tx_id, timestamp } = msg;

        // verify timestamp
        let d = duration_since(timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *FIND_FILE_TIMEOUT {
            debug!(%timestamp, "Invalid timestamp, ignoring FindFile message");
            return MessageAcceptance::Ignore;
        }

        // check if we have it
        if matches!(self.store.check_tx_completed(tx_id.seq).await, Ok(true)) {
            if let Ok(Some(tx)) = self.store.get_tx_by_seq_number(tx_id.seq).await {
                if tx.id() == tx_id {
                    debug!(?tx_id, "Found file locally, responding to FindFile query");

                    return match self.construct_announce_file_message(tx_id) {
                        Some(msg) => {
                            self.publish(msg);
                            MessageAcceptance::Ignore
                        }
                        // propagate FindFile query to other nodes
                        None => MessageAcceptance::Accept,
                    };
                }
            }
        }

        // try from cache
        if let Some(mut msg) = self.file_location_cache.get_one(tx_id) {
            debug!(?tx_id, "Found file in cache, responding to FindFile query");

            msg.resend_timestamp = timestamp_now();
            self.publish(PubsubMessage::AnnounceFile(msg));

            return MessageAcceptance::Ignore;
        }

        // propagate FindFile query to other nodes
        MessageAcceptance::Accept
    }

    fn on_announce_file(
        &self,
        propagation_source: PeerId,
        msg: SignedAnnounceFile,
    ) -> MessageAcceptance {
        // verify message signature
        let pk = match peer_id_to_public_key(&msg.peer_id) {
            Ok(pk) => pk,
            Err(e) => {
                error!(
                    "Failed to convert peer id {:?} to public key: {:?}",
                    msg.peer_id, e
                );
                return MessageAcceptance::Reject;
            }
        };

        if !msg.verify_signature(&pk) {
            warn!(
                "Received message with invalid signature from peer {:?}",
                propagation_source
            );
            return MessageAcceptance::Reject;
        }

        // propagate gossip to peers
        let d = duration_since(msg.resend_timestamp);
        if d < TOLERABLE_DRIFT.neg() || d > *ANNOUNCE_FILE_TIMEOUT {
            debug!(%msg.resend_timestamp, "Invalid resend timestamp, ignoring AnnounceFile message");
            return MessageAcceptance::Ignore;
        }

        // notify sync layer
        self.send_to_sync(SyncMessage::AnnounceFileGossip {
            tx_id: msg.tx_id,
            peer_id: msg.peer_id.clone().into(),
            addr: msg.at.clone().into(),
        });

        // insert message to cache
        self.file_location_cache.insert(msg);

        MessageAcceptance::Accept
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use channel::Message::*;
    use file_location_cache::{test_util::AnnounceFileBuilder, FileLocationCache};
    use network::{
        discovery::{CombinedKey, ConnectionId},
        discv5::enr::EnrBuilder,
        rpc::{GetChunksRequest, StatusMessage, SubstreamId},
        types::FindFile,
        CombinedKeyExt, Keypair, MessageAcceptance, MessageId, Multiaddr, NetworkGlobals,
        NetworkMessage, PeerId, PubsubMessage, Request, RequestId, Response, SyncId,
    };
    use shared_types::{timestamp_now, ChunkArray, ChunkArrayWithProof, FlowRangeProof, TxID};
    use storage::{
        log_store::{log_manager::LogConfig, Store},
        LogManager,
    };
    use sync::{test_util::create_2_store, SyncMessage, SyncRequest, SyncResponse, SyncSender};
    use task_executor::test_utils::TestRuntime;
    use tokio::sync::{
        mpsc::{self, error::TryRecvError},
        RwLock,
    };

    use crate::{peer_manager::PeerManager, Config};

    use super::*;

    type SyncReceiver = channel::Receiver<SyncMessage, SyncRequest, SyncResponse>;

    struct Context {
        runtime: TestRuntime,
        network_globals: Arc<NetworkGlobals>,
        keypair: Keypair,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        network_recv: mpsc::UnboundedReceiver<NetworkMessage>,
        sync_send: SyncSender,
        sync_recv: SyncReceiver,
        store: Arc<RwLock<dyn Store>>,
        file_location_cache: Arc<FileLocationCache>,
        peers: Arc<RwLock<PeerManager>>,
    }

    impl Default for Context {
        fn default() -> Self {
            let runtime = TestRuntime::default();
            let (network_globals, keypair) = Context::new_network_globals();
            let (network_send, network_recv) = mpsc::unbounded_channel();
            let (sync_send, sync_recv) = channel::Channel::unbounded();
            let store = LogManager::memorydb(LogConfig::default()).unwrap();
            Self {
                runtime,
                network_globals: Arc::new(network_globals),
                keypair,
                network_send,
                network_recv,
                sync_send,
                sync_recv,
                store: Arc::new(RwLock::new(store)),
                file_location_cache: Arc::new(FileLocationCache::default()),
                peers: Arc::new(RwLock::new(PeerManager::new(Config::default()))),
            }
        }
    }

    impl Context {
        fn new_handler(&self) -> Libp2pEventHandler {
            Libp2pEventHandler::new(
                self.network_globals.clone(),
                self.network_send.clone(),
                self.sync_send.clone(),
                self.keypair.clone(),
                storage_async::Store::new(self.store.clone(), self.runtime.task_executor.clone()),
                self.file_location_cache.clone(),
                self.peers.clone(),
            )
        }

        fn new_network_globals() -> (NetworkGlobals, Keypair) {
            let keypair = Keypair::generate_secp256k1();
            let enr_key = CombinedKey::from_libp2p(&keypair).unwrap();
            let enr = EnrBuilder::new("v4").build(&enr_key).unwrap();
            let network_globals = NetworkGlobals::new(enr, 30000, 30000, vec![]);

            let listen_addr: Multiaddr = "/ip4/127.0.0.1/tcp/30000".parse().unwrap();
            network_globals.listen_multiaddrs.write().push(listen_addr);

            (network_globals, keypair)
        }

        fn assert_status_request(&mut self, expected_peer_id: PeerId) {
            match self.network_recv.try_recv() {
                Ok(NetworkMessage::SendRequest {
                    peer_id,
                    request,
                    request_id,
                }) => {
                    assert_eq!(peer_id, expected_peer_id);
                    assert!(matches!(request, Request::Status(..)));
                    assert!(matches!(request_id, RequestId::Router))
                }
                Ok(_) => panic!("Unexpected network message type received"),
                Err(e) => panic!("No network message received: {:?}", e),
            }
        }

        fn assert_file_announcement_published(&mut self, expected_tx_id: TxID) {
            match self.network_recv.try_recv() {
                Ok(NetworkMessage::Publish { messages }) => {
                    assert_eq!(messages.len(), 1);
                    assert!(
                        matches!(&messages[0], PubsubMessage::AnnounceFile(file) if file.tx_id == expected_tx_id)
                    );
                }
                Ok(_) => panic!("Unexpected network message type received"),
                Err(e) => panic!("No network message received: {:?}", e),
            }
        }
    }

    #[test]
    fn test_send_status() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        assert!(matches!(
            ctx.network_recv.try_recv(),
            Err(TryRecvError::Empty)
        ));

        let alice = PeerId::random();
        handler.send_status(alice);

        ctx.assert_status_request(alice);
    }

    #[tokio::test]
    async fn test_on_peer_connected_incoming() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        assert_eq!(handler.peers.read().await.size(), 0);

        let alice = PeerId::random();
        handler.on_peer_connected(alice, false).await;

        assert_eq!(handler.peers.read().await.size(), 1);
        assert!(matches!(
            ctx.network_recv.try_recv(),
            Err(TryRecvError::Empty)
        ));
        assert!(matches!(ctx.sync_recv.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_on_peer_connected_outgoing() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        assert_eq!(handler.peers.read().await.size(), 0);

        let alice = PeerId::random();
        handler.on_peer_connected(alice, true).await;

        assert_eq!(handler.peers.read().await.size(), 1);
        ctx.assert_status_request(alice);
        assert!(matches!(
            ctx.sync_recv.try_recv(),
            Ok(Notification(SyncMessage::PeerConnected {peer_id})) if peer_id == alice
        ));
    }

    #[tokio::test]
    async fn test_on_peer_disconnected() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        handler.on_peer_connected(alice, false).await;
        assert_eq!(handler.peers.read().await.size(), 1);

        handler.on_peer_disconnected(alice).await;
        assert_eq!(handler.peers.read().await.size(), 0);
        assert!(matches!(
            ctx.sync_recv.try_recv(),
            Ok(Notification(SyncMessage::PeerDisconnected {peer_id})) if peer_id == alice
        ));
    }

    #[tokio::test]
    async fn test_on_rpc_request_status() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let req_id = (ConnectionId::new(4), SubstreamId(12));
        let request = Request::Status(StatusMessage { data: 412 });
        handler.on_rpc_request(alice, req_id, request).await;

        match ctx.network_recv.try_recv() {
            Ok(NetworkMessage::SendResponse {
                peer_id,
                response,
                id,
            }) => {
                assert_eq!(peer_id, alice);
                assert!(matches!(response, Response::Status(..)));
                assert_eq!(id, req_id);
            }
            Ok(_) => panic!("Unexpected network message type received"),
            Err(e) => panic!("No network message received: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_on_rpc_request_get_chunks() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let id = (ConnectionId::new(4), SubstreamId(12));
        let raw_request = GetChunksRequest {
            tx_id: TxID::random_hash(7),
            index_start: 66,
            index_end: 99,
        };
        handler
            .on_rpc_request(alice, id, Request::GetChunks(raw_request.clone()))
            .await;

        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::RequestChunks {
                peer_id,
                request_id,
                request,
            })) => {
                assert_eq!(peer_id, alice);
                assert_eq!(request_id, id);
                assert_eq!(request, raw_request);
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_on_rpc_response() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let id = TxID::random_hash(555);
        let data = ChunkArrayWithProof {
            chunks: ChunkArray {
                data: vec![1, 2, 3, 4],
                start_index: 16,
            },
            proof: FlowRangeProof::new_empty(),
        };
        handler
            .on_rpc_response(
                alice,
                RequestId::Sync(SyncId::SerialSync { tx_id: id }),
                Response::Chunks(data.clone()),
            )
            .await;

        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::ChunksResponse {
                peer_id,
                request_id,
                response,
            })) => {
                assert_eq!(peer_id, alice);
                assert!(matches!(request_id, SyncId::SerialSync { tx_id } if tx_id == id ));
                assert_eq!(response, data);
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_on_rpc_error() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        let alice = PeerId::random();
        let id = TxID::random_hash(555);
        handler
            .on_rpc_error(alice, RequestId::Sync(SyncId::SerialSync { tx_id: id }))
            .await;

        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::RpcError {
                peer_id,
                request_id,
            })) => {
                assert_eq!(peer_id, alice);
                assert!(matches!(request_id, SyncId::SerialSync { tx_id } if tx_id == id ));
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }
    }

    async fn handle_find_file_msg(
        handler: &Libp2pEventHandler,
        tx_id: TxID,
        timestamp: u32,
    ) -> MessageAcceptance {
        let (alice, bob) = (PeerId::random(), PeerId::random());
        let id = MessageId::new(b"dummy message");
        let message = PubsubMessage::FindFile(FindFile { tx_id, timestamp });
        handler.on_pubsub_message(alice, bob, &id, message).await
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_invalid_timestamp() {
        let ctx = Context::default();
        let handler = ctx.new_handler();

        // message too future
        let result = handle_find_file_msg(
            &handler,
            TxID::random_hash(412),
            timestamp_now() + 10 + TOLERABLE_DRIFT.num_seconds() as u32,
        )
        .await;
        assert!(matches!(result, MessageAcceptance::Ignore));

        // message too old
        let result = handle_find_file_msg(
            &handler,
            TxID::random_hash(412),
            timestamp_now() - 10 - FIND_FILE_TIMEOUT.num_seconds() as u32,
        )
        .await;
        assert!(matches!(result, MessageAcceptance::Ignore));
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_not_found() {
        let ctx = Context::default();
        let handler = ctx.new_handler();

        let result = handle_find_file_msg(&handler, TxID::random_hash(412), timestamp_now()).await;
        assert!(matches!(result, MessageAcceptance::Accept));
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_in_store() {
        let mut ctx = Context::default();

        // prepare store with txs
        let (_, store, txs, _) = create_2_store(vec![1314]);
        ctx.store = store;

        let handler = ctx.new_handler();

        // receive find file request
        let result = handle_find_file_msg(&handler, txs[0].id(), timestamp_now()).await;
        assert!(matches!(result, MessageAcceptance::Ignore));
        ctx.assert_file_announcement_published(txs[0].id());
    }

    #[tokio::test]
    async fn test_on_pubsub_find_file_in_cache() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        // prepare tx in cache
        let tx_id = TxID::random_hash(412);
        let signed = AnnounceFileBuilder::default()
            .with_tx_id(tx_id)
            .with_timestamp(timestamp_now() - 5)
            .build();
        ctx.file_location_cache.insert(signed);

        // receive find file request
        let result = handle_find_file_msg(&handler, tx_id, timestamp_now()).await;
        assert!(matches!(result, MessageAcceptance::Ignore));
        ctx.assert_file_announcement_published(tx_id);
    }

    #[tokio::test]
    async fn test_on_pubsub_announce_file_invalid_sig() {
        let ctx = Context::default();
        let handler = ctx.new_handler();

        let (alice, bob) = (PeerId::random(), PeerId::random());
        let id = MessageId::new(b"dummy message");
        let tx_id = TxID::random_hash(412);

        // change signed message
        let message = match handler.construct_announce_file_message(tx_id).unwrap() {
            PubsubMessage::AnnounceFile(mut file) => {
                let malicious_addr: Multiaddr = "/ip4/127.0.0.38/tcp/30000".parse().unwrap();
                file.inner.at = malicious_addr.into();
                PubsubMessage::AnnounceFile(file)
            }
            _ => panic!("Unexpected pubsub message type"),
        };

        // failed to verify signature
        let result = handler.on_pubsub_message(alice, bob, &id, message).await;
        assert!(matches!(result, MessageAcceptance::Reject));
    }

    #[tokio::test]
    async fn test_on_pubsub_announce_file() {
        let mut ctx = Context::default();
        let handler = ctx.new_handler();

        // prepare message
        let (alice, bob) = (PeerId::random(), PeerId::random());
        let id = MessageId::new(b"dummy message");
        let tx = TxID::random_hash(412);
        let message = handler.construct_announce_file_message(tx).unwrap();

        // succeeded to handle
        let result = handler.on_pubsub_message(alice, bob, &id, message).await;
        assert!(matches!(result, MessageAcceptance::Accept));

        // ensure notify to sync layer
        match ctx.sync_recv.try_recv() {
            Ok(Notification(SyncMessage::AnnounceFileGossip {
                tx_id,
                peer_id,
                addr,
            })) => {
                assert_eq!(tx_id, tx);
                assert_eq!(peer_id, *ctx.network_globals.peer_id.read());
                assert_eq!(
                    addr,
                    *ctx.network_globals.listen_multiaddrs.read().get(0).unwrap()
                );
            }
            Ok(_) => panic!("Unexpected sync message type received"),
            Err(e) => panic!("No sync message received: {:?}", e),
        }

        // ensure cache updated
        assert_eq!(ctx.file_location_cache.get_all(tx).len(), 1);
    }
}
