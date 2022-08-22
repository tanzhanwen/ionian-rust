use crate::context::SyncNetworkContext;
use crate::controllers::peers::{PeerState, SyncPeers};
use file_location_cache::FileLocationCache;
use network::{
    multiaddr::Protocol, rpc::GetChunksRequest, types::FindFile, Multiaddr, NetworkMessage,
    PeerAction, PeerId, PubsubMessage, ReportSource, SyncId as RequestId,
};
use shared_types::{timestamp_now, ChunkArrayWithProof, DataRoot, CHUNK_SIZE};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use storage_async::Store;

const MAX_CHUNKS_TO_REQUEST: usize = 2 * 1024;
const MAX_REQUEST_FAILURES: usize = 3;
const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(5);

// TODO(ionian-dev):
// - parallel requests for multiple files
// - parallel requests for a single fie from multiple peers
// - limit number of peers
// - limit number of retries

#[derive(Debug, PartialEq, Eq)]
pub enum SyncState {
    Idle,
    FindingPeers {
        since: Instant,
    },
    FoundPeers,
    ConnectingPeers,
    AwaitingDownload,
    Downloading {
        peer_id: PeerId,
        from_chunk: usize,
        to_chunk: usize,
        since: Instant,
    },
    Completed,
    Failed {
        reason: String,
    },
}

pub struct SerialSyncController {
    /// The transaction sequence number.
    tx_seq: u64,

    /// The transaction data root.
    data_root: DataRoot,

    /// The size of the file to be synced.
    num_chunks: usize,

    /// The next chunk id that we need to retrieve.
    next_chunk: usize,

    /// Continuous RPC failures to request chunks.
    failures: usize,

    /// Current state of this request.
    state: SyncState,

    /// Sync peer manager.
    peers: SyncPeers,

    /// A network context to contact the network service.
    ctx: Arc<SyncNetworkContext>,

    /// Log and transaction storage.
    store: Store,

    /// Cache for storing and serving gossip messages.
    file_location_cache: Arc<FileLocationCache>,
}

impl SerialSyncController {
    pub fn new(
        tx_seq: u64,
        data_root: DataRoot,
        num_chunks: usize,
        ctx: Arc<SyncNetworkContext>,
        store: Store,
        file_location_cache: Arc<FileLocationCache>,
    ) -> Self {
        SerialSyncController {
            tx_seq,
            data_root,
            num_chunks,
            next_chunk: 0,
            failures: 0,
            state: SyncState::Idle,
            peers: Default::default(),
            ctx,
            store,
            file_location_cache,
        }
    }

    pub fn get_status(&self) -> &SyncState {
        &self.state
    }

    /// Resets the status to re-sync file when failed.
    pub fn reset(&mut self) {
        self.next_chunk = 0;
        self.failures = 0;
        self.state = SyncState::Idle;
        // remove disconnected peers
        self.peers.transition();
    }

    fn try_find_peers(&mut self) {
        info!(%self.tx_seq, "Finding peers");

        // try from cache
        let mut found_new_peer = false;

        for announcement in self.file_location_cache.get_all(self.tx_seq) {
            // make sure peer_id is part of the address
            let peer_id: PeerId = announcement.peer_id.clone().into();
            let mut addr: Multiaddr = announcement.at.clone().into();
            addr.push(Protocol::P2p(peer_id.into()));

            found_new_peer = self.on_peer_found(peer_id, addr) || found_new_peer;
        }

        if !found_new_peer {
            self.ctx.publish(PubsubMessage::FindFile(FindFile {
                tx_seq: self.tx_seq,
                timestamp: timestamp_now(),
            }));
        }

        self.state = SyncState::FindingPeers {
            since: Instant::now(),
        };
    }

    fn try_connect(&mut self) {
        // select a random peer
        let (peer_id, address) = match self.peers.random_peer(PeerState::Found) {
            Some((peer_id, address)) => (peer_id, address),
            None => {
                // peer may be disconnected by remote node and need to find peers again
                warn!(%self.tx_seq, "No peers available to connect");
                self.state = SyncState::Idle;
                return;
            }
        };

        // connect to peer
        info!(%peer_id, %address, "Attempting to connect to peer");
        self.ctx.send(NetworkMessage::DialPeer { address, peer_id });

        self.peers
            .update_state(&peer_id, PeerState::Found, PeerState::Connecting);

        self.state = SyncState::ConnectingPeers;
    }

    fn try_request_next(&mut self) {
        // select a random peer
        let peer_id = match self.peers.random_peer(PeerState::Connected) {
            Some((peer_id, _)) => peer_id,
            None => {
                warn!(%self.tx_seq, "No peers available to request chunks");
                self.state = SyncState::Idle;
                return;
            }
        };

        // request next chunk array
        let from_chunk = self.next_chunk;
        let to_chunk = std::cmp::min(from_chunk + MAX_CHUNKS_TO_REQUEST, self.num_chunks);

        let request_id = network::RequestId::Sync(RequestId::SerialSync {
            tx_seq: self.tx_seq,
        });

        let request = network::Request::GetChunks(GetChunksRequest {
            tx_seq: self.tx_seq,
            // FIXME(ionian-dev): unify APIs with usize or u32
            index_start: from_chunk as u32,
            index_end: to_chunk as u32,
        });

        self.ctx.send(NetworkMessage::SendRequest {
            peer_id,
            request_id,
            request,
        });

        self.state = SyncState::Downloading {
            peer_id,
            from_chunk,
            to_chunk,
            since: Instant::now(),
        };
    }

    fn ban_peer(&mut self, peer_id: PeerId, reason: &'static str) {
        info!(%peer_id, %self.tx_seq, %reason, "Banning peer");

        self.ctx.send(NetworkMessage::ReportPeer {
            peer_id,
            action: PeerAction::Fatal,
            source: ReportSource::SyncService,
            msg: reason,
        });

        self.peers
            .update_state(&peer_id, PeerState::Connected, PeerState::Disconnecting);
    }

    fn report_peer(&self, peer_id: PeerId, action: PeerAction, reason: &'static str) {
        debug!(%peer_id, ?action, %reason, "Report peer");

        self.ctx.send(NetworkMessage::ReportPeer {
            peer_id,
            action,
            source: ReportSource::SyncService,
            msg: reason,
        })
    }

    pub fn on_peer_found(&mut self, peer_id: PeerId, addr: Multiaddr) -> bool {
        if self.peers.add_new_peer(peer_id, addr.clone()) {
            info!(%peer_id, %addr, "Found new peer");
            true
        } else {
            // e.g. multiple `AnnounceFile` messages propagated
            debug!(%peer_id, %addr, "Found an existing peer");
            false
        }
    }

    pub fn on_dail_failed(&mut self, peer_id: PeerId) {
        if let Some(true) =
            self.peers
                .update_state(&peer_id, PeerState::Connecting, PeerState::Disconnected)
        {
            self.state = SyncState::Idle;
        }
    }

    pub fn on_peer_connected(&mut self, peer_id: PeerId) {
        match self
            .peers
            .update_state(&peer_id, PeerState::Connecting, PeerState::Connected)
        {
            Some(true) => info!(%peer_id, "Peer connected"),
            _ => {
                // e.g. one peer announced multiple files
                let state = self.peers.peer_state(&peer_id);
                debug!(%peer_id, ?state, "Peer already connected");
            }
        }
    }

    pub fn on_peer_disconnected(&mut self, peer_id: PeerId) {
        match self
            .peers
            .update_state_force(&peer_id, PeerState::Disconnected)
        {
            Some(PeerState::Disconnecting) => info!(%peer_id, "Peer disconnected"),
            Some(old_state) => info!(%peer_id, ?old_state, "Peer disconnected by remote"),
            None => warn!(%peer_id, "Peer already disconnected"),
        }
    }

    /// Handle the case that got an unexpected response:
    /// 1. not in `Downloading` sync state.
    /// 2. from unexpected peer.
    fn handle_on_response_mismatch(&self, from_peer_id: PeerId) -> bool {
        match self.state {
            SyncState::Downloading { peer_id, .. } => {
                if from_peer_id == peer_id {
                    return false;
                }

                // got response from wrong peer
                // this can happen if we get a response for a timeout request
                warn!(%self.tx_seq, %from_peer_id, %peer_id, "Got response from unexpected peer");
                self.report_peer(
                    from_peer_id,
                    PeerAction::LowToleranceError,
                    "Peer id mismatch",
                );
                true
            }
            _ => {
                warn!(%self.tx_seq, %from_peer_id, ?self.state, "Got response in unexpected state");
                self.report_peer(
                    from_peer_id,
                    PeerAction::LowToleranceError,
                    "Sync state mismatch",
                );
                true
            }
        }
    }

    pub async fn on_response(&mut self, from_peer_id: PeerId, response: ChunkArrayWithProof) {
        if self.handle_on_response_mismatch(from_peer_id) {
            return;
        }

        let (from_chunk, to_chunk) = match self.state {
            SyncState::Downloading {
                peer_id: _peer_id,
                from_chunk,
                to_chunk,
                ..
            } => (from_chunk, to_chunk),
            _ => return,
        };

        debug_assert!(from_chunk < to_chunk, "Invalid chunk boundaries");

        // invalid chunk array size: ban and re-request
        let data_len = response.chunks.data.len();
        if data_len == 0 || data_len % CHUNK_SIZE > 0 {
            warn!(%from_peer_id, %self.tx_seq, %data_len, "Invalid chunk response data length");
            self.ban_peer(from_peer_id, "Invalid chunk response data length");
            self.state = SyncState::Idle;
            return;
        }

        // invalid chunk range: ban and re-request
        let start_index = response.chunks.start_index as usize;
        let end_index = start_index + data_len / CHUNK_SIZE;
        if start_index != from_chunk || end_index != to_chunk {
            warn!(%self.tx_seq, "Invalid chunk response range, expected={from_chunk}..{to_chunk}, actual={start_index}..{end_index}");
            self.ban_peer(from_peer_id, "Invalid chunk response range");
            self.state = SyncState::Idle;
            return;
        }

        // validate Merkle proofs
        let validation_result = response.validate(&self.data_root, self.num_chunks);
        if !matches!(validation_result, Ok(true)) {
            self.ban_peer(from_peer_id, "Chunk array validation failed");
            self.state = SyncState::Idle;
            return;
        }

        self.failures = 0;

        // store in db
        if let Err(e) = self.store.put_chunks(self.tx_seq, response.chunks).await {
            let err = format!("Unexpected DB error while storing chunks: {:?}", e);
            error!("{}", err);
            self.state = SyncState::Failed { reason: err };
            return;
        }

        self.next_chunk = to_chunk;

        // prepare to download next
        if self.next_chunk < self.num_chunks {
            self.state = SyncState::Idle;
            return;
        }

        // finalize tx if all chunks downloaded
        if let Err(e) = self.store.finalize_tx(self.tx_seq).await {
            let err = format!("Unexpected error during finalize_tx: {:?}", e);
            error!("{}", err);
            self.state = SyncState::Failed { reason: err };
            return;
        }

        self.state = SyncState::Completed;
    }

    pub fn on_request_failed(&mut self, peer_id: PeerId) {
        if self.handle_on_response_mismatch(peer_id) {
            return;
        }

        self.handle_response_failure(peer_id, "RPC Error");
    }

    fn handle_response_failure(&mut self, peer_id: PeerId, reason: &'static str) {
        info!(%peer_id, %self.tx_seq, %reason, "Chunks request failed");

        // ban peer on too many failures
        self.report_peer(peer_id, PeerAction::LowToleranceError, reason);

        self.failures += 1;

        if self.failures <= MAX_REQUEST_FAILURES {
            // try again
            self.state = SyncState::AwaitingDownload;
        } else {
            // ban and find new peer to download
            self.ban_peer(peer_id, reason);
            self.state = SyncState::Idle;
        }
    }

    pub fn transition(&mut self) {
        use PeerState::*;

        // update peer connection states
        self.peers.transition();

        loop {
            match self.state {
                SyncState::Idle => {
                    if self.peers.count(&[Found, Connecting, Connected]) > 0 {
                        self.state = SyncState::FindingPeers {
                            since: Instant::now(),
                        };
                    } else {
                        self.try_find_peers();
                    }
                }

                SyncState::FindingPeers { since } => {
                    if self.peers.count(&[Found, Connecting, Connected]) > 0 {
                        self.state = SyncState::FoundPeers;
                    } else if since.elapsed() >= PEER_REQUEST_TIMEOUT {
                        warn!(%self.tx_seq, "Peer request timeout");
                        self.state = SyncState::Idle;
                    } else {
                        return;
                    }
                }

                SyncState::FoundPeers => {
                    if self.peers.count(&[Connecting, Connected]) > 0 {
                        self.state = SyncState::ConnectingPeers;
                    } else {
                        self.try_connect();
                    }
                }

                SyncState::ConnectingPeers => {
                    if self.peers.count(&[Connected]) > 0 {
                        self.state = SyncState::AwaitingDownload;
                    } else if self.peers.count(&[Connecting]) == 0 {
                        self.state = SyncState::Idle;
                    } else {
                        // peers.transition() will handle the case that peer connecting timeout
                        return;
                    }
                }

                SyncState::AwaitingDownload => {
                    self.try_request_next();
                }

                SyncState::Downloading { peer_id, since, .. } => {
                    if !matches!(self.peers.peer_state(&peer_id), Some(PeerState::Connected)) {
                        // e.g. peer disconnected by remote node
                        self.state = SyncState::Idle;
                    } else if since.elapsed() >= DOWNLOAD_TIMEOUT {
                        self.handle_response_failure(peer_id, "RPC timeout");
                    } else {
                        return;
                    }
                }

                SyncState::Completed => {
                    return;
                }

                SyncState::Failed { .. } => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp;

    use libp2p::identity;
    use network::{types::AnnounceFile, Request};
    use rand::random;
    use shared_types::{ChunkArray, Transaction};
    use storage::log_store::{
        sub_merkle_tree, LogStoreChunkWrite, LogStoreRead, LogStoreWrite, SimpleLogStore,
    };
    use task_executor::{test_utils::TestRuntime, TaskExecutor};
    use tokio::sync::mpsc::{self, UnboundedReceiver};

    use super::*;

    #[test]
    fn test_status() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_default_controller(task_executor, None);

        assert_eq!(*controller.get_status(), SyncState::Idle);
        controller.state = SyncState::Completed;
        assert_eq!(*controller.get_status(), SyncState::Completed);

        controller.reset();
        assert_eq!(*controller.get_status(), SyncState::Idle);
    }

    #[tokio::test]
    async fn test_find_peers() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        assert_eq!(controller.peers.count(&[PeerState::Found]), 0);

        controller.try_find_peers();
        assert!(matches!(
            *controller.get_status(),
            SyncState::FindingPeers { .. }
        ));
        assert_eq!(controller.peers.count(&[PeerState::Found]), 1);
        assert_eq!(network_recv.try_recv().is_err(), true);

        controller.try_find_peers();
        assert_eq!(controller.peers.count(&[PeerState::Found]), 1);

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::Publish { messages } => {
                    assert_eq!(messages.len(), 1);

                    match &messages[0] {
                        PubsubMessage::FindFile(data) => {
                            assert_eq!(data.tx_seq, 0);
                        }
                        _ => {
                            panic!("Not expected message: PubsubMessage::FindFile");
                        }
                    }
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::Publish");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_find_peers_not_in_file_cache() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        controller.tx_seq = 1;
        controller.try_find_peers();

        assert_eq!(controller.peers.count(&[PeerState::Found]), 0);

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::Publish { messages } => {
                    assert_eq!(messages.len(), 1);

                    match &messages[0] {
                        PubsubMessage::FindFile(data) => {
                            assert_eq!(data.tx_seq, 1);
                        }
                        _ => {
                            panic!("Not expected message: PubsubMessage::FindFile");
                        }
                    }
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::Publish");
                }
            }
        }

        assert!(matches!(
            *controller.get_status(),
            SyncState::FindingPeers { .. }
        ));
    }

    #[tokio::test]
    async fn test_connect_peers() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        controller.state = SyncState::FoundPeers;
        controller.try_connect();
        assert_eq!(controller.state, SyncState::Idle);
        assert_eq!(network_recv.try_recv().is_err(), true);

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        controller.peers.add_new_peer(new_peer_id, addr.clone());
        controller.try_connect();

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::DialPeer { address, peer_id } => {
                    assert_eq!(address, addr);
                    assert_eq!(peer_id, new_peer_id);
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::DialPeer");
                }
            }
        }

        assert_eq!(controller.state, SyncState::ConnectingPeers);
    }

    #[tokio::test]
    async fn test_request_chunks() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        controller.state = SyncState::AwaitingDownload;
        controller.try_request_next();
        assert_eq!(controller.state, SyncState::Idle);
        assert_eq!(network_recv.try_recv().is_err(), true);

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();

        controller.peers.add_new_peer(new_peer_id, addr.clone());
        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Connected);

        controller.try_request_next();
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::SendRequest {
                    peer_id,
                    request_id,
                    request,
                } => {
                    assert_eq!(peer_id, new_peer_id);
                    assert_eq!(
                        request,
                        Request::GetChunks(GetChunksRequest {
                            tx_seq: 0,
                            index_start: 0,
                            index_end: 123,
                        })
                    );

                    match request_id {
                        network::RequestId::Sync(sync_id) => match sync_id {
                            network::SyncId::SerialSync { tx_seq } => {
                                assert_eq!(tx_seq, 0);
                            }
                        },
                        _ => {
                            panic!("Not expected message: network::RequestId::Sync");
                        }
                    }
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::SendRequest");
                }
            }
        }

        assert!(matches!(
            *controller.get_status(),
            SyncState::Downloading { .. }
        ));
    }

    #[tokio::test]
    async fn test_ban_peer() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_default_controller(task_executor, None);

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        controller.ban_peer(new_peer_id, "unit test");

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, new_peer_id);
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

                    assert_eq!(msg, "unit test");
                }
                _ => {
                    panic!("Not received expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_report_peer() {
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (controller, mut network_recv) = create_default_controller(task_executor, None);

        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        controller.report_peer(new_peer_id, PeerAction::MidToleranceError, "unit test");

        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, new_peer_id);
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

                    assert_eq!(msg, "unit test");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[test]
    fn test_peeer_connected() {
        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_default_controller(task_executor, Some(new_peer_id));

        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        controller.peers.add_new_peer(new_peer_id, addr.clone());

        controller.on_peer_connected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Found)
        );

        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Connecting);
        controller.on_peer_connected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Connected)
        );
    }

    #[test]
    fn test_peeer_disconnected() {
        let new_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_default_controller(task_executor, Some(new_peer_id));

        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
        controller.peers.add_new_peer(new_peer_id, addr.clone());

        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Disconnecting);
        controller.on_peer_disconnected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Disconnected)
        );

        controller
            .peers
            .update_state_force(&new_peer_id, PeerState::Found);
        controller.on_peer_disconnected(new_peer_id);
        assert_eq!(
            controller.peers.peer_state(&new_peer_id),
            Some(PeerState::Disconnected)
        );

        let new_peer_id_1 = identity::Keypair::generate_ed25519().public().to_peer_id();
        controller.on_peer_disconnected(new_peer_id_1);
        assert_eq!(controller.peers.peer_state(&new_peer_id_1), None);
    }

    #[tokio::test]
    async fn test_response_mismatch_state_mismatch() {
        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (controller, mut network_recv) =
            create_default_controller(task_executor, Some(init_peer_id));

        assert_eq!(controller.handle_on_response_mismatch(init_peer_id), true);

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
                        PeerAction::LowToleranceError => {}
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

                    assert_eq!(msg, "Sync state mismatch");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_response_mismatch_peer_id_mismatch() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();
        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) =
            create_default_controller(task_executor, Some(peer_id));

        let peer_id_1 = identity::Keypair::generate_ed25519().public().to_peer_id();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: 1,
            since: Instant::now(),
        };
        assert_eq!(controller.handle_on_response_mismatch(peer_id_1), true);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id_1);
                    match action {
                        PeerAction::LowToleranceError => {}
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

                    assert_eq!(msg, "Peer id mismatch");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    #[should_panic(expected = "Invalid chunk boundaries")]
    #[ignore = "only panic in debug mode"]
    async fn test_response_panic() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, _) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: 0,
            since: Instant::now(),
        };
        controller.on_response(peer_id, chunks).await;
    }

    #[tokio::test]
    async fn test_response_chunk_len_invalid() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let mut chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count,
            since: Instant::now(),
        };

        chunks.chunks.data = Vec::new();
        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Idle);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id);
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

                    assert_eq!(msg, "Invalid chunk response data length");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_response_chunk_index_invalid() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 1,
            to_chunk: chunk_count,
            since: Instant::now(),
        };

        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Idle);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id);
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

                    assert_eq!(msg, "Invalid chunk response range");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_response_validate_failed() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count,
            since: Instant::now(),
        };

        controller.num_chunks = 1;

        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Idle);
        if let Some(msg) = network_recv.recv().await {
            match msg {
                NetworkMessage::ReportPeer {
                    peer_id,
                    action,
                    source,
                    msg,
                } => {
                    assert_eq!(peer_id, peer_id);
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

                    assert_eq!(msg, "Chunk array validation failed");
                }
                _ => {
                    panic!("Not expected message: NetworkMessage::ReportPeer");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_response_put_failed() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (_, peer_store, tx1) = create_2_store(tx_seq, chunk_count);
        let (store, _, tx2) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx2.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count,
            since: Instant::now(),
        };

        controller.data_root = tx1.data_merkle_root;

        drop(runtime);
        controller.on_response(peer_id, chunks).await;
        match controller.get_status() {
            SyncState::Failed { reason } => {
                assert!(reason.starts_with("Unexpected DB error while storing chunks: "));
            }
            _ => {
                panic!("Not expected SyncState");
            }
        }

        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_response_finilize_failed() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count,
            since: Instant::now(),
        };

        controller.tx_seq = 1;

        controller.on_response(peer_id, chunks).await;
        match controller.get_status() {
            SyncState::Failed { reason } => {
                assert!(reason.starts_with("Unexpected error during finalize_tx: "));
            }
            _ => {
                panic!("Not expected SyncState");
            }
        }

        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_response_success() {
        let peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, peer_store, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        let chunks = peer_store
            .get_chunks_with_proof_by_tx_and_index_range(tx_seq, 0, chunk_count)
            .unwrap()
            .unwrap();

        controller.state = SyncState::Downloading {
            peer_id,
            from_chunk: 0,
            to_chunk: chunk_count,
            since: Instant::now(),
        };

        controller.on_response(peer_id, chunks).await;
        assert_eq!(*controller.get_status(), SyncState::Completed);
        assert_eq!(network_recv.try_recv().is_err(), true);
    }

    #[tokio::test]
    async fn test_handle_response_failure() {
        let init_peer_id = identity::Keypair::generate_ed25519().public().to_peer_id();

        let tx_seq = 0;
        let chunk_count = 123;
        let (store, _, tx) = create_2_store(tx_seq, chunk_count);

        let runtime = TestRuntime::default();
        let task_executor = runtime.task_executor.clone();
        let (mut controller, mut network_recv) = create_controller(
            task_executor,
            Some(init_peer_id),
            store,
            tx.data_merkle_root,
            tx_seq,
            chunk_count,
        );

        for i in 0..(MAX_REQUEST_FAILURES + 1) {
            controller.handle_response_failure(init_peer_id, "unit test");
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
                            PeerAction::LowToleranceError => {}
                            _ => {
                                panic!("PeerAction expect LowToleranceError");
                            }
                        }

                        match source {
                            ReportSource::SyncService => {}
                            _ => {
                                panic!("ReportSource expect SyncService");
                            }
                        }

                        assert_eq!(msg, "unit test");
                    }
                    _ => {
                        panic!("Not expected message: NetworkMessage::ReportPeer");
                    }
                }
            }

            assert_eq!(controller.failures, i + 1);
            if i == MAX_REQUEST_FAILURES {
                assert_eq!(*controller.get_status(), SyncState::Idle);

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

                            assert_eq!(msg, "unit test");
                        }
                        _ => {
                            panic!("Not expected message: NetworkMessage::ReportPeer");
                        }
                    }
                }
            } else {
                assert_eq!(*controller.get_status(), SyncState::AwaitingDownload);
            }
        }
    }

    fn create_default_controller(
        task_executor: TaskExecutor,
        peer_id: Option<PeerId>,
    ) -> (SerialSyncController, UnboundedReceiver<NetworkMessage>) {
        let tx_seq = 0;
        let num_chunks = 123;
        let data_merkle_root = Default::default();

        let (network_send, network_recv) = mpsc::unbounded_channel::<NetworkMessage>();
        let ctx = Arc::new(SyncNetworkContext::new(network_send));

        let store = Arc::new(
            SimpleLogStore::memorydb()
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))
                .unwrap(),
        );

        let peer_id = match peer_id {
            Some(v) => v,
            _ => identity::Keypair::generate_ed25519().public().to_peer_id(),
        };

        let file_location_cache: Arc<FileLocationCache> = Default::default();
        {
            let address: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
            let msg = AnnounceFile {
                tx_seq,
                peer_id: peer_id.into(),
                at: address.into(),
                timestamp: timestamp_now(),
            };

            let local_private_key = identity::Keypair::generate_secp256k1();
            let signed_msg = msg
                .into_signed(&local_private_key)
                .expect("Sign msg failed");
            file_location_cache.insert(signed_msg);
        }

        let controller = SerialSyncController::new(
            tx_seq,
            data_merkle_root,
            num_chunks,
            ctx,
            Store::new(store, task_executor),
            file_location_cache.clone(),
        );

        (controller, network_recv)
    }

    fn create_controller(
        task_executor: TaskExecutor,
        peer_id: Option<PeerId>,
        store: Arc<SimpleLogStore>,
        data_merkle_root: DataRoot,
        tx_seq: u64,
        num_chunks: usize,
    ) -> (SerialSyncController, UnboundedReceiver<NetworkMessage>) {
        let (network_send, network_recv) = mpsc::unbounded_channel::<NetworkMessage>();
        let ctx = Arc::new(SyncNetworkContext::new(network_send));

        let peer_id = match peer_id {
            Some(v) => v,
            _ => identity::Keypair::generate_ed25519().public().to_peer_id(),
        };

        let file_location_cache: Arc<FileLocationCache> = Default::default();
        {
            let address: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
            let msg = AnnounceFile {
                tx_seq,
                peer_id: peer_id.into(),
                at: address.into(),
                timestamp: timestamp_now(),
            };

            let local_private_key = identity::Keypair::generate_secp256k1();
            let signed_msg = msg
                .into_signed(&local_private_key)
                .expect("Sign msg failed");
            file_location_cache.insert(signed_msg);
        }

        let controller = SerialSyncController::new(
            tx_seq,
            data_merkle_root,
            num_chunks,
            ctx,
            Store::new(store, task_executor),
            file_location_cache.clone(),
        );

        (controller, network_recv)
    }

    fn create_2_store(
        tx_seq: u64,
        chunk_count: usize,
    ) -> (Arc<SimpleLogStore>, Arc<SimpleLogStore>, Transaction) {
        let data_size = CHUNK_SIZE * chunk_count;
        let mut data = vec![0u8; data_size];

        for i in 0..chunk_count {
            data[i * CHUNK_SIZE] = random();
        }

        let merkle = sub_merkle_tree(&data).unwrap();
        let tx = Transaction {
            stream_ids: vec![],
            size: data_size as u64,
            data_merkle_root: merkle.root().into(),
            seq: tx_seq,
            data: vec![],
        };

        let store = Arc::new(
            SimpleLogStore::memorydb()
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))
                .unwrap(),
        );

        let peer_store = Arc::new(
            SimpleLogStore::memorydb()
                .map_err(|e| format!("Unable to start in-memory store: {:?}", e))
                .unwrap(),
        );

        store.put_tx(tx.clone()).unwrap();
        peer_store.put_tx(tx.clone()).unwrap();
        for start_index in (0..chunk_count).step_by(peer_store.chunk_batch_size) {
            let end = cmp::min(
                (start_index + peer_store.chunk_batch_size) * CHUNK_SIZE,
                data.len(),
            );
            let chunk_array = ChunkArray {
                data: data[start_index * CHUNK_SIZE..end].to_vec(),
                start_index: start_index as u32,
            };
            peer_store.put_chunks(tx.seq, chunk_array.clone()).unwrap();
        }
        peer_store.finalize_tx(tx.seq).unwrap();

        (store, peer_store, tx)
    }
}
