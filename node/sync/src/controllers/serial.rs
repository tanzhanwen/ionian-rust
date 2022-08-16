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

#[derive(Debug)]
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

                SyncState::Completed | SyncState::Failed { .. } => {}
            }
        }
    }
}
