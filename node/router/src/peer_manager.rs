use crate::Config;
use network::PeerId;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::time::Instant;

/// Connected peer info.
struct PeerInfo {
    /// Outgoing or incoming connection.
    outgoing: bool,
    /// Last update time.
    since: Instant,
}

impl PeerInfo {
    fn new(outgoing: bool) -> Self {
        Self {
            outgoing,
            since: Instant::now(),
        }
    }

    fn elapsed_secs(&self) -> u64 {
        self.since.elapsed().as_secs()
    }
}

/// Manages connected outgoing and incoming peers.
///
/// Basically, records the last update time for RPC requests/responses,
/// and disconnect some peers periodically if too many idle ones. So that,
/// there are enough incoming connections available for other peers to
/// sync file from this peer.
///
/// On the other hand, pub-sub message propagation rely on peer connections,
/// so a peer should have enough peers connected to broadcast pub-sub messages.
#[derive(Default)]
pub struct PeerManager {
    peers: HashMap<PeerId, PeerInfo>,
    config: Config,
}

impl PeerManager {
    pub fn new(config: Config) -> Self {
        Self {
            peers: Default::default(),
            config,
        }
    }

    pub fn add(&mut self, peer_id: PeerId, outgoing: bool) -> bool {
        let old = self.peers.insert(peer_id, PeerInfo::new(outgoing));
        if old.is_none() {
            debug!(%peer_id, %outgoing, "New peer added");
            true
        } else {
            // peer should not be connected multiple times
            error!(%peer_id, %outgoing, "Peer already exists");
            false
        }
    }

    pub fn remove(&mut self, peer_id: &PeerId) -> bool {
        if self.peers.remove(peer_id).is_some() {
            debug!(%peer_id, "Peer removed");
            true
        } else {
            error!(%peer_id, "Peer not found to remove");
            false
        }
    }

    /// Updates the timestamp of specified peer if any.
    pub fn update(&mut self, peer_id: &PeerId) -> bool {
        match self.peers.get_mut(peer_id) {
            Some(peer) => {
                peer.since = Instant::now();
                trace!(%peer_id, "Peer updated");
                true
            }
            None => {
                error!(%peer_id, "Peer not found to update");
                false
            }
        }
    }

    /// Finds idle peers for garbage collection in advance.
    pub fn expired_peers(&self) -> Vec<PeerId> {
        let mut expired_outgoing = self.expired(true, self.config.max_idle_outgoing_peers);
        let mut expired_incoming = self.expired(false, self.config.max_idle_incoming_peers);
        expired_outgoing.append(&mut expired_incoming);
        expired_outgoing
    }

    fn expired(&self, outgoing: bool, max_idle: usize) -> Vec<PeerId> {
        let expired: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|(_, peer)| {
                peer.outgoing == outgoing && peer.elapsed_secs() >= self.config.idle_time_secs
            })
            .map(|(peer_id, _)| *peer_id)
            .collect();

        if expired.len() <= max_idle {
            return vec![];
        }

        let num_expired = expired.len() - max_idle;
        expired
            .into_iter()
            .choose_multiple(&mut rand::thread_rng(), num_expired)
    }
}
