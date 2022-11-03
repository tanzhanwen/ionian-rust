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

#[cfg(test)]
mod tests {
    use std::{
        ops::Sub,
        time::{Duration, Instant},
    };

    use network::PeerId;

    use crate::Config;

    use super::PeerManager;

    impl PeerManager {
        pub fn size(&self) -> usize {
            self.peers.len()
        }
    }

    #[test]
    fn test_add() {
        let mut manager = PeerManager::new(Config::default());

        let peer1 = PeerId::random();
        assert_eq!(manager.add(peer1, false), true);
        assert_eq!(manager.add(peer1, false), false);
        assert_eq!(manager.add(peer1, true), false);
        assert_eq!(manager.size(), 1);

        let peer2 = PeerId::random();
        assert_eq!(manager.add(peer2, false), true);
        assert_eq!(manager.size(), 2);
    }

    #[test]
    fn test_remove() {
        let mut manager = PeerManager::new(Config::default());

        let peer1 = PeerId::random();
        assert_eq!(manager.add(peer1, false), true);
        let peer2 = PeerId::random();
        assert_eq!(manager.add(peer2, true), true);

        let peer3 = PeerId::random();
        assert_eq!(manager.remove(&peer3), false);
        assert_eq!(manager.remove(&peer1), true);
        assert_eq!(manager.remove(&peer2), true);
        assert_eq!(manager.size(), 0);
    }

    #[test]
    fn test_update() {
        let mut manager = PeerManager::new(Config::default());

        let peer1 = PeerId::random();
        assert_eq!(manager.add(peer1, false), true);
        let ts1 = manager.peers.get(&peer1).unwrap().since;

        let peer2 = PeerId::random();
        assert_eq!(manager.update(&peer2), false);
        assert_eq!(manager.update(&peer1), true);

        let ts2 = manager.peers.get(&peer1).unwrap().since;
        assert!(ts2 > ts1);
    }

    #[test]
    fn test_expired() {
        let config = Config::default();
        let mut manager = PeerManager::new(config.clone());

        let mut peers = vec![];

        // setup incoming peers: max + 3
        for _ in 0..(config.max_idle_incoming_peers + 3) {
            let peer_id = PeerId::random();
            peers.push(peer_id);
            assert_eq!(manager.add(peer_id, false), true);
        }

        // setup outgoing peers: max + 2
        for _ in 0..(config.max_idle_outgoing_peers + 2) {
            let peer_id = PeerId::random();
            peers.push(peer_id);
            assert_eq!(manager.add(peer_id, true), true);
        }

        // change timestamp for all peers
        let idle_timeout = Duration::from_secs(config.idle_time_secs);
        for peer_id in peers.iter() {
            let peer = manager.peers.get_mut(peer_id).unwrap();
            peer.since = Instant::now().sub(idle_timeout);
        }

        assert_eq!(
            manager.expired(false, config.max_idle_incoming_peers).len(),
            3
        );
        assert_eq!(
            manager.expired(true, config.max_idle_outgoing_peers).len(),
            2
        );
        assert_eq!(manager.expired_peers().len(), 5);
    }
}
