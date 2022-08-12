use network::{Multiaddr, PeerId};
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeerState {
    Found,
    Connecting,
    Connected,
    Disconnecting,
    Disconnected,
    Failed,
}

struct PeerInfo {
    /// The reported/connected address of the peer.
    pub addr: Multiaddr,

    /// The current state of the peer.
    pub state: PeerState,

    /// Timestamp of the last state change.
    pub since: Instant,
}

impl PeerInfo {
    fn update_state(&mut self, new_state: PeerState) {
        self.state = new_state;
        self.since = Instant::now();
    }
}

#[derive(Default)]
pub(crate) struct SyncPeers {
    peers: HashMap<PeerId, PeerInfo>,
}

impl SyncPeers {
    pub fn add_new_peer(&mut self, peer_id: PeerId, addr: Multiaddr) -> bool {
        if self.peers.contains_key(&peer_id) {
            return false;
        }

        self.peers.insert(
            peer_id,
            PeerInfo {
                addr,
                state: PeerState::Found,
                since: Instant::now(),
            },
        );

        true
    }

    pub fn update_state(&mut self, peer_id: PeerId, from: PeerState, to: PeerState) {
        match self.peers.get_mut(&peer_id) {
            Some(info) if info.state == from => info.update_state(to),
            Some(PeerInfo { state, .. }) => {
                warn!(%peer_id, expected=?from, got=?state, "Received update_state in unexpected peer state");
                *state = PeerState::Failed;
            }
            None => {
                warn!(%peer_id, "Received update_state for nonexistent peer");
            }
        }
    }

    pub fn peer_state(&self, peer_id: PeerId) -> Option<PeerState> {
        self.peers.get(&peer_id).map(|info| info.state)
    }

    pub fn random_peer(&self, state: PeerState) -> Option<(PeerId, Multiaddr)> {
        self.peers
            .iter()
            .filter(|(_, info)| info.state == state)
            .map(|(peer_id, info)| (*peer_id, info.addr.clone()))
            .choose(&mut rand::thread_rng())
    }

    pub fn count(&self, states: &[PeerState]) -> usize {
        self.peers
            .values()
            .filter(|info| states.contains(&info.state))
            .count()
    }

    pub fn transition(&mut self) {
        let mut bad_peers = vec![];

        for (peer_id, info) in self.peers.iter_mut() {
            match info.state {
                PeerState::Connecting => {
                    // handle timeout
                    // Note: timeouts and other connection issues generate SwarmEvents,
                    // however, these are currently not propagated to the higher layers.
                    // TODO(ionian-dev): consider handling connection failure events.
                    if info.since.elapsed() >= PEER_CONNECT_TIMEOUT {
                        warn!(%peer_id, "Peer connection timeout");
                        info.state = PeerState::Failed;
                        bad_peers.push(*peer_id);
                    }
                }

                PeerState::Disconnecting => {
                    if info.since.elapsed() >= PEER_DISCONNECT_TIMEOUT {
                        warn!(%peer_id, "Peer disconnect timeout");
                        info.state = PeerState::Failed;
                        bad_peers.push(*peer_id);
                    }
                }

                PeerState::Failed => bad_peers.push(*peer_id),

                _ => {}
            }
        }

        for peer_id in bad_peers {
            self.peers.remove(&peer_id);
        }
    }
}
