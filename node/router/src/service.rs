use crate::Config;
use crate::{libp2p_event_handler::Libp2pEventHandler, peer_manager::PeerManager};
use file_location_cache::FileLocationCache;
use futures::{channel::mpsc::Sender, prelude::*};
use miner::MinerMessage;
use network::{
    BehaviourEvent, Keypair, Libp2pEvent, NetworkGlobals, NetworkMessage, RequestId,
    Service as LibP2PService, Swarm,
};
use std::sync::Arc;
use std::time::Duration;
use storage::log_store::Store as LogStore;
use storage_async::Store;
use sync::{SyncMessage, SyncSender};
use task_executor::ShutdownReason;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::time::interval;

/// Service that handles communication between internal services and the libp2p service.
pub struct RouterService {
    config: Config,

    /// The underlying libp2p service that drives all the network interactions.
    libp2p: LibP2PService<RequestId>,

    /// A collection of global variables, accessible outside of the network service.
    network_globals: Arc<NetworkGlobals>,

    /// The receiver channel for Ionian to communicate with the network service.
    network_recv: mpsc::UnboundedReceiver<NetworkMessage>,

    /// All connected peers.
    peers: Arc<RwLock<PeerManager>>,

    /// Handler for libp2p events.
    libp2p_event_handler: Libp2pEventHandler,
}

impl RouterService {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        executor: task_executor::TaskExecutor,
        libp2p: LibP2PService<RequestId>,
        network_globals: Arc<NetworkGlobals>,
        network_recv: mpsc::UnboundedReceiver<NetworkMessage>,
        network_send: mpsc::UnboundedSender<NetworkMessage>,
        sync_send: SyncSender,
        _miner_send: Option<broadcast::Sender<MinerMessage>>,
        store: Arc<RwLock<dyn LogStore>>,
        file_location_cache: Arc<FileLocationCache>,
        local_keypair: Keypair,
        config: Config,
    ) {
        let store = Store::new(store, executor.clone());
        let peers = Arc::new(RwLock::new(PeerManager::new(config.clone())));

        // create the network service and spawn the task
        let router = RouterService {
            config,
            libp2p,
            network_globals: network_globals.clone(),
            network_recv,
            peers: peers.clone(),
            libp2p_event_handler: Libp2pEventHandler::new(
                network_globals,
                network_send,
                sync_send,
                local_keypair,
                store,
                file_location_cache,
                peers,
            ),
        };

        // spawn service
        let shutdown_sender = executor.shutdown_sender();

        executor.spawn(router.main(shutdown_sender), "router");
    }

    async fn main(mut self, mut shutdown_sender: Sender<ShutdownReason>) {
        let mut heartbeat = interval(Duration::from_secs(self.config.heartbeat_interval_secs));

        loop {
            tokio::select! {
                // handle a message sent to the network
                Some(msg) = self.network_recv.recv() => self.on_network_msg(msg, &mut shutdown_sender).await,

                // handle event coming from the network
                event = self.libp2p.next_event() => self.on_libp2p_event(event, &mut shutdown_sender).await,

                // heartbeat
                _ = heartbeat.tick() => self.on_heartbeat().await,
            }
        }
    }

    /// Handle an event received from the network.
    async fn on_libp2p_event(
        &mut self,
        ev: Libp2pEvent<RequestId>,
        shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        debug!(?ev, "Received new event from libp2p");

        match ev {
            Libp2pEvent::Behaviour(event) => match event {
                BehaviourEvent::PeerConnectedOutgoing(peer_id) => {
                    self.libp2p_event_handler
                        .on_peer_connected(peer_id, true)
                        .await;
                }
                BehaviourEvent::PeerConnectedIncoming(peer_id) => {
                    self.libp2p_event_handler
                        .on_peer_connected(peer_id, false)
                        .await;
                }
                BehaviourEvent::PeerBanned(_) | BehaviourEvent::PeerUnbanned(_) => {
                    // No action required for these events.
                }
                BehaviourEvent::PeerDisconnected(peer_id) => {
                    self.libp2p_event_handler
                        .on_peer_disconnected(peer_id)
                        .await;
                }
                BehaviourEvent::RequestReceived {
                    peer_id,
                    id,
                    request,
                } => {
                    if self.network_globals.peers.read().is_connected(&peer_id) {
                        self.libp2p_event_handler
                            .on_rpc_request(peer_id, id, request)
                            .await;
                    } else {
                        debug!(%peer_id, ?request, "Dropping request of disconnected peer");
                    }
                }
                BehaviourEvent::ResponseReceived {
                    peer_id,
                    id,
                    response,
                } => {
                    self.libp2p_event_handler
                        .on_rpc_response(peer_id, id, response)
                        .await;
                }
                BehaviourEvent::RPCFailed { id, peer_id } => {
                    self.libp2p_event_handler.on_rpc_error(peer_id, id).await;
                }
                BehaviourEvent::StatusPeer(peer_id) => {
                    self.libp2p_event_handler.send_status(peer_id);
                }
                BehaviourEvent::PubsubMessage {
                    id,
                    propagation_source,
                    source,
                    message,
                    ..
                } => {
                    let result = self
                        .libp2p_event_handler
                        .on_pubsub_message(propagation_source, source, &id, message)
                        .await;

                    self.libp2p
                        .swarm
                        .behaviour_mut()
                        .report_message_validation_result(&propagation_source, id, result);
                }
            },
            Libp2pEvent::NewListenAddr(multiaddr) => {
                self.network_globals
                    .listen_multiaddrs
                    .write()
                    .push(multiaddr);
            }
            Libp2pEvent::ZeroListeners => {
                let _ = shutdown_sender
                    .send(ShutdownReason::Failure(
                        "All listeners are closed. Unable to listen",
                    ))
                    .await
                    .map_err(|e| {
                        warn!(
                            error = %e,
                            "failed to send a shutdown signal",
                        )
                    });
            }
        }
    }

    /// Handle a message sent to the network service.
    async fn on_network_msg(
        &mut self,
        msg: NetworkMessage,
        _shutdown_sender: &mut Sender<ShutdownReason>,
    ) {
        debug!(?msg, "Received new message");

        match msg {
            NetworkMessage::SendRequest {
                peer_id,
                request,
                request_id,
            } => {
                self.libp2p.send_request(peer_id, request_id, request);
            }
            NetworkMessage::SendResponse {
                peer_id,
                response,
                id,
            } => {
                self.libp2p.send_response(peer_id, id, response);
            }
            NetworkMessage::SendErrorResponse {
                peer_id,
                error,
                id,
                reason,
            } => {
                self.libp2p.respond_with_error(peer_id, id, error, reason);
            }
            NetworkMessage::Publish { messages } => {
                if self.libp2p.swarm.connected_peers().next().is_none() {
                    // this is a boardcast message, when current node doesn't have any peers connected, try to connect any peer in config
                    for multiaddr in &self.config.libp2p_nodes {
                        match Swarm::dial(&mut self.libp2p.swarm, multiaddr.clone()) {
                            Ok(()) => {
                                debug!(address = %multiaddr, "Dialing libp2p peer");
                                break;
                            }
                            Err(err) => {
                                debug!(address = %multiaddr, error = ?err, "Could not connect to peer")
                            }
                        };
                    }
                }

                let mut topic_kinds = Vec::new();
                for message in &messages {
                    if !topic_kinds.contains(&message.kind()) {
                        topic_kinds.push(message.kind());
                    }
                }
                debug!(
                    count = messages.len(),
                    topics = ?topic_kinds,
                    "Sending pubsub messages",
                );
                self.libp2p.swarm.behaviour_mut().publish(messages);
            }
            NetworkMessage::ReportPeer {
                peer_id,
                action,
                source,
                msg,
            } => self.libp2p.report_peer(&peer_id, action, source, msg),
            NetworkMessage::GoodbyePeer {
                peer_id,
                reason,
                source,
            } => self.libp2p.goodbye_peer(&peer_id, reason, source),
            NetworkMessage::DialPeer { address, peer_id } => {
                if self.libp2p.swarm.is_connected(&peer_id) {
                    self.libp2p_event_handler
                        .send_to_sync(SyncMessage::PeerConnected { peer_id });
                } else {
                    match Swarm::dial(&mut self.libp2p.swarm, address.clone()) {
                        Ok(()) => debug!(%address, "Dialing libp2p peer"),
                        Err(err) => {
                            info!(%address, error = ?err, "Failed to dial peer");
                            self.libp2p_event_handler
                                .send_to_sync(SyncMessage::DailFailed { peer_id, err });
                        }
                    };
                }
            }
            NetworkMessage::AnnounceLocalFile { tx_id } => {
                if let Some(msg) = self
                    .libp2p_event_handler
                    .construct_announce_file_message(tx_id)
                {
                    self.libp2p_event_handler.publish(msg);
                }
            }
        }
    }

    async fn on_heartbeat(&mut self) {
        let expired_peers = self.peers.write().await.expired_peers();

        trace!("heartbeat, expired peers = {:?}", expired_peers.len());

        for peer_id in expired_peers {
            // async operation, once peer disconnected, swarm event `PeerDisconnected`
            // will be polled to handle in advance.
            match self.libp2p.swarm.disconnect_peer_id(peer_id) {
                Ok(_) => debug!(%peer_id, "Peer expired and disconnect it"),
                Err(_) => error!(%peer_id, "Peer expired but failed to disconnect"),
            }
        }
    }
}

impl Drop for RouterService {
    fn drop(&mut self) {
        info!("Router service shutdown");
    }
}
