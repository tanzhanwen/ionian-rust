use network::{NetworkMessage, PubsubMessage};
use tokio::sync::mpsc;

pub struct SyncNetworkContext {
    network_send: mpsc::UnboundedSender<NetworkMessage>,
}

impl SyncNetworkContext {
    pub fn new(network_send: mpsc::UnboundedSender<NetworkMessage>) -> Self {
        Self { network_send }
    }

    /// Sends an arbitrary network message.
    pub fn send(&self, msg: NetworkMessage) {
        self.network_send.send(msg).unwrap_or_else(|_| {
            warn!("Could not send message to the network service");
        })
    }

    /// Publishes a single message.
    pub fn publish(&self, msg: PubsubMessage) {
        self.send(NetworkMessage::Publish {
            messages: vec![msg],
        });
    }
}
