#[macro_use]
extern crate tracing;

mod admin;
mod config;
mod error;
mod ionian;
mod types;

use admin::RpcServer as AdminRpcServer;
use chunk_pool::MemoryChunkPool;
use futures::channel::mpsc::Sender;
use ionian::RpcServer as IonianRpcServer;
use jsonrpsee::core::RpcResult;
use jsonrpsee::http_server::{HttpServerBuilder, HttpServerHandle};
use network::NetworkGlobals;
use network::NetworkMessage;
use std::error::Error;
use std::sync::Arc;
use storage_async::Store;
use sync::{SyncRequest, SyncResponse, SyncSender};
use task_executor::ShutdownReason;
use tokio::sync::mpsc::UnboundedSender;

pub use config::Config as RPCConfig;

/// A wrapper around all the items required to spawn the HTTP server.
///
/// The server will gracefully handle the case where any fields are `None`.
#[derive(Clone)]
pub struct Context {
    pub config: RPCConfig,
    pub network_globals: Arc<NetworkGlobals>,
    pub network_send: UnboundedSender<NetworkMessage>,
    pub sync_send: SyncSender,
    pub chunk_pool: Arc<MemoryChunkPool>,
    pub log_store: Store,
    pub shutdown_sender: Sender<ShutdownReason>,
}

impl Context {
    pub fn send_network(&self, msg: NetworkMessage) -> RpcResult<()> {
        self.network_send
            .send(msg)
            .map_err(|e| error::internal_error(format!("Failed to send network message: {:?}", e)))
    }

    pub async fn request_sync(&self, request: SyncRequest) -> RpcResult<SyncResponse> {
        self.sync_send
            .request(request)
            .await
            .map_err(|e| error::internal_error(format!("Failed to send sync request: {:?}", e)))
    }
}

pub async fn run_server(ctx: Context) -> Result<HttpServerHandle, Box<dyn Error>> {
    let server = HttpServerBuilder::default()
        .build(ctx.config.listen_address)
        .await?;

    let mut ionian = (ionian::RpcServerImpl { ctx: ctx.clone() }).into_rpc();
    let admin = (admin::RpcServerImpl { ctx }).into_rpc();
    ionian.merge(admin)?;

    let addr = server.local_addr()?;
    let handle = server.start(ionian)?;
    info!("Server started http://{}", addr);

    Ok(handle)
}
