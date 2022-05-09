mod api;
mod commands;
mod config;
mod error;
mod r#impl;
mod types;

#[macro_use]
extern crate tracing;

use api::RpcServer;
use futures::channel::mpsc;
use jsonrpsee::http_server::{HttpServerBuilder, HttpServerHandle};
use r#impl::RpcServerImpl;
use std::error::Error;

pub use commands::Command;
pub use config::Config;

pub async fn run_server(
    config: &Config,
    command_sender: mpsc::Sender<Command>,
) -> Result<HttpServerHandle, Box<dyn Error>> {
    let server = HttpServerBuilder::default()
        .build(config.listen_address)
        .await?;

    let addr = server.local_addr()?;
    let rpc_impl = RpcServerImpl { command_sender };
    let handle = server.start(rpc_impl.into_rpc())?;
    info!("Server started http://{}", addr);

    Ok(handle)
}
