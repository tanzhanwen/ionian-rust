use crate::api::RpcServer;
use crate::commands::Command;
use crate::error;
use crate::types::Status;
use futures::channel::mpsc;
use jsonrpsee::core::async_trait;

pub struct RpcServerImpl {
    pub command_sender: mpsc::Sender<Command>,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    async fn get_status(&self) -> Result<Status, jsonrpsee::core::Error> {
        info!("ionian_getStatus()");
        Ok(Status {
            msg: "hello".to_string(),
        })
    }

    async fn send_status(&self) -> Result<String, jsonrpsee::core::Error> {
        info!("ionian_sendStatus()");
        // self.command_sender.clone().try_send(Command::SendStatus).expect("success");
        // Ok("ok".to_string())
        Err(error::not_supported())
    }
}
