use super::api::RpcServer;
use crate::{error, Context};
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use network::NetworkMessage;
use sync::{SyncRequest, SyncResponse};
use task_executor::ShutdownReason;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn shutdown(&self) -> RpcResult<()> {
        info!("admin_shutdown()");

        self.ctx
            .shutdown_sender
            .clone()
            .send(ShutdownReason::Success("Shutdown by admin"))
            .await
            .map_err(|e| error::internal_error(format!("Failed to send shutdown command: {:?}", e)))
    }

    #[tracing::instrument(skip(self), err)]
    async fn announce_local_file(&self, tx_seq: u64) -> RpcResult<()> {
        info!("admin_startSyncFile({tx_seq})");

        self.ctx
            .send_network(NetworkMessage::AnnounceLocalFile { tx_seq })
    }

    #[tracing::instrument(skip(self), err)]
    async fn start_sync_file(&self, tx_seq: u64) -> RpcResult<()> {
        info!("admin_startSyncFile({tx_seq})");

        let response = self
            .ctx
            .request_sync(SyncRequest::SyncFile { tx_seq })
            .await?;

        match response {
            SyncResponse::SyncFile { err } => {
                if err.is_empty() {
                    Ok(())
                } else {
                    Err(error::internal_error(err))
                }
            }
            _ => Err(error::internal_error("unexpected response type")),
        }
    }

    #[tracing::instrument(skip(self), err)]
    async fn get_sync_status(&self, tx_seq: u64) -> RpcResult<String> {
        info!("admin_getSyncStatus({tx_seq})");

        let response = self
            .ctx
            .request_sync(SyncRequest::SyncStatus { tx_seq })
            .await?;

        match response {
            SyncResponse::SyncStatus { status } => Ok(status),
            _ => Err(error::internal_error("unexpected response type")),
        }
    }
}
