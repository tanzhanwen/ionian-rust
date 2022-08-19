use super::api::RpcServer;
use crate::types::RpcResult;
use crate::{error, Context};
use futures::prelude::*;
use jsonrpsee::core::async_trait;
use sync::{SyncRequest, SyncResponse, SyncSender};
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
    async fn start_sync_file(&self, tx_seq: u64) -> RpcResult<()> {
        info!("admin_startSyncFile({tx_seq})");

        let response = self
            .sync_send()?
            .request(SyncRequest::SyncFile { tx_seq })
            .await
            .map_err(|e| error::internal_error(format!("Failed to send sync command: {:?}", e)))?;

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
            .sync_send()?
            .request(SyncRequest::SyncStatus { tx_seq })
            .await
            .map_err(|e| error::internal_error(format!("Failed to send sync command: {:?}", e)))?;

        match response {
            SyncResponse::SyncStatus { status } => Ok(status),
            _ => Err(error::internal_error("unexpected response type")),
        }
    }
}

impl RpcServerImpl {
    fn sync_send(&self) -> Result<&SyncSender, jsonrpsee::core::Error> {
        match &self.ctx.sync_send {
            Some(sync_send) => Ok(sync_send),
            None => Err(error::internal_error("Sync send is not initialized.")),
        }
    }
}
