use super::api::RpcServer;
use crate::error;
use crate::types::{FileInfo, Segment, SegmentWithProof, Status};
use crate::Context;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use shared_types::DataRoot;
use storage::try_option;

pub struct RpcServerImpl {
    pub ctx: Context,
}

#[async_trait]
impl RpcServer for RpcServerImpl {
    #[tracing::instrument(skip(self), err)]
    async fn get_status(&self) -> RpcResult<Status> {
        info!("ionian_getStatus()");

        Ok(Status {
            connected_peers: self.ctx.network_globals.connected_peers(),
        })
    }

    async fn upload_segment(&self, segment: SegmentWithProof) -> RpcResult<()> {
        debug!("ionian_uploadSegment()");

        // TODO(qhz): allow to cache small files before log entry retrieved from blockchain.
        let tx_seq = match self
            .ctx
            .log_store
            .get_tx_seq_by_data_root(&segment.root)
            .await?
        {
            Some(seq) => seq,
            None => return Err(error::invalid_params("root", "data root not found")),
        };

        // Transaction already finalized for the specified file data root.
        if self.ctx.log_store.check_tx_completed(tx_seq).await? {
            return Err(error::invalid_params(
                "root",
                "already uploaded and finalized",
            ));
        }

        let tx = match self.ctx.log_store.get_tx_by_seq_number(tx_seq).await? {
            Some(tx) => tx,
            None => return Err(error::invalid_params("root", "data root not found")),
        };

        segment.validate(tx.size as usize, self.ctx.config.chunks_per_segment)?;

        // Chunk pool will validate the data size.
        let chunk_index = segment.chunk_index(self.ctx.config.chunks_per_segment);
        self.ctx
            .chunk_pool
            .add_chunks(segment.root, segment.data, chunk_index)
            .await?;

        Ok(())
    }

    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: u32,
        end_index: u32,
    ) -> RpcResult<Option<Segment>> {
        debug!("ionian_downloadSegment()");

        if start_index >= end_index {
            return Err(error::invalid_params("end_index", "invalid chunk index"));
        }

        if end_index - start_index > self.ctx.config.chunks_per_segment as u32 {
            return Err(error::invalid_params(
                "end_index",
                format!(
                    "exceeds maximum chunks {}",
                    self.ctx.config.chunks_per_segment
                ),
            ));
        }

        let tx_seq = try_option!(
            self.ctx
                .log_store
                .get_tx_seq_by_data_root(&data_root)
                .await?
        );
        let segment = try_option!(
            self.ctx
                .log_store
                .get_chunks_by_tx_and_index_range(tx_seq, start_index as usize, end_index as usize)
                .await?
        );

        Ok(Some(Segment(segment.data)))
    }

    async fn get_file_info(&self, data_root: DataRoot) -> RpcResult<Option<FileInfo>> {
        debug!("get_file_info()");

        let tx_seq = try_option!(
            self.ctx
                .log_store
                .get_tx_seq_by_data_root(&data_root)
                .await?
        );
        let tx = try_option!(self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?);

        Ok(Some(FileInfo {
            tx,
            finalized: self.ctx.log_store.check_tx_completed(tx_seq).await?,
        }))
    }
}
