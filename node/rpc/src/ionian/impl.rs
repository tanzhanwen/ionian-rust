use super::api::RpcServer;
use crate::error;
use crate::types::{FileInfo, Segment, SegmentWithProof, Status};
use crate::Context;
use chunk_pool::SegmentInfo;
use jsonrpsee::core::async_trait;
use jsonrpsee::core::RpcResult;
use shared_types::{DataRoot, CHUNK_SIZE};
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

        let _ = self.ctx.chunk_pool.validate_segment_size(&segment.data)?;

        let seq = self
            .ctx
            .log_store
            .get_tx_seq_by_data_root(&segment.root)
            .await?;

        let mut need_cache = false;

        if self
            .ctx
            .chunk_pool
            .check_already_has_cache(&segment.root)
            .await
        {
            need_cache = true;
        }

        if !need_cache {
            need_cache = self.check_need_cache(seq, segment.file_size).await?;
        }

        segment.validate(self.ctx.config.chunks_per_segment)?;

        let seg_info = SegmentInfo {
            root: segment.root,
            seg_data: segment.data,
            seg_index: segment.index,
            chunks_per_segment: self.ctx.config.chunks_per_segment,
        };

        if need_cache {
            self.ctx.chunk_pool.cache_chunks(seg_info).await?;
        } else {
            self.ctx
                .chunk_pool
                .write_chunks(seg_info, seq.unwrap(), segment.file_size)
                .await?;
        }

        Ok(())
    }

    async fn download_segment(
        &self,
        data_root: DataRoot,
        start_index: usize,
        end_index: usize,
    ) -> RpcResult<Option<Segment>> {
        debug!("ionian_downloadSegment()");

        if start_index >= end_index {
            return Err(error::invalid_params("end_index", "invalid chunk index"));
        }

        if end_index - start_index > self.ctx.config.chunks_per_segment {
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
                .get_chunks_by_tx_and_index_range(tx_seq, start_index, end_index)
                .await?
        );

        Ok(Some(Segment(segment.data)))
    }

    async fn download_segment_with_proof(
        &self,
        data_root: DataRoot,
        index: usize,
    ) -> RpcResult<Option<SegmentWithProof>> {
        debug!("ionian_downloadSegmentWithProof()");

        let tx_seq = try_option!(
            self.ctx
                .log_store
                .get_tx_seq_by_data_root(&data_root)
                .await?
        );

        // ensure file already finalized
        if !self.ctx.log_store.check_tx_completed(tx_seq).await? {
            return Err(error::invalid_params("root", "file not finalized yet"));
        }

        let tx = try_option!(self.ctx.log_store.get_tx_by_seq_number(tx_seq).await?);

        // validate index
        let chunks_per_segment = self.ctx.config.chunks_per_segment;
        let (num_segments, last_segment_size) =
            SegmentWithProof::split_file_into_segments(tx.size as usize, chunks_per_segment)?;

        if index >= num_segments {
            return Err(error::invalid_params("index", "index out of bound"));
        }

        // calculate chunk start and end index
        let start_index = index * chunks_per_segment;
        let end_index = if index == num_segments - 1 {
            // last segment without padding chunks by flow
            start_index + last_segment_size / CHUNK_SIZE
        } else {
            start_index + chunks_per_segment
        };

        let segment = try_option!(
            self.ctx
                .log_store
                .get_chunks_with_proof_by_tx_and_index_range(tx_seq, start_index, end_index)
                .await?
        );

        let proof = tx.compute_segment_proof(&segment, chunks_per_segment)?;

        Ok(Some(SegmentWithProof {
            root: data_root,
            data: segment.chunks.data,
            index,
            proof,
            file_size: tx.size as usize,
        }))
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

impl RpcServerImpl {
    async fn check_need_cache(&self, seq: Option<u64>, file_size: usize) -> RpcResult<bool> {
        let mut need_cache = false;

        if let Some(tx_seq) = seq {
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

            if tx.size != file_size as u64 {
                return Err(error::invalid_params(
                    "file_size",
                    "segment file size not matched with tx file size",
                ));
            }
        } else {
            //Check whether file is small enough to cache in the system
            if file_size > self.ctx.config.max_cache_file_size {
                return Err(error::invalid_params(
                    "file_size",
                    "caching of large file when tx is unavailable is not supported",
                ));
            }

            need_cache = true;
        }

        Ok(need_cache)
    }
}
