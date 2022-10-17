use super::chunk_cache::{ChunkPoolCache, MemoryCachedFile};
use super::chunk_write_control::ChunkPoolWriteCtrl;
use crate::Config;
use anyhow::{bail, Result};
use async_lock::Mutex;
use shared_types::{
    bytes_to_chunks, compute_segment_size, ChunkArray, DataRoot, Transaction, CHUNK_SIZE,
};
use storage_async::Store;
use tokio::sync::mpsc::UnboundedSender;

struct Inner {
    config: Config,
    segment_cache: ChunkPoolCache,
    write_control: ChunkPoolWriteCtrl,
}

impl Inner {
    fn new(config: Config) -> Self {
        Inner {
            config,
            segment_cache: ChunkPoolCache::new(config),
            write_control: ChunkPoolWriteCtrl::new(config),
        }
    }

    fn on_write_cache_succeeded(&mut self, root: &DataRoot, cached_segs_chunks: usize) {
        assert!(self.write_control.total_writings > 0);
        self.write_control.total_writings -= 1;
        assert!(self.segment_cache.total_chunks >= cached_segs_chunks);
        self.segment_cache.total_chunks -= cached_segs_chunks;

        let file = self.segment_cache.get_file_mut(root).unwrap();
        file.cached_chunk_num -= cached_segs_chunks;
    }

    // TODO(qhz) the same with `on_write_cache_succeeded`?
    fn on_write_cache_failed(&mut self, root: &DataRoot, cached_segs_chunks: usize) {
        assert!(self.write_control.total_writings > 0);
        self.write_control.total_writings -= 1;
        assert!(self.segment_cache.total_chunks >= cached_segs_chunks);
        self.segment_cache.total_chunks -= cached_segs_chunks;

        let file = self.segment_cache.get_file_mut(root).unwrap();
        file.cached_chunk_num -= cached_segs_chunks;
    }

    /// Return the tx seq and all segments that belong to the root.
    fn get_all_cached_segments_to_write(
        &mut self,
        root: &DataRoot,
    ) -> Result<(u64, usize, Vec<ChunkArray>)> {
        // Limits the number of writing threads.
        if self.write_control.total_writings >= self.config.max_writings {
            bail!("too many data writing: {}", self.config.max_writings);
        }

        let file = match self.segment_cache.remove_file(root) {
            Some(f) => f,
            None => bail!("file not found to write into store {:?}", root),
        };
        let tx_seq = file.tx_seq;
        let cached_chunk_num = file.cached_chunk_num;
        let segs = file.segments.into_iter().map(|(_k, v)| v).collect();

        self.write_control.total_writings += 1;

        Ok((tx_seq, cached_chunk_num, segs))
    }
}

pub struct SegmentInfo {
    pub root: DataRoot,
    pub seg_data: Vec<u8>,
    pub seg_index: usize,
    pub chunks_per_segment: usize,
}

impl From<SegmentInfo> for ChunkArray {
    fn from(seg_info: SegmentInfo) -> Self {
        let start_index = seg_info.seg_index * seg_info.chunks_per_segment;
        ChunkArray {
            data: seg_info.seg_data,
            start_index: start_index as u64,
        }
    }
}

/// Caches data chunks in memory before the entire file uploaded to storage node
/// and data root verified on blockchain.
pub struct MemoryChunkPool {
    inner: Mutex<Inner>,
    log_store: Store,
    sender: UnboundedSender<DataRoot>,
}

impl MemoryChunkPool {
    pub(crate) fn new(config: Config, log_store: Store, sender: UnboundedSender<DataRoot>) -> Self {
        MemoryChunkPool {
            inner: Mutex::new(Inner::new(config)),
            log_store,
            sender,
        }
    }

    pub fn validate_segment_size(&self, segment: &Vec<u8>) -> Result<()> {
        if segment.is_empty() {
            bail!("data is empty");
        }

        if segment.len() % CHUNK_SIZE != 0 {
            bail!("invalid data length");
        }

        Ok(())
    }

    pub async fn cache_chunks(&self, seg_info: SegmentInfo) -> Result<()> {
        let root = seg_info.root;
        let file_completed = self
            .inner
            .lock()
            .await
            .segment_cache
            .cache_segment(seg_info)?;

        // store and finalize the cached file if completed
        if file_completed {
            self.write_all_cached_chunks_and_finalize(root).await?;
        }

        Ok(())
    }

    pub async fn write_chunks(
        &self,
        seg_info: SegmentInfo,
        tx_seq: u64,
        file_size: usize,
    ) -> Result<()> {
        let total_chunks = bytes_to_chunks(file_size);

        debug!(
            "Begin to write segment, root={}, segment_size={}, segment_index={}",
            seg_info.root,
            seg_info.seg_data.len(),
            seg_info.seg_index,
        );

        //Write the segment in window
        let (total_segments, _) = compute_segment_size(total_chunks, seg_info.chunks_per_segment);
        self.inner.lock().await.write_control.write_segment(
            seg_info.root,
            tx_seq,
            seg_info.seg_index,
            total_segments,
        )?;

        // Write memory cached segments into store.
        // TODO(qhz): error handling
        // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
        // 2. Put the incompleted segments back to memory pool.
        let seg = ChunkArray {
            data: seg_info.seg_data,
            start_index: (seg_info.seg_index * seg_info.chunks_per_segment) as u64,
        };

        if let Err(e) = self.log_store.put_chunks(tx_seq, seg).await {
            self.inner
                .lock()
                .await
                .write_control
                .on_write_failed(&seg_info.root, seg_info.seg_index);
            return Err(e);
        }

        let all_uploaded = self
            .inner
            .lock()
            .await
            .write_control
            .on_write_succeeded(&seg_info.root, seg_info.seg_index);

        // Notify to finalize transaction asynchronously.
        if all_uploaded {
            if let Err(e) = self.sender.send(seg_info.root) {
                // Channel receiver will not be dropped until program exit.
                bail!("channel send error: {}", e);
            }
            debug!("Queue to finalize transaction for file {}", seg_info.root);
        }

        Ok(())
    }

    /// Updates the cached file info when log entry retrieved from blockchain.
    pub async fn update_file_info(&self, tx: &Transaction) -> Result<bool> {
        let mut inner = self.inner.lock().await;

        // Do nothing if file not uploaded yet.
        let file = match inner.segment_cache.get_file_mut(&tx.data_merkle_root) {
            Some(f) => f,
            None => return Ok(false),
        };

        // Update the file info with transaction.
        file.update_with_tx(tx);

        // File partially uploaded and it's up to user thread
        // to write chunks into store and finalize transaction.
        if file.cached_chunk_num < file.total_chunks {
            return Ok(true);
        }

        // Otherwise, notify to write all memory cached chunks and finalize transaction.
        if let Err(e) = self.sender.send(tx.data_merkle_root) {
            // Channel receiver will not be dropped until program exit.
            bail!("channel send error: {}", e);
        }

        Ok(true)
    }

    pub(crate) async fn remove_cached_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        self.inner.lock().await.segment_cache.remove_file(root)
    }

    pub(crate) async fn remove_file(&self, root: &DataRoot) -> bool {
        let mut inner = self.inner.lock().await;
        inner.segment_cache.remove_file(root).is_some()
            || inner.write_control.remove_file(root).is_some()
    }

    pub async fn check_already_has_cache(&self, root: &DataRoot) -> bool {
        self.inner
            .lock()
            .await
            .segment_cache
            .get_file(root)
            .is_some()
    }

    async fn write_all_cached_chunks_and_finalize(&self, root: DataRoot) -> Result<()> {
        let (tx_seq, cache_chunk_num, mut segments) = self
            .inner
            .lock()
            .await
            .get_all_cached_segments_to_write(&root)?;

        while let Some(seg) = segments.pop() {
            // TODO(qhz): error handling
            // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
            // 2. Put the incompleted segments back to memory pool.
            if let Err(e) = self.log_store.put_chunks(tx_seq, seg).await {
                self.inner
                    .lock()
                    .await
                    .on_write_cache_failed(&root, cache_chunk_num);
                return Err(e);
            }
        }

        self.inner
            .lock()
            .await
            .on_write_cache_succeeded(&root, cache_chunk_num);

        if let Err(e) = self.sender.send(root) {
            // Channel receiver will not be dropped until program exit.
            bail!("channel send error: {}", e);
        }

        Ok(())
    }

    pub async fn get_tx_seq(&self, root: &DataRoot) -> u64 {
        let inner = self.inner.lock().await;

        if let Some(file) = inner.segment_cache.get_file(root) {
            file.tx_seq
        } else if let Some(file) = inner.write_control.get_file(root) {
            file.tx_seq
        } else {
            0
        }
    }

    pub async fn get_uploaded_seg_num(&self, root: &DataRoot) -> (usize, bool) {
        let inner = self.inner.lock().await;

        if let Some(file) = inner.segment_cache.get_file(root) {
            (file.cached_chunk_num, true)
        } else if let Some(file) = inner.write_control.get_file(root) {
            (file.uploaded_seg_num(), false)
        } else {
            (0, false)
        }
    }
}
