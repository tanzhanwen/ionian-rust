use crate::Config;
use anyhow::{anyhow, bail, Result};
use async_lock::Mutex;
use hashlink::LinkedHashMap;
use shared_types::{ChunkArray, DataRoot, Transaction, CHUNK_SIZE};
use std::collections::{HashMap, VecDeque};
use std::ops::Add;
use std::time::{Duration, Instant};
use storage_async::Store;
use tokio::sync::mpsc::UnboundedSender;

// TODO(qhz): Suppose that file uploaded in sequence and following scenarios are to be resolved:
// 1) Uploaded not in sequence: costly to determine if all chunks uploaded, so as to finalize tx in store.
// 2) Upload concurrently: by one user or different users.

/// The segment status in sliding window
#[derive(PartialEq, Eq, Debug)]
enum SlotStatus {
    Writing,
    Finished,
}

/// Sliding window is used to control the concurrent uploading process of a file.
/// Bounded window allows segments to be uploaded concurrenly, while having a capacity limit on writing threads per file
/// Meanwhile, the left_boundary field records how many segments have been uploaded
pub struct CtrlWindow {
    size: usize,
    left_boundary: usize,
    slots: HashMap<usize, SlotStatus>,
}

impl CtrlWindow {
    fn new(size: usize) -> Self {
        CtrlWindow {
            size,
            left_boundary: 0,
            slots: HashMap::default(),
        }
    }

    fn check_duplicate(&self, index: usize) -> bool {
        if index < self.left_boundary {
            return true;
        }

        if self.slots.contains_key(&index) {
            return true;
        }

        false
    }

    //Should call check_duplicate and handle the duplicated case before calling this function
    //This function assumes that there are no duplicate slots in the window
    fn start_writing(&mut self, index: usize) -> Result<()> {
        assert!(index >= self.left_boundary);

        if index >= self.left_boundary + self.size {
            bail!(
                "index exceeds window limit, index = {}, left_boundary = {}, window_size = {}",
                index,
                self.left_boundary,
                self.size
            );
        }

        assert!(!self.slots.contains_key(&index));
        self.slots.insert(index, SlotStatus::Writing);

        Ok(())
    }

    fn rollback_writing(&mut self, index: usize) {
        let slot_status = self.slots.remove(&index).unwrap();
        assert_eq!(slot_status, SlotStatus::Writing);
    }

    fn finish_writing(&mut self, index: usize) {
        let slot_status = self.slots.get_mut(&index).unwrap();
        assert_eq!(slot_status, &SlotStatus::Writing);
        *slot_status = SlotStatus::Finished;

        //Check whether left boundary should move forward
        let mut left_boundary = self.left_boundary;
        while let Some(&SlotStatus::Finished) = self.slots.get(&left_boundary) {
            self.slots.remove(&left_boundary);
            left_boundary += 1;
        }

        self.left_boundary = left_boundary;
    }
}

/// Used to cache chunks in memory pool and persist into db once log entry
/// retrieved from blockchain.
pub struct MemoryCachedFile {
    pub window: CtrlWindow,
    /// Memory cached segments before log entry retrieved from blockchain.
    pub segments: Option<VecDeque<ChunkArray>>,
    /// Total number of chunks for the cache file, which is updated from log entry.
    total_chunks: usize,
    /// Transaction seq that used to write chunks into store.
    pub tx_seq: u64,
    /// Used for garbage collection.
    expired_at: Instant,
}

impl MemoryCachedFile {
    fn new(timeout: Duration, window_size: usize) -> Self {
        MemoryCachedFile {
            window: CtrlWindow::new(window_size),
            segments: None,
            total_chunks: 0,
            tx_seq: 0,
            expired_at: Instant::now().add(timeout),
        }
    }
}

impl MemoryCachedFile {
    /// Updates file with transaction once log entry retrieved from blockchain.
    /// So that, write memory cached segments into database.
    fn update_with_tx(&mut self, tx: &Transaction) {
        self.total_chunks = tx.size as usize / CHUNK_SIZE;
        if tx.size as usize % CHUNK_SIZE > 0 {
            self.total_chunks += 1;
        }

        self.tx_seq = tx.seq;
    }
}

struct Inner {
    config: Config,
    expiration_timeout: Duration,
    /// All cached files.
    files: LinkedHashMap<DataRoot, MemoryCachedFile>,
    /// Total number of chunks that cached in the memory pool.
    total_chunks: usize,
    /// Total number of threads that are writing chunks into store.
    total_writings: usize,
}

impl Inner {
    fn new(config: Config) -> Self {
        let expiration_timeout = Duration::from_secs(config.expiration_time_secs);
        Inner {
            config,
            expiration_timeout,
            files: Default::default(),
            total_chunks: 0,
            total_writings: 0,
        }
    }

    fn update_expiration_time(&mut self, root: &DataRoot) {
        if let Some(file) = self.files.to_back(root) {
            file.expired_at = Instant::now().add(self.expiration_timeout);
        }
    }

    fn garbage_collect(&mut self) {
        while let Some((_, file)) = self.files.front() {
            if file.expired_at > Instant::now() {
                return;
            }

            if let Some((r, f)) = self.files.pop_front() {
                self.update_total_chunks_when_remove_file(&f);
                debug!("Garbage collected for file {}", r);
            }
        }
    }

    fn update_total_chunks_when_remove_file(&mut self, file: &MemoryCachedFile) {
        if let Some(ref segments) = file.segments {
            for seg in segments.iter() {
                let seg_chunks = seg.data.len() / CHUNK_SIZE;
                assert!(self.total_chunks >= seg_chunks);
                self.total_chunks -= seg_chunks;
            }
        }
    }

    /// Try to cache the segment into memory pool if log entry not retrieved from blockchain yet.
    /// Otherwise, return segments to write into store asynchronously for different files.
    fn cache_or_write_segment(
        &mut self,
        root: DataRoot,
        segment: Vec<u8>,
        seg_index: usize,
        chunks_per_segment: usize,
        maybe_tx: Option<Transaction>,
    ) -> Result<Option<(u64, VecDeque<ChunkArray>)>> {
        let file = self.files.entry(root).or_insert_with(|| {
            MemoryCachedFile::new(self.expiration_timeout, self.config.window_size)
        });

        // Update transaction in case that log entry already retrieved from blockchain
        // before any segment uploaded to storage node. In this case, segment will not
        // be cached in memory pool.
        if file.total_chunks == 0 {
            if let Some(tx) = maybe_tx {
                file.update_with_tx(&tx);
            }
        }

        // Prepare segments to write into store when log entry already retrieved.
        if file.total_chunks > 0 {
            //Check whether the segment is within the file_size limit
            if (seg_index + 1) * chunks_per_segment > file.total_chunks {
                bail!(anyhow!(
                    "seg index exceeds file size limit. seg_index: {}, chunks_per_segment:{}, file total chunks:{}",
                    seg_index,
                    chunks_per_segment,
                    file.total_chunks
                ));
            }

            // Segment already uploaded.
            if file.window.check_duplicate(seg_index) {
                return Ok(None);
            }

            // Limits the number of writing threads.
            if self.total_writings >= self.config.max_writings {
                bail!(anyhow!(
                    "too many data writing: {}",
                    self.config.max_writings
                ));
            }

            file.window.start_writing(seg_index)?;

            // Note, do not update the counter of cached chunks in this case.
            self.total_writings += 1;
            file.segments
                .get_or_insert_with(Default::default)
                .push_back(ChunkArray {
                    data: segment,
                    start_index: (seg_index * chunks_per_segment) as u64,
                });
            return Ok(Some((file.tx_seq, file.segments.take().unwrap())));
        }

        // Otherwise, just cache segment in memory
        let num_chunks = segment.len() / CHUNK_SIZE;

        // Limits the cached chunks in the memory pool.
        if self.total_chunks + num_chunks > self.config.max_cached_chunks_all {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of whole pool: {}",
                self.config.max_cached_chunks_all
            ));
        }

        // Cache segment and update the counter for cached chunks.
        self.total_chunks += num_chunks;
        file.segments
            .get_or_insert_with(Default::default)
            .push_back(ChunkArray {
                data: segment,
                start_index: (seg_index * chunks_per_segment) as u64,
            });

        Ok(None)
    }

    fn on_write_succeeded(
        &mut self,
        root: &DataRoot,
        cached_segs_chunks: usize,
        seg_index: usize,
        chunks_per_segment: usize,
    ) -> bool {
        let file = match self.files.get_mut(root) {
            Some(f) => f,
            None => return false,
        };

        file.window.finish_writing(seg_index);

        assert!(self.total_chunks >= cached_segs_chunks);
        self.total_chunks -= cached_segs_chunks;
        assert!(self.total_writings > 0);
        self.total_writings -= 1;

        debug!("Succeeded to write segment, root={}, seg_index={}({}), pool_total_chunks={}, total_writings={}",
            root, seg_index, file.total_chunks, self.total_chunks, self.total_writings);

        // All chunks of file written into store.
        file.total_chunks > 0 && file.window.left_boundary * chunks_per_segment >= file.total_chunks
    }

    fn on_write_failed(&mut self, root: &DataRoot, cached_segs_chunks: usize, seg_index: usize) {
        let file = match self.files.get_mut(root) {
            Some(f) => f,
            None => return,
        };

        //Rollback the segment status if failed
        file.window.rollback_writing(seg_index);

        assert!(self.total_chunks >= cached_segs_chunks);
        self.total_chunks -= cached_segs_chunks;
        assert!(self.total_writings > 0);
        self.total_writings -= 1;
    }

    /// If log entry retrieved timely, we should update the transaction for the first 2 segments.
    fn requires_to_update_tx_before_cache(&self, root: &DataRoot, seg_index: usize) -> bool {
        if seg_index == 0 {
            return true;
        }

        // Must be updated by `update_file_info` by log entry poller.
        if seg_index > 1 {
            return false;
        }

        let file = match self.files.get(root) {
            Some(f) => f,
            None => return false,
        };

        // Already updated
        if file.total_chunks > 0 {
            return false;
        }

        // Only required for the 2nd segment.
        match &file.segments {
            Some(segs) => segs.len() == 1,
            None => false,
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

    fn validate_segment_size(&self, segment: &Vec<u8>) -> Result<usize> {
        if segment.is_empty() {
            bail!(anyhow!("data is empty"));
        }

        if segment.len() % CHUNK_SIZE != 0 {
            bail!(anyhow!("invalid data length"))
        }

        let num_chunks = segment.len() / CHUNK_SIZE;

        Ok(num_chunks)
    }

    async fn get_tx_by_root(&self, root: &DataRoot) -> Result<Option<Transaction>> {
        match self.log_store.get_tx_seq_by_data_root(root).await? {
            Some(tx_seq) => self.log_store.get_tx_by_seq_number(tx_seq).await,
            None => Ok(None),
        }
    }

    /// Adds chunks into memory pool if log entry not retrieved from blockchain yet. Otherwise, write
    /// the segment into store directly.
    pub async fn add_chunks(
        &self,
        root: DataRoot,
        segment: Vec<u8>,
        seg_index: usize,
        chunks_per_segment: usize,
    ) -> Result<()> {
        // Lazy GC when new chunks added.
        self.inner.lock().await.garbage_collect();

        self.add_chunks_inner(root, segment, seg_index, chunks_per_segment)
            .await?;

        // Update expiration time when succeeded.
        self.inner.lock().await.update_expiration_time(&root);

        Ok(())
    }

    async fn add_chunks_inner(
        &self,
        root: DataRoot,
        segment: Vec<u8>,
        seg_index: usize,
        chunks_per_segment: usize,
    ) -> Result<()> {
        let num_chunks = self.validate_segment_size(&segment)?;

        // Try to update file with transaction for the first 2 segments,
        // in case that log entry already retrieved from blockchain.
        //
        // Note, the log entry may be retrieved immediately before the mutex acquired to `cache_or_write_segment`.
        // So, the `maybe_tx` below may be `None` for the 1st segment. When the 2nd segment arrived, try again to
        // update the log entry info from db.
        let mut maybe_tx = None;
        if self
            .inner
            .lock()
            .await
            .requires_to_update_tx_before_cache(&root, seg_index)
        {
            maybe_tx = self.get_tx_by_root(&root).await?;
        }

        debug!(
            "Begin to cache or write segment, root={}, segment_size={}, segment_index={}, tx={:?}",
            root,
            segment.len(),
            seg_index,
            maybe_tx
        );

        // Cache segment in memory if log entry not retrieved yet, or write into store directly.
        let (tx_seq, mut segments) = match self.inner.lock().await.cache_or_write_segment(
            root,
            segment,
            seg_index,
            chunks_per_segment,
            maybe_tx,
        )? {
            Some(tuple) => tuple,
            None => return Ok(()),
        };

        let mut total_chunks_to_write = 0;
        for seg in segments.iter() {
            total_chunks_to_write += seg.data.len() / CHUNK_SIZE;
        }
        let pending_seg_chunks = total_chunks_to_write - num_chunks;

        // Write memory cached segments into store.
        while let Some(seg) = segments.pop_front() {
            // TODO(qhz): error handling
            // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
            // 2. Put the incompleted segments back to memory pool.
            if let Err(e) = self.log_store.put_chunks(tx_seq, seg).await {
                self.inner
                    .lock()
                    .await
                    .on_write_failed(&root, pending_seg_chunks, seg_index);
                return Err(e);
            }
        }

        let all_uploaded = self.inner.lock().await.on_write_succeeded(
            &root,
            pending_seg_chunks,
            seg_index,
            chunks_per_segment,
        );

        // Notify to finalize transaction asynchronously.
        if all_uploaded {
            if let Err(e) = self.sender.send(root) {
                // Channel receiver will not be dropped until program exit.
                bail!(anyhow!("channel send error: {}", e));
            }
            debug!("Queue to finalize transaction for file {}", root);
        }

        Ok(())
    }

    /// Updates the cached file info when log entry retrieved from blockchain.
    pub async fn update_file_info(&self, tx: &Transaction) -> Result<bool> {
        let mut inner = self.inner.lock().await;

        // Do nothing if file not uploaded yet.
        let file = match inner.files.get_mut(&tx.data_merkle_root) {
            Some(f) => f,
            None => return Ok(false),
        };

        // Update the file info with transaction.
        file.update_with_tx(tx);

        // TODO(zhanwen): Add the following logic back in the pr of caching samll file
        // File partially uploaded and it's up to user thread
        // to write chunks into store and finalize transaction.
        //if file.next_index < file.total_chunks {
        //    return Ok(true);
        //}

        // Otherwise, notify to write all memory cached chunks and finalize transaction.
        if let Err(e) = self.sender.send(tx.data_merkle_root) {
            // Channel receiver will not be dropped until program exit.
            bail!(anyhow!("channel send error: {}", e));
        }

        Ok(true)
    }

    pub(crate) async fn remove_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        let mut inner = self.inner.lock().await;

        let file = inner.files.remove(root)?;
        inner.update_total_chunks_when_remove_file(&file);

        Some(file)
    }
}
