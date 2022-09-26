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

struct FileWriteCtrl {
    tx_seq: u64,
    total_chunks: usize,
    window: CtrlWindow,
}

impl FileWriteCtrl {
    fn new(tx_seq: u64, total_chunks: usize, window_size: usize) -> Self {
        FileWriteCtrl {
            tx_seq,
            total_chunks,
            window: CtrlWindow::new(window_size),
        }
    }
}

pub struct ChunkPoolWriteCtrl {
    /// Total number of threads that are writing chunks into store.
    pub total_writings: usize,
    /// Windows to control writing processes of files
    files: HashMap<DataRoot, FileWriteCtrl>,
}

impl ChunkPoolWriteCtrl {
    pub fn new() -> Self {
        ChunkPoolWriteCtrl {
            total_writings: 0,
            files: HashMap::default(),
        }
    }

    /// Try to cache the segment into memory pool if log entry not retrieved from blockchain yet.
    /// Otherwise, return segments to write into store asynchronously for different files.
    pub fn write_segment(
        &mut self,
        root: DataRoot,
        tx_seq: u64,
        seg_index: usize,
        file_total_chunk_num: usize,
        write_window_size: usize,
        max_writings: usize,
    ) -> Result<()> {
        let file_ctrl = self
            .files
            .entry(root)
            .or_insert_with(|| FileWriteCtrl::new(tx_seq, file_total_chunk_num, write_window_size));

        if file_ctrl.total_chunks != file_total_chunk_num {
            bail!(anyhow!(
                "file size in segment doesn't match with file size declared in previous segment. Previous total chunk:{}, current total chunk:{}s",
                file_ctrl.total_chunks,
                file_total_chunk_num
            ));
        }

        let window = &mut file_ctrl.window;

        // Segment already uploaded.
        if window.check_duplicate(seg_index) {
            return Ok(());
        }

        // Limits the number of writing threads.
        if self.total_writings >= max_writings {
            bail!(anyhow!("too many data writing: {}", max_writings));
        }

        window.start_writing(seg_index)?;

        self.total_writings += 1;

        Ok(())
    }

    pub fn on_write_succeeded(
        &mut self,
        root: &DataRoot,
        seg_index: usize,
        chunks_per_segment: usize,
        file_total_chunk_num: usize,
    ) -> bool {
        let file_ctrl = match self.files.get_mut(root) {
            Some(w) => w,
            None => return false,
        };

        let window = &mut file_ctrl.window;

        window.finish_writing(seg_index);

        assert!(self.total_writings > 0);
        self.total_writings -= 1;

        debug!("Succeeded to write segment, root={}, seg_index={}, file_total_chunk_num={}, total_writings={}",
            root, seg_index, file_total_chunk_num, self.total_writings);

        // All chunks of file written into store.
        window.left_boundary * chunks_per_segment >= file_total_chunk_num
    }

    pub fn on_write_failed(&mut self, root: &DataRoot, seg_index: usize) {
        let file_ctrl = match self.files.get_mut(root) {
            Some(w) => w,
            None => return,
        };

        let window = &mut file_ctrl.window;

        //Rollback the segment status if failed
        window.rollback_writing(seg_index);

        assert!(self.total_writings > 0);
        self.total_writings -= 1;
    }

    pub fn get_tx_seq(&self, root: &DataRoot) -> u64 {
        let file_ctrl = self.files.get(root).unwrap();
        file_ctrl.tx_seq
    }
}

/// Used to cache chunks in memory pool and persist into db once log entry
/// retrieved from blockchain.
pub struct MemoryCachedFile {
    /// Window to control the cache of each file
    pub segments: Option<HashMap<usize, ChunkArray>>,
    /// Total number of chunks for the cache file, which is updated from log entry.
    pub total_chunks: usize,
    /// Transaction seq that used to write chunks into store.
    pub tx_seq: u64,
    /// Used for garbage collection.
    expired_at: Instant,
    /// Number of chunks that's currently cached for this file
    pub cached_chunk_num: usize,
}

impl MemoryCachedFile {
    fn new(timeout: Duration) -> Self {
        MemoryCachedFile {
            segments: Some(HashMap::default()),
            total_chunks: 0,
            tx_seq: 0,
            expired_at: Instant::now().add(timeout),
            cached_chunk_num: 0,
        }
    }

    /// Updates file with transaction once log entry retrieved from blockchain.
    /// So that, write memory cached segments into database.
    pub fn update_with_tx(&mut self, tx: &Transaction) {
        self.total_chunks = file_size_to_chunk_num(tx.size as usize);
        self.tx_seq = tx.seq;
    }
}

pub struct ChunkPoolCache {
    expiration_timeout: Duration,
    /// All cached files.
    files: LinkedHashMap<DataRoot, MemoryCachedFile>,
    /// Total number of chunks that cached in the memory pool.
    pub total_chunks: usize,
}

impl ChunkPoolCache {
    pub fn new(expiration_timeout: Duration) -> Self {
        ChunkPoolCache {
            expiration_timeout,
            files: LinkedHashMap::default(),
            total_chunks: 0,
        }
    }

    pub fn update_expiration_time(&mut self, root: &DataRoot) {
        if let Some(file) = self.files.to_back(root) {
            file.expired_at = Instant::now().add(self.expiration_timeout);
        }
    }

    pub fn garbage_collect(&mut self) {
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

    pub fn update_total_chunks_when_remove_file(&mut self, file: &MemoryCachedFile) {
        assert!(self.total_chunks >= file.cached_chunk_num);
        self.total_chunks -= file.cached_chunk_num;
    }

    pub fn cache_segment(
        &mut self,
        root: DataRoot,
        segment: Vec<u8>,
        seg_index: usize,
        chunks_per_segment: usize,
        max_cached_chunks_all: usize,
    ) -> Result<bool> {
        let file = self
            .files
            .entry(root)
            .or_insert_with(|| MemoryCachedFile::new(self.expiration_timeout));

        let mut file_complete = false;

        //Segment already cached in memory. Directly return OK
        let segments = file.segments.get_or_insert(HashMap::default());
        if segments.contains_key(&seg_index) {
            return Ok(file_complete);
        }

        // Otherwise, just cache segment in memory
        let num_chunks = segment.len() / CHUNK_SIZE;

        // Limits the cached chunks in the memory pool.
        if self.total_chunks + num_chunks > max_cached_chunks_all {
            bail!(anyhow!(
                "exceeds the maximum cached chunks of whole pool: {}",
                max_cached_chunks_all
            ));
        }

        // Cache segment and update the counter for cached chunks.
        self.total_chunks += num_chunks;
        file.cached_chunk_num += num_chunks;
        segments.insert(
            seg_index,
            ChunkArray {
                data: segment,
                start_index: (seg_index * chunks_per_segment) as u64,
            },
        );

        if file.total_chunks > 0 {
            file_complete = file.cached_chunk_num >= file.total_chunks;
        }

        Ok(file_complete)
    }

    pub fn has_cache(&self, root: &DataRoot) -> bool {
        self.files.contains_key(root)
    }

    pub fn get_file_mut(&mut self, root: &DataRoot) -> Option<&mut MemoryCachedFile> {
        self.files.get_mut(root)
    }

    pub fn remove_file(&mut self, root: &DataRoot) -> Option<MemoryCachedFile> {
        self.files.remove(root)
    }
}

struct Inner {
    config: Config,
    segment_cache: ChunkPoolCache,
    write_control: ChunkPoolWriteCtrl,
}

impl Inner {
    fn new(config: Config) -> Self {
        let expiration_timeout = Duration::from_secs(config.expiration_time_secs);
        Inner {
            config,
            segment_cache: ChunkPoolCache::new(expiration_timeout),
            write_control: ChunkPoolWriteCtrl::new(),
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
            bail!(anyhow!(
                "too many data writing: {}",
                self.config.max_writings
            ));
        }

        let mut file = self.segment_cache.remove_file(root).unwrap();
        let tx_seq = file.tx_seq;
        let segs = file.segments.take().unwrap();
        let segs = segs.into_iter().map(|(_k, v)| v).collect();

        self.write_control.total_writings += 1;

        Ok((tx_seq, file.cached_chunk_num, segs))
    }
}

pub struct SegmentMetaInfo {
    pub chunks_per_segment: usize,
    pub need_cache: bool,
    pub tx_seq: Option<u64>,
    pub file_size: usize,
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
            bail!(anyhow!("data is empty"));
        }

        if segment.len() % CHUNK_SIZE != 0 {
            bail!(anyhow!("invalid data length"))
        }

        Ok(())
    }

    /// Adds chunks into memory pool if log entry not retrieved from blockchain yet. Otherwise, write
    /// the segment into store directly.
    pub async fn add_chunks(
        &self,
        root: DataRoot,
        segment: Vec<u8>,
        seg_index: usize,
        info: SegmentMetaInfo,
    ) -> Result<()> {
        // Lazy GC when new chunks added.
        self.inner.lock().await.segment_cache.garbage_collect();

        if info.need_cache {
            let mut inner = self.inner.lock().await;
            let max_cached_chunks_all = inner.config.max_cached_chunks_all;
            let file_complete = inner.segment_cache.cache_segment(
                root,
                segment,
                seg_index,
                info.chunks_per_segment,
                max_cached_chunks_all,
            )?;
            inner.segment_cache.update_expiration_time(&root);
            drop(inner);

            if file_complete {
                //Trigger writing the cached file into store and finalize when all chunks of a file are cached
                self.write_all_cached_chunks_and_finalize(root).await?;
            }
        } else {
            self.write_chunks_inner(
                root,
                segment,
                seg_index,
                info.chunks_per_segment,
                info.tx_seq.unwrap(),
                info.file_size,
            )
            .await?;
        }

        Ok(())
    }

    async fn write_chunks_inner(
        &self,
        root: DataRoot,
        segment: Vec<u8>,
        seg_index: usize,
        chunks_per_segment: usize,
        tx_seq: u64,
        file_size: usize,
    ) -> Result<()> {
        let file_total_chunk_num = file_size_to_chunk_num(file_size);

        debug!(
            "Begin to write segment, root={}, segment_size={}, segment_index={}",
            root,
            segment.len(),
            seg_index,
        );

        //Write the segment in window
        let mut inner = self.inner.lock().await;
        let write_window_size = inner.config.write_window_size;
        let max_writings = inner.config.max_writings;
        inner.write_control.write_segment(
            root,
            tx_seq,
            seg_index,
            file_total_chunk_num,
            write_window_size,
            max_writings,
        )?;
        drop(inner);

        // Write memory cached segments into store.
        // TODO(qhz): error handling
        // 1. Push the failed segment back to front. (enhance store to return Err(ChunkArray))
        // 2. Put the incompleted segments back to memory pool.
        let seg = ChunkArray {
            data: segment,
            start_index: (seg_index * chunks_per_segment) as u64,
        };

        if let Err(e) = self.log_store.put_chunks(tx_seq, seg).await {
            self.inner
                .lock()
                .await
                .write_control
                .on_write_failed(&root, seg_index);
            return Err(e);
        }

        let all_uploaded = self.inner.lock().await.write_control.on_write_succeeded(
            &root,
            seg_index,
            chunks_per_segment,
            file_total_chunk_num,
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
            bail!(anyhow!("channel send error: {}", e));
        }

        Ok(true)
    }

    pub(crate) async fn remove_cached_file(&self, root: &DataRoot) -> Option<MemoryCachedFile> {
        let mut inner = self.inner.lock().await;

        let file = inner.segment_cache.remove_file(root)?;
        inner
            .segment_cache
            .update_total_chunks_when_remove_file(&file);

        Some(file)
    }

    pub async fn check_already_has_cache(&self, root: &DataRoot) -> bool {
        self.inner.lock().await.segment_cache.has_cache(root)
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
            bail!(anyhow!("channel send error: {}", e));
        }

        Ok(())
    }

    pub async fn get_tx_seq(&self, root: &DataRoot) -> u64 {
        let mut inner = self.inner.lock().await;
        let tx_seq = if inner.segment_cache.has_cache(root) {
            inner.segment_cache.get_file_mut(root).unwrap().tx_seq
        } else {
            inner.write_control.get_tx_seq(root)
        };

        tx_seq
    }
}

pub fn file_size_to_chunk_num(file_size: usize) -> usize {
    let mut chunk_num = file_size as usize / CHUNK_SIZE;
    if file_size as usize % CHUNK_SIZE > 0 {
        chunk_num += 1;
    }

    chunk_num
}
