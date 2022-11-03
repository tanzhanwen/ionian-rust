#[cfg(test)]
pub mod tests {
    use file_location_cache::{test_util::AnnounceFileBuilder, FileLocationCache};
    use libp2p::PeerId;
    use rand::random;
    use shared_types::{ChunkArray, Transaction, TxID, CHUNK_SIZE};
    use std::{cmp, sync::Arc};
    use storage::{
        log_store::{
            log_manager::{
                sub_merkle_tree, tx_subtree_root_list_padded, LogConfig, PORA_CHUNK_SIZE,
            },
            LogStoreChunkWrite, LogStoreWrite, Store as LogStore,
        },
        LogManager,
    };
    use storage_async::Store;
    use task_executor::test_utils::TestRuntime;
    use tokio::sync::RwLock;

    pub struct TestStoreRuntime {
        pub runtime: TestRuntime,
        pub store: Store,
    }

    impl Default for TestStoreRuntime {
        fn default() -> Self {
            let runtime = TestRuntime::default();
            let store = Arc::new(RwLock::new(Self::new_store()));
            let executor = runtime.task_executor.clone();
            Self {
                runtime,
                store: Store::new(store, executor),
            }
        }
    }

    impl TestStoreRuntime {
        pub fn new_store() -> impl LogStore {
            LogManager::memorydb(LogConfig::default()).unwrap()
        }
    }

    pub fn create_2_store(
        chunk_count: Vec<usize>,
    ) -> (
        Arc<RwLock<LogManager>>,
        Arc<RwLock<LogManager>>,
        Vec<Transaction>,
        Vec<Vec<u8>>,
    ) {
        let config = LogConfig::default();
        let mut store = LogManager::memorydb(config.clone()).unwrap();
        let mut peer_store = LogManager::memorydb(config).unwrap();

        let mut offset = 1;
        let mut txs = vec![];
        let mut data = vec![];

        for tx_seq in 0..chunk_count.len() {
            let ret = generate_data(
                chunk_count[tx_seq],
                &mut store,
                &mut peer_store,
                tx_seq as u64,
                offset,
            );
            txs.push(ret.0);
            data.push(ret.1);
            offset = ret.2;
        }

        (
            Arc::new(RwLock::new(store)),
            Arc::new(RwLock::new(peer_store)),
            txs,
            data,
        )
    }

    fn generate_data(
        chunk_count: usize,
        store: &mut LogManager,
        peer_store: &mut LogManager,
        seq: u64,
        offset: u64,
    ) -> (Transaction, Vec<u8>, u64) {
        let data_size = CHUNK_SIZE * chunk_count;
        let mut data = vec![0u8; data_size];

        for i in 0..chunk_count {
            data[i * CHUNK_SIZE] = random();
        }

        let merkel_nodes = tx_subtree_root_list_padded(&data);
        let first_tree_size = 1 << (merkel_nodes[0].0 - 1);
        let start_offset = if offset % first_tree_size == 0 {
            offset
        } else {
            (offset / first_tree_size + 1) * first_tree_size
        };

        let merkle = sub_merkle_tree(&data).unwrap();
        let tx = Transaction {
            stream_ids: vec![],
            size: data_size as u64,
            data_merkle_root: merkle.root().into(),
            seq,
            data: vec![],
            start_entry_index: start_offset,
            merkle_nodes: merkel_nodes,
        };
        store.put_tx(tx.clone()).unwrap();
        peer_store.put_tx(tx.clone()).unwrap();
        for start_index in (0..chunk_count).step_by(PORA_CHUNK_SIZE) {
            let end = cmp::min((start_index + PORA_CHUNK_SIZE) * CHUNK_SIZE, data.len());
            let chunk_array = ChunkArray {
                data: data[start_index * CHUNK_SIZE..end].to_vec(),
                start_index: start_index as u64,
            };
            peer_store.put_chunks(tx.seq, chunk_array.clone()).unwrap();
        }
        peer_store.finalize_tx(tx.seq).unwrap();

        (tx, data, start_offset + chunk_count as u64)
    }

    pub fn create_file_location_cache(peer_id: PeerId, txs: Vec<TxID>) -> Arc<FileLocationCache> {
        let cache = FileLocationCache::default();

        for tx_id in txs {
            let announcement = AnnounceFileBuilder::default()
                .with_tx_id(tx_id)
                .with_peer_id(peer_id)
                .build();
            cache.insert(announcement);
        }

        Arc::new(cache)
    }
}
