#[cfg(test)]
pub mod tests {
    use std::{cmp, sync::Arc};

    use file_location_cache::FileLocationCache;
    use libp2p::{identity, Multiaddr, PeerId};
    use network::types::AnnounceFile;
    use rand::random;
    use shared_types::{timestamp_now, ChunkArray, Transaction, CHUNK_SIZE};
    use storage::{
        log_store::{
            log_manager::{
                sub_merkle_tree, tx_subtree_root_list_padded, LogConfig, PORA_CHUNK_SIZE,
            },
            LogStoreChunkWrite, LogStoreWrite,
        },
        LogManager,
    };
    use tokio::sync::RwLock;

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

    pub fn create_file_location_cache(peer_id: PeerId, seq_size: usize) -> Arc<FileLocationCache> {
        let file_location_cache: Arc<FileLocationCache> = Default::default();
        generate_announce_file(peer_id, file_location_cache.clone(), seq_size);

        file_location_cache
    }

    fn generate_announce_file(
        peer_id: PeerId,
        file_location_cache: Arc<FileLocationCache>,
        seq_size: usize,
    ) {
        for i in 0..seq_size {
            let address: Multiaddr = "/ip4/127.0.0.1/tcp/10000".parse().unwrap();
            let msg = AnnounceFile {
                tx_seq: i as u64,
                peer_id: peer_id.into(),
                at: address.into(),
                timestamp: timestamp_now(),
            };

            let local_private_key = identity::Keypair::generate_secp256k1();
            let signed_msg = msg
                .into_signed(&local_private_key)
                .expect("Sign msg failed");
            file_location_cache.insert(signed_msg);
        }
    }
}
