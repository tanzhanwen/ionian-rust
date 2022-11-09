#!/usr/bin/env python3
from test_framework.blockchain_node import BlockChainNodeType
from test_framework.test_framework import TestFramework
from config.node_config import MINER_ID, GENESIS_PRIV_KEY
from test_framework.blockchain_node import BlockChainNodeType
from utility.submission import create_submission, submit_data
from utility.utils import wait_until


class MineTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1
        self.ionian_node_configs[0] = {
            "miner_id": MINER_ID,
            "miner_key": GENESIS_PRIV_KEY,
        }

    def submit_data(self, item, size):
        submissions_before = self.contract.num_submissions()
        client = self.nodes[0]
        chunk_data = item * 256 * size
        submissions, data_root = create_submission(chunk_data)
        self.contract.submit(submissions)
        wait_until(lambda: self.contract.num_submissions() == submissions_before + 1)
        wait_until(lambda: client.ionian_get_file_info(data_root) is not None)

        segment = submit_data(client, chunk_data)
        wait_until(lambda: client.ionian_get_file_info(data_root)["finalized"])

    def run_test(self):
        blockchain = self.blockchain_nodes[0]

        self.log.info("flow address: %s", self.contract.address())
        self.log.info("mine address: %s", self.mine_contract.address())

        quality = int(2**256 / 40960)
        self.mine_contract.set_quality(quality)

        self.log.info("Submit the first data chunk")
        self.submit_data(b"\x11", 2000)

        self.log.info("Wait for the first mine context release")
        wait_until(lambda: int(blockchain.eth_blockNumber(), 16) > 100, timeout=180)

        self.log.info("Wait for the first mine answer")
        wait_until(lambda: self.mine_contract.last_mined_epoch() == 1)

        self.log.info("Wait for the second mine context release")
        wait_until(lambda: int(blockchain.eth_blockNumber(), 16) > 200, timeout=180)

        self.log.info("Wait for the second mine answer")
        wait_until(lambda: self.mine_contract.last_mined_epoch() == 2)

        self.nodes[0].miner_stop()
        self.log.info("Wait for the third mine context release")
        wait_until(lambda: int(blockchain.eth_blockNumber(), 16) > 307, timeout=180)
        self.log.info("Submit the second data chunk")
        self.submit_data(b"\x22", 2000)
        # Now the storage node should have the latest flow, but the mining context is using an old one.
        self.nodes[0].miner_start()

        self.log.info("Wait for the third mine answer")
        wait_until(lambda: self.mine_contract.last_mined_epoch() == 3)


if __name__ == "__main__":
    MineTest(blockchain_node_type=BlockChainNodeType.BSC).main()
