#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.utils import create_proof_and_segment, generate_data_root, wait_until


class RecoveryTest(TestFramework):
    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x00" * 256
        data_root = generate_data_root(chunk_data)
        nodes = [[data_root, 0]]
        self.contract.submit([256, nodes])
        wait_until(lambda: self.contract.num_submissions() == 1)
        wait_until(lambda: client.ionian_get_file_info(data_root) is not None)

        _, segment = create_proof_and_segment(chunk_data, data_root)
        self.log.info("segment: %s", segment)
        self.log.info("ionian_uploadSegment: %s", client.ionian_upload_segment(segment))
        wait_until(lambda: client.ionian_get_file_info(data_root)["finalized"])

        self.stop_storage_node(0)
        chunk_data = b"\x01" * 256
        data_root = generate_data_root(chunk_data)
        nodes = [[data_root, 1]]
        self.contract.submit([256, nodes])
        wait_until(lambda: self.contract.num_submissions() == 2)
        self.start_storage_node(0)
        self.nodes[0].wait_for_rpc_connection()
        wait_until(lambda: client.ionian_get_file_info(data_root) is not None)
        _, segment = create_proof_and_segment(chunk_data, data_root)
        self.log.info("segment: %s", segment)
        self.log.info("ionian_uploadSegment: %s", client.ionian_upload_segment(segment))
        wait_until(lambda: client.ionian_get_file_info(data_root)["finalized"])




if __name__ == "__main__":
    RecoveryTest().main()
