#!/usr/bin/env python3

import base64
import random
from test_framework.test_framework import TestFramework
from utility.submission import ENTRY_SIZE, submit_data
from utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)


class SubmissionTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1

    def run_test(self):
        data_size = [
            2,
            256,
            256 * 1023 + 1,
            256 * 1024 * 256,
        ]
        same_root_tx_count = 3

        submission_index = 1
        for i, v in enumerate(data_size):
            chunk_data = random.randbytes(v)
            for _ in range(same_root_tx_count):
                self.submit_tx_for_data(chunk_data, submission_index)
                submission_index += 1
            self.submit_data(chunk_data)
            # TODO: Check tx status with tx seq.

    def submit_tx_for_data(self, chunk_data, submission_index, node_idx=0):
        submissions, data_root = create_submission(chunk_data)
        self.log.info("data root: %s, submissions: %s", data_root, submissions)
        self.contract.submit(submissions)

        wait_until(lambda: self.contract.num_submissions() == submission_index)

        client = self.nodes[node_idx]
        wait_until(lambda: client.ionian_get_file_info(data_root) is not None)
        assert_equal(client.ionian_get_file_info(data_root)["finalized"], False)

    def submit_data(self, chunk_data, node_idx=0):
        _, data_root = create_submission(chunk_data)
        client = self.nodes[node_idx]
        segments = submit_data(client, chunk_data)
        self.log.debug(
            "segments: %s", [(s["root"], s["index"], s["proof"]) for s in segments]
        )
        wait_until(lambda: client.ionian_get_file_info(data_root)["finalized"])


if __name__ == "__main__":
    SubmissionTest().main()
