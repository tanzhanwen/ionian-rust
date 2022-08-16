#!/usr/bin/env python3

from test_framework.test_framework import TestFramework
from utility.utils import create_proof_and_segment, generate_data_root, wait_until


class ExampleTest(TestFramework):
    def run_test(self):
        client = self.nodes[0]

        chunk_data = b"\x00" * 256
        data_root = generate_data_root(chunk_data)
        self.contract.append_log(data_root, 256)
        wait_until(lambda: self.contract.num_log_entries() == 1)
        wait_until(lambda: client.ionian_get_file_info(data_root) is not None)

        _, segment = create_proof_and_segment(chunk_data, data_root)
        self.log.info("segment: %s", segment)
        self.log.info("ionian_uploadSegment: %s", client.ionian_upload_segment(segment))
        wait_until(lambda: client.ionian_get_file_info(data_root)["finalized"])


if __name__ == "__main__":
    ExampleTest().main()
