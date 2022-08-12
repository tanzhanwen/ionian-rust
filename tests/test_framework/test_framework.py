import argparse
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

from eth_utils import encode_hex
from test_framework.bsc_node import BSCNode
from test_framework.contract_proxy import ContractProxy
from test_framework.ionian_node import IonianNode
from test_framework.blockchain_node import BlockChainNodeType

from utility.utils import PortMin, is_windows_platform, wait_until

__file_path__ = os.path.dirname(os.path.realpath(__file__))


class TestFramework:
    def __init__(self, blockchain_node_type=BlockChainNodeType.BSC):
        self.num_blockchain_nodes = None
        self.num_nodes = None
        self.blockchain_nodes = []
        self.nodes = []
        self.contract = None
        self.blockchain_node_configs = {}
        self.ionian_node_configs = {}
        self.blockchain_node_type = blockchain_node_type

    def __setup_blockchain_node(self):
        for i in range(self.num_blockchain_nodes):
            if i in self.blockchain_node_configs:
                updated_config = self.blockchain_node_configs[i]
            else:
                updated_config = {}

            node = (
                BSCNode(
                    i,
                    self.root_dir,
                    self.blockchain_binary,
                    updated_config,
                    self.contract_path,
                    self.log,
                    30,
                )
                if self.blockchain_node_type == BlockChainNodeType.BSC
                else None
            )
            self.blockchain_nodes.append(node)
            node.setup_config()
            node.start()

        # wait node to start to avoid NewConnectionError
        time.sleep(1)
        for node in self.blockchain_nodes:
            node.wait_for_rpc_connection()

        if self.blockchain_node_type == BlockChainNodeType.BSC:
            enode = self.blockchain_nodes[0].admin_nodeInfo()["enode"]
            for node in self.blockchain_nodes[1:]:
                node.admin_addPeer([enode])

            # mine
            self.blockchain_nodes[0].miner_start([1])

            for node in self.blockchain_nodes:
                node.wait_for_start_mining()

        contract = self.blockchain_nodes[0].setup_contract()
        self.contract = ContractProxy(contract, self.blockchain_nodes)

    def __setup_ionian_node(self):
        for i in range(self.num_nodes):
            if i in self.ionian_node_configs:
                updated_config = self.ionian_node_configs[i]
            else:
                updated_config = {}

            node = IonianNode(
                i,
                self.root_dir,
                self.ionian_binary,
                updated_config,
                self.contract.address(),
                self.log,
            )
            self.nodes.append(node)
            node.setup_config()
            node.start()

        time.sleep(1)
        for node in self.nodes:
            node.wait_for_rpc_connection()

        for node in self.nodes:
            wait_until(lambda: node.ionian_get_status() == self.num_nodes - 1)

    def __parse_arguments(self):
        parser = argparse.ArgumentParser(usage="%(prog)s [options]")

        parser.add_argument(
            "--conflux-binary",
            dest="conflux",
            default=os.path.join(
                __file_path__,
                "../utility/conflux" + (".exe" if is_windows_platform() else ""),
            ),
            type=str,
        )

        parser.add_argument(
            "--bsc-binary",
            dest="bsc",
            default=os.path.join(
                __file_path__,
                "../utility/geth" + (".exe" if is_windows_platform() else ""),
            ),
            type=str,
        )

        parser.add_argument(
            "--ionian-binary",
            dest="ionian",
            default=os.path.join(
                __file_path__,
                "../../target/release/ionian_node"
                + (".exe" if is_windows_platform() else ""),
            ),
            type=str,
        )

        parser.add_argument(
            "--ionian-client",
            dest="cli",
            default=os.path.join(
                __file_path__,
                "../../ionian-client/ionian-client"
                + (".exe" if is_windows_platform() else ""),
            ),
            type=str,
        )

        parser.add_argument(
            "--contract-path",
            dest="contract",
            default=os.path.join(
                __file_path__,
                "../../node/log_entry_sync/src/contracts/IonianLog.json",
            ),
            type=str,
        )

        parser.add_argument(
            "-l",
            "--loglevel",
            dest="loglevel",
            default="INFO",
            help="log events at this level and higher to the console. Can be set to DEBUG, INFO, WARNING, ERROR or CRITICAL. Passing --loglevel DEBUG will output all logs to console. Note that logs at all levels are always written to the test_framework.log file in the temporary test directory.",
        )

        parser.add_argument(
            "--tmpdir", dest="tmpdir", help="Root directory for datadirs"
        )

        parser.add_argument(
            "--randomseed", dest="random_seed", type=int, help="Set a random seed"
        )

        parser.add_argument("--port-min", dest="port_min", default=11000, type=int)

        self.options = parser.parse_args()

    def __start_logging(self):
        # Add logger and logging handlers
        self.log = logging.getLogger("TestFramework")
        self.log.setLevel(logging.DEBUG)

        # Create file handler to log all messages
        fh = logging.FileHandler(
            self.options.tmpdir + "/test_framework.log", encoding="utf-8"
        )
        fh.setLevel(logging.DEBUG)

        # Create console handler to log messages to stderr. By default this logs only error messages, but can be configured with --loglevel.
        ch = logging.StreamHandler(sys.stdout)
        # User can provide log level as a number or string (eg DEBUG). loglevel was caught as a string, so try to convert it to an int
        ll = (
            int(self.options.loglevel)
            if self.options.loglevel.isdigit()
            else self.options.loglevel.upper()
        )
        ch.setLevel(ll)

        # Format logs the same as bitcoind's debug.log with microprecision (so log files can be concatenated and sorted)
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d000Z %(name)s (%(levelname)s): %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        formatter.converter = time.gmtime
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        self.log.addHandler(fh)
        self.log.addHandler(ch)

    def _upload_file_use_cli(
        self,
        blockchain_node_rpc_url,
        contract_address,
        key,
        ionion_node_rpc_url,
        file_to_upload,
    ):
        assert os.path.exists(self.cli_binary)
        upload_args = [
            self.cli_binary,
            "upload",
            "--url",
            blockchain_node_rpc_url,
            "--contract",
            contract_address,
            "--key",
            encode_hex(key),
            "--node",
            ionion_node_rpc_url,
            "--log-level",
            "debug",
            "--file",
        ]

        proc = subprocess.Popen(
            upload_args + [file_to_upload.name],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        root = None
        for line in proc.stdout.readlines():
            self.log.debug("line: %s", line)
            if "root" in line:
                index = line.find("root=")
                root = line[index + 5 : -1]
                self.log.info("root: %s", root)

        proc.wait()
        assert proc.returncode == 0
        self.log.info("file uploaded")

        return root

    def setup_params(self):
        self.num_blockchain_nodes = 1
        self.num_nodes = 1

    def setup_nodes(self):
        self.__setup_blockchain_node()
        self.__setup_ionian_node()

    def stop_nodes(self):
        # stop ionion nodes first
        for node in self.nodes:
            node.stop()

        for node in self.blockchain_nodes:
            node.stop()

    def run_test(self):
        raise NotImplementedError

    def main(self):
        self.__parse_arguments()
        PortMin.n = self.options.port_min

        # Set up temp directory and start logging
        if self.options.tmpdir:
            self.options.tmpdir = os.path.abspath(self.options.tmpdir)
            os.makedirs(self.options.tmpdir, exist_ok=True)
        else:
            self.options.tmpdir = os.getenv(
                "IONIAN_TESTS_LOG_DIR", default=tempfile.mkdtemp(prefix="ionian_test_")
            )

        self.root_dir = self.options.tmpdir

        self.__start_logging()
        self.log.info("Root dir: %s", self.root_dir)

        if self.blockchain_node_type == BlockChainNodeType.Conflux:
            self.blockchain_binary = self.options.conflux
        else:
            self.blockchain_binary = self.options.bsc

        self.ionian_binary = self.options.ionian
        self.cli_binary = self.options.cli
        self.contract_path = self.options.contract

        assert os.path.exists(self.contract_path)

        if self.options.random_seed is not None:
            random.seed(self.options.random_seed)

        success = False
        try:
            self.setup_params()
            self.setup_nodes()
            self.run_test()
            success = True
        except Exception as e:
            self.log.error("Test exception %s %s", repr(e), traceback.format_exc())
            self.log.error(f"Test data are not deleted: {self.root_dir}")

        self.stop_nodes()

        if success:
            self.log.info("Test success")
            shutil.rmtree(self.root_dir)

        handlers = self.log.handlers[:]
        for handler in handlers:
            self.log.removeHandler(handler)
            handler.close()
        logging.shutdown()
