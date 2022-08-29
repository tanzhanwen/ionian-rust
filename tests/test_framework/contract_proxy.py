from config.node_config import TX_PARAMS
from utility.utils import assert_equal


class ContractProxy:
    def __init__(self, contract, blockchain_nodes):
        self.contract = contract
        self.contract_address = contract.address
        self.blockchain_nodes = blockchain_nodes

    def _get_contract(self, node_idx=0):
        return (
            self.contract
            if node_idx == 0
            else self.blockchain_nodes[node_idx].get_contract(self.contract_address)
        )

    def address(self):
        return self.contract_address

    def submit(self, submission_nodes, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        tx_hash = contract.functions.submit(submission_nodes).transact(TX_PARAMS)
        receipt = contract.web3.eth.wait_for_transaction_receipt(tx_hash)
        assert_equal(receipt["status"], 1)

    def num_submissions(self, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return contract.functions.numSubmissions().call()
