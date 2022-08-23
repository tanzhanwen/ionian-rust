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

    def append_log(self, submission_nodes, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        tx_hash = contract.functions.submit(submission_nodes).transact(TX_PARAMS)
        receipt = contract.web3.eth.wait_for_transaction_receipt(tx_hash)
        assert_equal(receipt["status"], 1)


    def append_log_with_data(self, data, stream_ids=None, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        if stream_ids:
            contract.functions.appendLogWithData(data, stream_ids).transact(TX_PARAMS)
        else:
            contract.functions.appendLogWithData(data).transact(TX_PARAMS)

    def num_log_entries(self, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return contract.functions.numSubmissions().call()

    def get_log_entries(self, offset, limit, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return contract.functions.getLogEntries(offset, limit).call()
