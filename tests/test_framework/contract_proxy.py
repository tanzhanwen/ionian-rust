from config.node_config import TX_PARAMS


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

    def append_log(self, data_root, size, stream_ids=None, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        if stream_ids:
            contract.functions.appendLog(data_root, size, stream_ids).transact(
                TX_PARAMS
            )
        else:
            contract.functions.appendLog(data_root, size).transact(TX_PARAMS)

    def append_log_with_data(self, data, stream_ids=None, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        if stream_ids:
            contract.functions.appendLogWithData(data, stream_ids).transact(TX_PARAMS)
        else:
            contract.functions.appendLogWithData(data).transact(TX_PARAMS)

    def create_stream(self, access_control, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        contract.functions.createStream(access_control).transact(TX_PARAMS)

    def num_log_entries(self, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return contract.functions.numLogEntries().call()

    def num_streams(self, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return contract.functions.numStreams().call()

    def get_log_entries(self, offset, limit, node_idx=0):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return contract.functions.getLogEntries(offset, limit).call()
