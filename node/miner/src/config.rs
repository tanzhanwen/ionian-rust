use ethereum_types::{Address, H256};
use ethers::providers::Http;
use ethers::providers::Provider;

pub struct MinerConfig {
    pub(crate) miner_id: H256,
    pub(crate) rpc_endpoint_url: String,
    pub(crate) mine_address: Address,
    pub(crate) flow_address: Address,
}

impl MinerConfig {
    pub fn new(
        miner_id: H256,
        rpc_endpoint_url: String,
        mine_address: Address,
        flow_address: Address,
    ) -> MinerConfig {
        MinerConfig {
            miner_id,
            rpc_endpoint_url,
            mine_address,
            flow_address,
        }
    }

    pub(crate) fn make_provider(&self) -> Result<Provider<Http>, String> {
        Provider::<Http>::try_from(&self.rpc_endpoint_url)
            .map_err(|e| format!("Can not parse blockchain endpoint: {:?}", e))
    }
}
