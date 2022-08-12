from web3 import Web3

IONIAN_CONFIG = dict(log_config_file="log_config")

BSC_CONFIG = dict(
    NetworkId=1000,
    HTTPPort=8545,
    HTTPHost="127.0.0.1",
    Etherbase="0x7df9a875a174b3bc565e6424a0050ebc1b2d1d82",
    DataDir="test/local_ethereum_blockchain/node1",
    Port=30303,
    Verbosity=3,
)

GENESIS_PRIV_KEY = "46b9e861b63d3509c88b7817275a30d22d62c8cd8fa6486ddee35ef0d8e0495f"
GENESIS_ACCOUNT = Web3().eth.account.from_key(GENESIS_PRIV_KEY)
TX_PARAMS = {"gasPrice": 1_000_000_000, "from": GENESIS_ACCOUNT.address}
