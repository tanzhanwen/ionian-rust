use ethers::prelude::abigen;

// run `cargo doc -p contract-interface --open` to read struct definition

abigen!(
    IonianFlow,
    "../../ionian-contracts/artifacts/contracts/dataFlow/Flow.sol/Flow.json"
);

abigen!(
    IonianMine,
    "../../ionian-contracts/artifacts/contracts/miner/IonianMine.sol/IonianMine.json"
);
