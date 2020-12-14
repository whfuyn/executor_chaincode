# Secured asset transfer

This example chaincode is from fabric-samples.

[Secured asset transfer in Fabric Tutorial](https://hyperledger-fabric.readthedocs.io/en/latest/secured_asset_transfer/secured_private_asset_transfer_tutorial.html)

## Usage

Build docker image:
```
docker build -t asset-transfer-secured-agreement .
```

Run:
```
docker run -e CORE_PEER_LOCALMSPID="Org1MSP" -e CORE_PEER_TLS_ENABLED="false" -e CORE_CHAINCODE_ID_NAME="asset-transfer-secured-agreement" --net host asset-transfer-secured-agreement --peer.address "127.0.0.1:7052"
```

You need to run a chaincode for each executor with corresponding `--peer.address`.

## Code modification
I comment out the code verifying client org match peer org in asset_transfer.go (line 497-500).
