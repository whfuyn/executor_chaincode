# Asset transfer basic

This chaincode example is from [fabric-samples](https://github.com/hyperledger/fabric-samples).

## Usage

Build docker image:
```
docker build -t asset-transfer-basic .
```

Run:
```
docker run \
    -e CORE_PEER_LOCALMSPID="Org1MSP" \
    -e CORE_PEER_TLS_ENABLED="false" \
    -e CORE_CHAINCODE_ID_NAME="asset-transfer-basic" \
    --net host \
    asset-transfer-basic \
    --peer.address "127.0.0.1:7052"
```

You need to run a chaincode for each executor with corresponding `--peer.address`.
