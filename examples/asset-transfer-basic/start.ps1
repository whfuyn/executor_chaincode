$env:CORE_PEER_LOCALMSPID="Org1MSP"
$env:CORE_CHAINCODE_ID_NAME="asset-transfer-basic"
$env:CORE_PEER_TLS_ENABLED="false"
./chaincode-go.exe --peer.address 127.0.0.1:7052
