$env:CORE_CHAINCODE_ID_NAME="asset-transfer-secured-agreement"
$env:CORE_PEER_TLS_ENABLED="false"
$env:CORE_PEER_LOCALMSPID="Org1MSP"
./tradingMarbles.exe --peer.address 127.0.0.1:7052
