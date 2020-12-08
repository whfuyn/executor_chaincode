# executor_chaincode
`executor_chaincode` is an implementation of CITA-Cloud's executor. 

It's compatible with fabric's chaincode with some limitations.

Users can write their smart contracts in chaincode, and run it in CITA-Cloud. 

## Usage

### 1. Executor

```
cargo build --release
```

You may find more details in [runner_k8s](https://github.com/cita-cloud/runner_k8s) and [runner_consul](https://github.com/cita-cloud/runner_consul)

### 2. Chaincode

Build your chaincode and run it after the executor started.

You need to run a chaincode instance for each executor.

Note that some environment variables need to be set.
```
CORE_PEER_LOCALMSPID="Org1MSP" \
CORE_PEER_TLS_ENABLED="false" \
CORE_CHAINCODE_ID_NAME="asset-transfer-secured-agreement" \
./tradingMarbles -peer.address 127.0.0.1:7052
```
In fabric, some transaction can only be simulated and endorsed by peers in the same org with the peer.

* `CORE_PEER_LOCALMSPID` represents the org of the peer running this chaincode server.

    Due to the different models of executing a transaction between CITA-Cloud and fabric, constraining a transaction to be executed in a specific executor is not viable. So the org check in the `asset-transfer-secured-agreement` is removed.

* `CORE_PEER_TLS_ENABLED` is not supported yet.

* `CORE_CHAINCODE_ID_NAME` is the name of the chaincode.

* `-peer.address` is the executor's address. Chaincode will register itself to the executor to start a bidirectional request/response stream.

You may check `./examples` to see some chaincodes examples from `fabric-samples`.

### 3. Send transaction

Transactions in a finalized block will be executed by sending its data directly to the chaincode.

The chaincode, as a standalone server, expect a `ChaincodeMessage` defined in `fabric-protos/peer/chaincode_shim.proto`

And the detailed structure of the `ChaincodeMessage` is complicated. Here is a rust code snippet to show you how it looks like:

```rust
ChaincodeMessage {
    r#type: ChaincodeMsgType::Transaction as i32,
    // This payload contains the function name to be called and its args.
    payload,
    txid,
    channel_id,
    // signed_proposal is used to provide caller's identity and this call's private data.
    proposal: Some(signed_proposal),
    ..Default::default()
}
```

I wrote a [tool](https://github.com/cita-cloud/chaincode_cli) for building and sending this kind of transaction.


## Build docker image
```
docker build -t citacloud/executor_chaincode .
```
