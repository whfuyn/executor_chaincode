# executor_chaincode

[![Build Status](https://travis-ci.org/cita-cloud/executor_chaincode.svg?branch=main)](https://travis-ci.org/cita-cloud/executor_chaincode)

`executor_chaincode` is an implementation of CITA-Cloud's executor. 

It's compatible with fabric's chaincode with some limitations.

Users can write their smart contracts in chaincode, and run it in CITA-Cloud. 

## Usage

### 1. Executor

```
cargo build --release
```

To build docker image:

```
docker build -t citacloud/executor_chaincode .
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

    Due to the different executing models of transactions between CITA-Cloud and fabric, constraining a transaction to be executed in a specific executor is infeasible. So the org check in the `asset-transfer-secured-agreement` is removed.

* `CORE_PEER_TLS_ENABLED` is not supported yet.

* `CORE_CHAINCODE_ID_NAME` is the name of the chaincode.

* `-peer.address` is the executor's address. Chaincode will register itself to the executor to start a bidirectional request/response stream.

You may check `./examples` to see some chaincodes examples from `fabric-samples`.

### 3. Send transaction

Transaction data is the concat of `chaincode_name.len().to_be_bytes()` `chaincode_name` `payload`.

Transactions in a finalized block will be executed by sending its payload to the chaincode indicated by `chaincode_name`.

The chaincode, as a standalone server, expect a `ChaincodeMessage` defined in `fabric-protos/peer/chaincode_shim.proto`

The detail format of the `ChaincodeMessage` is complicated, but its basic structure is:

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

I write a [library](https://github.com/cita-cloud/chaincode_invoker) for building and sending this kind of transaction.

## Limitations

### 1. Private data and organization check

Since transactions are executed equally on every peers, 
organization check that verify peer and client org match will fail, 
and private data attached to the transaction will be sent to and stored by every peers.

### 2. Channel

Channel is not supported yet. Users may run multiple CITA-Cloud instances to simulate channel.

### 3. Pagination

Paginated queries are not supported yet. Queries will return full results at once.

### 4. Invoke other chaincodes

Invoking other chaincodes is not supported yet.

### 5. DB specified queries

```go
ctx.GetStub().GetQueryResult(queryString)
```
This kind of DB specified queries is not supported yet. 
