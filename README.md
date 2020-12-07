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

### 3. Send transaction

There is a [cli](https://github.com/cita-cloud/chaincode_cli) for sending transactions.

