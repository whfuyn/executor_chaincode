use log::info;
use prost::Message;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use bytes::Buf;
use bytes::Bytes;

use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::SinkExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;

use crate::chaincode::ChaincodeSupportService;
use crate::chaincode::ExecutorCommand;
use crate::chaincode::Task;
use crate::chaincode::TransactionResult;

use crate::protos::chaincode_support_server::ChaincodeSupportServer;

use cita_cloud_proto::blockchain::CompactBlock;
use cita_cloud_proto::common::Hash;
use cita_cloud_proto::controller::raw_transaction::Tx;
use cita_cloud_proto::controller::RawTransaction;
use cita_cloud_proto::executor::executor_service_server::ExecutorServiceServer;
use cita_cloud_proto::executor::{
    executor_service_server::ExecutorService, CallRequest, CallResponse,
};

use tonic::transport::Server;

pub struct ExecutorServer {
    cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
}

impl ExecutorServer {
    pub fn new(cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>) -> Self {
        Self { cc_handles }
    }

    async fn send_task(&self, cc_name: &str, task: Task) {
        let mut h = self
            .cc_handles
            .read()
            .await
            .get(cc_name)
            .unwrap_or_else(|| panic!("chaincode `{}` is not registered", cc_name))
            .clone();
        h.send(task).await.unwrap();
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServer {
    async fn exec(&self, request: Request<CompactBlock>) -> Result<Response<Hash>, Status> {
        let block = request.into_inner();
        let mut updated_cc = HashSet::new();

        if let Some(body) = block.body {
            for tx_hash in body.tx_hashes {
                let filename = hex::encode(&tx_hash);
                let root_path = Path::new(".");
                let tx_path = root_path.join("txs").join(filename);

                let tx_bytes = fs::read(tx_path).await.unwrap();

                let raw_tx = RawTransaction::decode(&tx_bytes[..]).unwrap();
                match raw_tx.tx {
                    Some(Tx::NormalTx(utx)) => {
                        if let Some(tx) = utx.transaction {
                            let mut payload = Bytes::from(tx.data);
                            let cc_name_len = payload.get_u64();
                            let cc_name =
                                String::from_utf8_lossy(&payload.split_to(cc_name_len as usize))
                                    .to_string();
                            let (notifier, waiter) = oneshot::channel();
                            self.send_task(
                                &cc_name,
                                Task::Executor(ExecutorCommand::Execute {
                                    payload: payload.to_vec(),
                                    notifier,
                                }),
                            )
                            .await;
                            let TransactionResult { msg, result } = waiter.await.unwrap();
                            updated_cc.insert(cc_name.clone());
                            info!("tx completed:\n  msg: `{}`\n  result: `{}`", msg, result);
                            println!("tx completed:\n  msg: `{}`\n  result: `{}`", msg, result);
                        } else {
                            info!("block contains normal tx, but is empty");
                        }
                    }
                    Some(unknown) => info!("block contains unknown tx: `{:?}`", unknown),
                    None => info!("block contains empty tx"),
                }
            }
        }
        for cc_name in updated_cc {
            self.send_task(&cc_name, Task::Executor(ExecutorCommand::Sync))
                .await;
        }

        // TODO: return real hash
        let hash = vec![0u8; 33];
        let reply = Hash { hash };
        Ok(Response::new(reply))
    }

    async fn call(&self, _request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        Err(Status::unimplemented("read-only call is not supported"))
    }
}

#[derive(Clone)]
pub struct ChaincodeExecutor {
    cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
}

impl ChaincodeExecutor {
    pub fn new(cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>) -> Self {
        Self { cc_handles }
    }

    pub async fn run(&mut self, executor_addr: SocketAddr, chaincode_listen_addr: SocketAddr) {
        let chaincode_support = ChaincodeSupportService::new(self.cc_handles.clone());
        let ccs_svc = ChaincodeSupportServer::new(chaincode_support);

        tokio::spawn(async move {
            Server::builder()
                .add_service(ccs_svc)
                .serve(chaincode_listen_addr)
                .await
                .unwrap();
        });

        let executor_svc = ExecutorServiceServer::new(ExecutorServer::new(self.cc_handles.clone()));
        Server::builder()
            .add_service(executor_svc)
            .serve(executor_addr)
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chaincode::get_timestamp;
    use crate::chaincode::MessageDump;
    use crate::common;
    use crate::msp;
    use crate::protos;
    use crate::protos::chaincode_message::Type as ChaincodeMsgType;
    use crate::protos::ChaincodeMessage;

    const EXECUTOR_ADDR: &'static str = "127.0.0.1:50003";
    const CHAINCODE_LISTEN_ADDR: &'static str = "127.0.0.1:7052";

    fn run_executor() -> ChaincodeExecutor {
        let cc_handles = Arc::new(RwLock::new(HashMap::new()));
        let mut cc_executor = ChaincodeExecutor::new(cc_handles);
        let cc_executor_cloned = cc_executor.clone();
        tokio::spawn(async move {
            cc_executor
                .run(
                    EXECUTOR_ADDR.parse().unwrap(),
                    CHAINCODE_LISTEN_ADDR.parse().unwrap(),
                )
                .await;
        });
        cc_executor_cloned
    }

    struct TestTransaction {
        method: String,
        args: Vec<Vec<u8>>,
        transient_map: HashMap<String, Vec<u8>>,

        mspid: String,
        id_bytes: Vec<u8>,

        tx_id: String,
        channel_id: String,
        nonce: Vec<u8>,
    }

    impl TestTransaction {
        fn dump(&self) -> Vec<u8> {
            let header = {
                let channel_header = common::ChannelHeader {
                    r#type: common::HeaderType::EndorserTransaction as i32,
                    channel_id: self.channel_id.clone(),
                    timestamp: get_timestamp(),
                    tx_id: self.tx_id.clone(),
                    ..Default::default()
                }
                .dump();
                let creator = msp::SerializedIdentity {
                    mspid: self.mspid.clone(),
                    id_bytes: self.id_bytes.clone(),
                }
                .dump();
                let signature_header = common::SignatureHeader {
                    creator,
                    nonce: self.nonce.clone(),
                }
                .dump();
                common::Header {
                    channel_header,
                    signature_header,
                }
                .dump()
            };
            let input = {
                let args: Vec<Vec<u8>> =
                    [&[self.method.as_bytes().to_vec()], &self.args[..]].concat();
                protos::ChaincodeInput {
                    args,
                    decorations: HashMap::new(),
                    is_init: false,
                }
                .dump()
            };
            let payload = protos::ChaincodeProposalPayload {
                input: input.clone(),
                transient_map: self.transient_map.clone(),
            }
            .dump();
            let proposal = protos::Proposal {
                header,
                payload,
                extension: vec![],
            }
            .dump();
            let signed_proposal = protos::SignedProposal {
                proposal_bytes: proposal,
                signature: vec![],
            };
            ChaincodeMessage {
                r#type: ChaincodeMsgType::Transaction as i32,
                payload: input,
                txid: self.tx_id.clone(),
                channel_id: self.channel_id.clone(),
                proposal: Some(signed_proposal),
                ..Default::default()
            }
            .dump()
        }
    }

    struct TestTransactionFactory {
        channel_id: String,
        mspid: String,
        id_bytes: Vec<u8>,
        nonce: u64,
    }

    impl TestTransactionFactory {
        fn new(channel_id: String, mspid: String, id_bytes: Vec<u8>) -> Self {
            TestTransactionFactory {
                channel_id,
                mspid,
                id_bytes,
                nonce: 0,
            }
        }

        fn build<T: AsRef<str>>(
            &mut self,
            method: T,
            args: &[T],
            transient_map: &[(T, T)],
        ) -> TestTransaction {
            let method = method.as_ref().to_string();
            let args = args
                .iter()
                .map(|arg| arg.as_ref().as_bytes().to_vec())
                .collect();
            let transient_map: HashMap<String, Vec<u8>> = transient_map
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().as_bytes().to_vec()))
                .collect();

            let nonce = self.nonce.to_string();
            self.nonce += 1;
            TestTransaction {
                method,
                args,
                transient_map,
                tx_id: nonce.clone(),
                mspid: self.mspid.clone(),
                id_bytes: self.id_bytes.clone(),
                nonce: nonce.as_bytes().to_vec(),
                channel_id: self.channel_id.clone(),
            }
        }
    }

    #[tokio::test]
    async fn test_asset_transfer_secured_agreement() {
        let channel_id = "cita-cloud".to_string();
        // certs are from fabric-samples
        let org1_mspid = "Org1MSP".to_string();
        let org1_cert = "-----BEGIN CERTIFICATE-----
MIICJzCCAc6gAwIBAgIQKxBV8QdNKmtS2wu7DExPWzAKBggqhkjOPQQDAjBzMQsw
CQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZy
YW5jaXNjbzEZMBcGA1UEChMQb3JnMS5leGFtcGxlLmNvbTEcMBoGA1UEAxMTY2Eu
b3JnMS5leGFtcGxlLmNvbTAeFw0yMDEwMTIwODI5MDBaFw0zMDEwMTAwODI5MDBa
MGoxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRYwFAYDVQQHEw1T
YW4gRnJhbmNpc2NvMQ0wCwYDVQQLEwRwZWVyMR8wHQYDVQQDExZwZWVyMC5vcmcx
LmV4YW1wbGUuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEMutVyQ9OX0Ui
29Cn/E4+eq3SZl1LlSlqMNDup5KQqo9lVY2CKcNuWeKeV+YoDijQRPTLW7o2ZDuJ
yn7ZvtOBXaNNMEswDgYDVR0PAQH/BAQDAgeAMAwGA1UdEwEB/wQCMAAwKwYDVR0j
BCQwIoAg6WZDnHPhiJpYBVNBJTwE0YW45ThbtJt7qhk7WivY+AIwCgYIKoZIzj0E
AwIDRwAwRAIgDNvR3C6j+SVncmmr0GvcomW3j3SqbQ4toRRMOiRa56ICIHHcMiAM
S4u7BSot5a2st7igwkukLRk2e5TwFhECcZDA
-----END CERTIFICATE-----";

        let org2_mspid = "Org2MSP".to_string();
        let org2_cert = "-----BEGIN CERTIFICATE-----
MIICJzCCAc6gAwIBAgIQLn1I5xYJ7cb+d5MN8+U+tzAKBggqhkjOPQQDAjBzMQsw
CQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZy
YW5jaXNjbzEZMBcGA1UEChMQb3JnMi5leGFtcGxlLmNvbTEcMBoGA1UEAxMTY2Eu
b3JnMi5leGFtcGxlLmNvbTAeFw0yMDEwMTIwODI5MDBaFw0zMDEwMTAwODI5MDBa
MGoxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRYwFAYDVQQHEw1T
YW4gRnJhbmNpc2NvMQ0wCwYDVQQLEwRwZWVyMR8wHQYDVQQDExZwZWVyMC5vcmcy
LmV4YW1wbGUuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEPgfrahKAsBxC
mJZSFblj7f2pgiO3sZ2I4I24YB9YKsFFZVXO2USqGnndxhYxHdG2gryZGQ4jDB2B
pgzhSEfUeaNNMEswDgYDVR0PAQH/BAQDAgeAMAwGA1UdEwEB/wQCMAAwKwYDVR0j
BCQwIoAgNIhFkVF64ELH7I2LMF5ozCFDVTDpODp2NUgy9w4tEPQwCgYIKoZIzj0E
AwIDRwAwRAIgXEKPv1tgXjum6aikVT3AJIjig1TF7KCojogDrZqu3lACIGdji2sX
Jfn1p8cfo4BPd3tSllZEIbXE2uCMkKE4LGmo
-----END CERTIFICATE-----";

        let mut org1 = TestTransactionFactory::new(
            channel_id.clone(),
            org1_mspid,
            org1_cert.as_bytes().to_vec(),
        );
        let mut org2 =
            TestTransactionFactory::new(channel_id, org2_mspid, org2_cert.as_bytes().to_vec());

        let mut txs = vec![];

        txs.push(org1.build(
            "CreateAsset",
            &["asset1", "A new asset for Org1MSP"],
            &[("asset_properties", "asset1's property")],
        ));
        txs.push(org1.build("GetAssetPrivateProperties", &["asset1"], &[]));
        txs.push(org1.build("ReadAsset", &["asset1"], &[]));
        txs.push(org1.build(
            "ChangePublicDescription",
            &["asset1", "This asset is for sale"],
            &[],
        ));
        txs.push(org1.build("ReadAsset", &["asset1"], &[]));
        txs.push(org2.build(
            "ChangePublicDescription",
            &["asset1", "The worst asset"],
            &[],
        ));
        txs.push(org1.build("ReadAsset", &["asset1"], &[]));
        txs.push(org1.build(
            "AgreeToSell",
            &["asset1"],
            &[("asset_price", "{\"asset_id\":\"asset1\",\"trade_id\":\"109f4b3c50d7b0df729d299bc6f8e9ef9066971f\",\"price\":110}")]
        ));
        txs.push(org1.build("GetAssetSalesPrice", &["asset1"], &[]));
        txs.push(org2.build(
            "VerifyAssetProperties",
            &["asset1"],
            &[("asset_properties", "asset1's property")],
        ));
        txs.push(org2.build(
            "AgreeToBuy",
            &["asset1"],
            &[("asset_price", "{\"asset_id\":\"asset1\",\"trade_id\":\"109f4b3c50d7b0df729d299bc6f8e9ef9066971f\",\"price\":100}")]
        ));
        txs.push(org2.build("GetAssetBidPrice", &["asset1"], &[]));
        txs.push(org1.build(
            "TransferAsset",
            &["asset1","Org2MSP"],
            &[("asset_properties", "asset1's property"), ("asset_price", "{\"asset_id\":\"asset1\",\"trade_id\":\"109f4b3c50d7b0df729d299bc6f8e9ef9066971f\",\"price\":100}")]
        ));
        txs.push(org1.build(
            "AgreeToSell",
            &["asset1"],
            &[("asset_price", "{\"asset_id\":\"asset1\",\"trade_id\":\"109f4b3c50d7b0df729d299bc6f8e9ef9066971f\",\"price\":100}")]
        ));
        txs.push(org1.build(
            "TransferAsset",
            &["asset1","Org2MSP"],
            &[("asset_properties", "asset1's property"), ("asset_price", "{\"asset_id\":\"asset1\",\"trade_id\":\"109f4b3c50d7b0df729d299bc6f8e9ef9066971f\",\"price\":100}")]
        ));
        txs.push(org2.build("ReadAsset", &["asset1"], &[]));
        txs.push(org2.build("GetAssetPrivateProperties", &["asset1"], &[]));
        txs.push(org2.build(
            "ChangePublicDescription",
            &["asset1", "This asset is not for sale"],
            &[],
        ));
        txs.push(org2.build("ReadAsset", &["asset1"], &[]));

        exec_txs(txs).await;
    }

    async fn exec_txs(txs: Vec<TestTransaction>) {
        use std::time::Duration;
        use tokio::time::delay_for;
        let executor = run_executor();
        let mut sender = executor
            .cc_handles
            .read()
            .await
            .values()
            .next()
            .expect("no chaincode registered")
            .clone();
        delay_for(Duration::from_secs(5)).await;
        for tx in txs {
            let (notifier, waiter) = futures::channel::oneshot::channel();
            sender
                .send(Task::Executor(ExecutorCommand::Execute {
                    payload: tx.dump(),
                    notifier,
                }))
                .await
                .unwrap();
            waiter.await.unwrap();
        }
    }
}
