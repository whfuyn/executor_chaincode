use cita_cloud_proto::blockchain::CompactBlock;
use cita_cloud_proto::common::Hash;
use cita_cloud_proto::executor::{
    executor_service_server::ExecutorService, CallRequest, CallResponse,
};
use cita_cloud_proto::storage::{storage_service_client::StorageServiceClient, Content, ExtKey};
use parking_lot::RwLock;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::chaincode::ExecutorCommand;
use crate::chaincode::Task;
use futures::channel::mpsc;
use futures::SinkExt;
use std::collections::HashMap;
use std::net::SocketAddr;

use crate::protos::chaincode_support_server::ChaincodeSupportServer;
use cita_cloud_proto::controller::raw_transaction::Tx;
use cita_cloud_proto::controller::RawTransaction;
use cita_cloud_proto::executor::executor_service_server::ExecutorServiceServer;
use prost::Message;

use tonic::transport::Server;

use crate::chaincode::ChaincodeSupportService;

use log::{debug, info};

use std::path::Path;
use tokio::fs;

pub struct ExecutorServer {
    storage_port: u16,
    cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
}

impl ExecutorServer {
    pub fn new(
        storage_port: u16,
        cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
    ) -> Self {
        Self {
            storage_port,
            cc_handles,
        }
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServer {
    async fn exec(&self, request: Request<CompactBlock>) -> Result<Response<Hash>, Status> {
        let block = request.into_inner();
        // let mut client = {
        //     let storage_addr = format!("http://127.0.0.1:{}", self.storage_port);
        //     StorageServiceClient::connect(storage_addr).await.unwrap()
        // };

        if let Some(body) = block.body {
            for tx_hash in body.tx_hashes {
                // info!("tx_hash: {:?}", &tx_hash);
                // let request = Request::new(ExtKey { region: 1, key: tx_hash });
                // let response = client.load(request).await?;

                let filename = hex::encode(&tx_hash);
                let root_path = Path::new(".");
                let tx_path = root_path.join("txs").join(filename);

                let tx_bytes = fs::read(tx_path).await.unwrap();
                // For now, there is only one chaincode.
                let mut h = self.cc_handles.read().values().next().unwrap().clone();
                // let tx_bytes = response.into_inner().value;
                // info!("raw_tx: {:?}", &tx_bytes);
                let raw_tx = RawTransaction::decode(tx_bytes.as_slice()).unwrap();
                match raw_tx.tx {
                    Some(Tx::NormalTx(utx)) => {
                        if let Some(tx) = utx.transaction {
                            h.send(Task::Executor(ExecutorCommand::new(tx_hash, tx.data)))
                                .await
                                .unwrap();
                        } else {
                            info!("block contains normal tx, but is empty");
                        }
                    }
                    Some(unknown) => info!("block contains unknown tx: `{:?}`", unknown),
                    None => info!("block contains empty tx"),
                }
            }
        }

        let hash = vec![0u8; 33];
        let reply = Hash { hash };
        Ok(Response::new(reply))
    }

    async fn call(&self, _request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {
        let value = vec![0u8];
        let reply = CallResponse { value };
        Ok(Response::new(reply))
    }
}

pub struct ChaincodeExecutor;

impl ChaincodeExecutor {
    pub async fn run(executor_addr: SocketAddr, chaincode_addr: SocketAddr) {
        let cc_handles = Arc::new(RwLock::new(HashMap::new()));

        let chaincode_support = ChaincodeSupportService::new(cc_handles.clone());

        let ccs_svc = ChaincodeSupportServer::new(chaincode_support);

        tokio::spawn(async move {
            Server::builder()
                .add_service(ccs_svc)
                .serve(chaincode_addr)
                .await
                .unwrap();
        });

        let executor_svc = ExecutorServiceServer::new(ExecutorServer::new(50003u16, cc_handles));
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
    use crate::protos::ChaincodeInput;
    use cita_cloud_proto::blockchain::CompactBlock;
    use cita_cloud_proto::executor::executor_service_client::ExecutorServiceClient;

    const EXECUTOR_ADDR: &'static str = "127.0.0.1:50003";
    const CHAINCODE_ADDR: &'static str = "127.0.0.1:7052";

    fn run_executor() {
        tokio::spawn(async move {
            ChaincodeExecutor::run(
                EXECUTOR_ADDR.parse().unwrap(),
                CHAINCODE_ADDR.parse().unwrap(),
            )
            .await;
        });
    }

    #[tokio::test]
    async fn test_basic() {
        // run_executor();
        // let client = ExecutorServiceClient::connect(executor_addr).await.unwrap();
        // let args = vec!["InitLedger".as_bytes().to_vec()];
        // let input = ChaincodeInput {
        //     args,
        //     decorations: HashMap::new(),
        //     is_init: false,
        // };
        // let payload = input.dump();
    }
}
