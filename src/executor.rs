use tonic::{Request, Response, Status};
use cita_cloud_proto::blockchain::CompactBlock;
use cita_cloud_proto::common::Hash;
use cita_cloud_proto::executor::{
    executor_service_server::ExecutorService,
    CallRequest, CallResponse,
};
use cita_cloud_proto::storage::{storage_service_client::StorageServiceClient, Content, ExtKey};
use std::sync::Arc;
use parking_lot::RwLock;

use futures::channel::mpsc;
use std::collections::HashMap;
use crate::chaincode::Task;
use crate::chaincode::ExecutorCommand;
use futures::SinkExt;
use std::net::SocketAddr;

use crate::protos::chaincode_support_server::ChaincodeSupportServer;
use cita_cloud_proto::executor::executor_service_server::ExecutorServiceServer;

use tonic::transport::Server;

use crate::chaincode::ChaincodeSupportService;


pub struct ExecutorServer {
    storage_port: u16,
    cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
}

impl ExecutorServer {
    pub fn new(storage_port: u16, cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>) -> Self {
        Self{
            storage_port,
            cc_handles,
        }
    }
}

#[tonic::async_trait]
impl ExecutorService for ExecutorServer {
    async fn exec(&self, request: Request<CompactBlock>) -> Result<Response<Hash>, Status> {
        let block = request.into_inner();
        let mut client = {
            let storage_addr = format!("http://127.0.0.1:{}", self.storage_port);
            StorageServiceClient::connect(storage_addr).await.unwrap()
        };

        if let Some(body) = block.body {
            for tx_hash in body.tx_hashes {
                let request = Request::new(ExtKey { region: 1, key: tx_hash });
                let response = client.load(request).await?;
                // For now, there is only one chaincode.
                let mut h = self.cc_handles
                    .read()
                    .values()
                    .next()
                    .unwrap()
                    .clone();
                h.send(Task::Executor(ExecutorCommand::new(response.into_inner().value))).await.unwrap();
            }
        }

        let hash = vec![0u8; 33];
        let reply = Hash { hash };
        Ok(Response::new(reply))
    }
    async fn call(&self, request: Request<CallRequest>) -> Result<Response<CallResponse>, Status> {

        let value = vec![0u8];
        let reply = CallResponse { value };
        Ok(Response::new(reply))
    }
}

pub struct ChaincodeExecutor;

impl ChaincodeExecutor {
    pub async fn run(executor_addr: SocketAddr, chaincode_addr: SocketAddr) {

        let cc_handles = Arc::new(RwLock::new(HashMap::new()));

        let chaincode_support = ChaincodeSupportService::new(
            cc_handles.clone()
        );

        let ccs_svc = ChaincodeSupportServer::new(chaincode_support);

        tokio::spawn(async move {
            Server::builder()
                .add_service(ccs_svc)
                .serve(chaincode_addr)
                .await
                .unwrap();
        });

        let executor_svc = ExecutorServiceServer::new(
            ExecutorServer::new(50001u16, cc_handles)
        );
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
    use cita_cloud_proto::executor::executor_service_client::ExecutorServiceClient;
    use cita_cloud_proto::blockchain::CompactBlock;
    use crate::protos::ChaincodeInput;

    const EXECUTOR_ADDR: &'static str = "127.0.0.1:50003";
    const CHAINCODE_ADDR: &'static str = "127.0.0.1:7052";

    fn run_executor() {
        tokio::spawn(async move {
            ChaincodeExecutor::run(
                EXECUTOR_ADDR.parse().unwrap(),
                CHAINCODE_ADDR.parse().unwrap()
            ).await;
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
