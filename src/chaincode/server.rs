use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use tonic::{Response, Status};

use futures::channel::mpsc;
use futures::stream::Stream;
use futures::stream::StreamExt;

use tokio::sync::RwLock;

use crate::protos::chaincode_support_server::ChaincodeSupport;
use crate::protos::ChaincodeMessage;

use super::Task;

use super::handler::Handler;
use super::handler::Registry;

pub struct ChaincodeSupportService {
    cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
}

impl ChaincodeSupportService {
    pub fn new(cc_handles: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>) -> Self {
        Self { cc_handles }
    }
}

#[tonic::async_trait]
impl ChaincodeSupport for ChaincodeSupportService {
    type RegisterStream =
        Pin<Box<dyn Stream<Item = Result<ChaincodeMessage, Status>> + Send + Sync + 'static>>;

    async fn register(
        &self,
        request: tonic::Request<tonic::Streaming<ChaincodeMessage>>,
    ) -> Result<tonic::Response<Self::RegisterStream>, tonic::Status> {
        let cc_stream = request.into_inner();
        let Registry {
            name,
            handle,
            mut resp_rx,
        } = Handler::register(cc_stream).await.unwrap();

        self.cc_handles.write().await.insert(name, handle);

        let output = async_stream::try_stream! {
            while let Some(msg) = resp_rx.next().await {
                yield msg;
            }
        };

        Ok(Response::new(Box::pin(output) as Self::RegisterStream))
    }
}
