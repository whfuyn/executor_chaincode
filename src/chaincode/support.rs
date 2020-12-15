// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;

use tonic::{Response, Status};

use futures::stream::Stream;
use futures::stream::StreamExt;

use crate::protos::chaincode_support_server::ChaincodeSupport;
use crate::protos::ChaincodeMessage;

use super::handler::Handler;
use super::ChaincodeRegistry;

pub struct ChaincodeSupportService {
    cc_registry: ChaincodeRegistry,
}

impl ChaincodeSupportService {
    pub fn new(cc_registry: ChaincodeRegistry) -> Self {
        Self { cc_registry }
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
        let mut resp_rx = match Handler::register(cc_stream, self.cc_registry.clone()).await {
            Ok(resp_rx) => resp_rx,
            Err(e) => return Err(tonic::Status::aborted(e.to_string())),
        };

        let output = async_stream::try_stream! {
            while let Some(msg) = resp_rx.next().await {
                yield msg;
            }
        };

        Ok(Response::new(Box::pin(output) as Self::RegisterStream))
    }
}
