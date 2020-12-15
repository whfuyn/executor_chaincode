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

use futures::channel::mpsc;
use prost::DecodeError;
use thiserror::Error as ThisError;

use crate::protos::ChaincodeMessage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("chaincode `{0}` is not registered")]
    NotRegistered(String),
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    #[error("chaincode msg decode failed: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("chaincode chat stream is interrupted: {0}")]
    InterruptedStream(#[from] mpsc::SendError),
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("invalid operation: {0}")]
    InvalidOperation(&'static str),
    #[error("invalid chaincode msg: {0:?}")]
    InvalidChaincodeMsg(ChaincodeMessage),
    #[error("unknown chaincode msg type: {0:?}")]
    UnknownChaincodeMsg(ChaincodeMessage),
    #[error("other error: {0}")]
    Other(String),
}
