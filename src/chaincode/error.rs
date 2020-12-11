use futures::channel::mpsc;
use prost::DecodeError;
use thiserror::Error as ThisError;

use crate::protos::ChaincodeMessage;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("The first chaincode msg must be register")]
    NotRegistered,
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    #[error("chaincode msg decode failed: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("chat stream with chaincode is interrupted: {0}")]
    InterruptedStream(#[from] mpsc::SendError),
    #[error("invalid operation: {0}")]
    InvalidOperation(&'static str),
    #[error("invalid chaincode msg: {0:?}")]
    InvalidChaincodeMsg(ChaincodeMessage),
    #[error("unknown chaincode msg type: {0:?}")]
    UnknownChaincodeMsg(ChaincodeMessage),
    #[error("other")]
    Other,
}
