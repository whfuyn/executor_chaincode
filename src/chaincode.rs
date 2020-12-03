pub mod error;
mod handler;
mod ledger;
mod server;

use crate::protos::ChaincodeMessage;
use prost::Message;
pub use server::ChaincodeSupportService;

#[derive(Debug)]
pub enum Task {
    Chaincode(ChaincodeMessage),
    Executor(ExecutorCommand),
}

#[derive(Debug)]
pub struct ExecutorCommand {
    tx_hash: Vec<u8>,
    payload: Vec<u8>,
}

impl ExecutorCommand {
    pub fn new(tx_hash: Vec<u8>, payload: Vec<u8>) -> Self {
        Self { tx_hash, payload }
    }
}

pub trait MessageDump {
    fn dump(&self) -> Vec<u8>;
}

impl<T: Message> MessageDump for T {
    fn dump(&self) -> Vec<u8> {
        let mut payload = vec![];
        self.encode(&mut payload).unwrap();
        payload
    }
}

pub fn get_timestamp() -> Option<prost_types::Timestamp> {
    use std::convert::TryFrom;
    use std::time::SystemTime;
    let now = SystemTime::now();
    prost_types::Timestamp::try_from(now).ok()
}
