mod error;
mod handler;
mod server;

use crate::protos::ChaincodeMessage;
use crate::protos::ChaincodeInput;
pub use server::ChaincodeSupportService;
use prost::Message;

#[derive(Debug)]
pub enum Task {
    Chaincode(ChaincodeMessage),
    Executor(ExecutorCommand),
}

#[derive(Debug)]
pub struct ExecutorCommand {
    payload: Vec<u8>,
}

impl ExecutorCommand {
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    //args includes the function name and its args.
    pub fn from_args(args: Vec<Vec<u8>>) -> Self {
        let input = ChaincodeInput {
            args,
            ..Default::default()
        };
        Self {
            payload: input.dump(),
        }
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