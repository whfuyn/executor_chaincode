pub mod error;
mod handler;
mod ledger;
mod support;

use log::info;
use log::warn;
use prost::Message;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use futures::channel::mpsc;
use futures::channel::oneshot;

use crate::protos::ChaincodeMessage;
pub use support::ChaincodeSupportService;

#[derive(Debug)]
pub enum Task {
    Executor(ExecutorCommand),
    Chaincode(Box<std::result::Result<ChaincodeMessage, tonic::Status>>),
}

#[derive(Debug)]
pub struct TransactionResult {
    pub msg: String,
    pub result: String,
}

#[derive(Debug)]
pub enum ExecutorCommand {
    Execute {
        payload: Vec<u8>,
        notifier: oneshot::Sender<TransactionResult>,
    },
    Sync,
}

#[derive(Debug, Clone)]
pub struct ChaincodeRegistry {
    registry: Arc<RwLock<HashMap<String, mpsc::Sender<Task>>>>,
}

impl ChaincodeRegistry {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, name: String, sender: mpsc::Sender<Task>) {
        use std::collections::hash_map::Entry;
        let mut registry = self.registry.write().await;

        match registry.entry(name.clone()) {
            Entry::Occupied(_) => warn!("chaincode `{}` has already been registered, skip.", name),
            Entry::Vacant(e) => {
                e.insert(sender);
                info!("Chaincode `{}` registered.", name);
            }
        }
    }

    pub async fn deregister(&self, name: &str) {
        let mut registry = self.registry.write().await;
        if registry.remove(name).is_none() {
            warn!(
                "try to deregister chaincode `{}`, but it's not registered.",
                name
            );
        } else {
            info!("Chaincode `{}` deregistered.", name);
        }
    }

    pub async fn entry(&self, name: &str) -> Option<mpsc::Sender<Task>> {
        self.registry.read().await.get(name).cloned()
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
