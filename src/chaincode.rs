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
        msg: ChaincodeMessage,
        notifier: oneshot::Sender<TransactionResult>,
    },
    Put {
        tx_id: Vec<u8>,
        key: String,
        value: Vec<u8>,
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
        let mut payload = Vec::with_capacity(self.encoded_len());
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
