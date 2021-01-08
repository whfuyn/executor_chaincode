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
use futures::channel::oneshot;
use futures::stream;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use log::warn;
use prost::Message;
use std::collections::HashMap;
use std::path::Path;

use super::error::Error;
use super::error::Result;
use super::ledger::Ledger;
use super::ChaincodeRegistry;
use super::ExecutorCommand;
use super::MessageDump;
use super::Task;
use crate::chaincode::TransactionResult;
use crate::protos as pb;
use crate::protos::chaincode_message::Type as ChaincodeMsgType;
use crate::protos::ChaincodeMessage;

const LEDGER_DATA_DIR: &str = "ledger";

#[derive(Debug)]
struct TransactionContext {
    pub channel_id: String,
    pub tx_id: String,
    pub is_init: bool,
    pub notifier: oneshot::Sender<TransactionResult>,
}

#[derive(Debug)]
pub struct Handler {
    cc_name: String,
    cc_side: mpsc::Sender<ChaincodeMessage>,
    ledger: Ledger,
    contexts: HashMap<String, TransactionContext>,
    total_query_limit: usize,
    nonce: u64,
}

impl Handler {
    pub async fn register<T>(
        mut cc_stream: T,
        cc_registry: ChaincodeRegistry,
    ) -> Result<mpsc::Receiver<ChaincodeMessage>>
    where
        T: Stream<Item = std::result::Result<ChaincodeMessage, tonic::Status>>
            + Unpin
            + Send
            + Sync
            + 'static,
    {
        let msg = match cc_stream.next().await {
            Some(req) => req?,
            None => {
                return Err(Error::Other("register stream is empty".to_string()));
            }
        };
        if let Some(ChaincodeMsgType::Register) = ChaincodeMsgType::from_i32(msg.r#type) {
            let (mut cc_side, resp_rx) = mpsc::channel(64);

            let chaincode_id = pb::ChaincodeId::decode(&msg.payload[..])?;
            let cc_name = chaincode_id.name;
            let registered_resp = ChaincodeMessage {
                r#type: ChaincodeMsgType::Registered as i32,
                ..Default::default()
            };
            cc_side.send(registered_resp).await?;

            let ready_req = ChaincodeMessage {
                r#type: ChaincodeMsgType::Ready as i32,
                ..Default::default()
            };
            cc_side.send(ready_req).await?;

            let (task_tx, task_rx) = mpsc::channel(64);
            cc_registry.register(cc_name.clone(), task_tx).await;

            let data_dir = Path::new(LEDGER_DATA_DIR).join(&cc_name);
            let mut handler = Self {
                cc_name: cc_name.clone(),
                cc_side,
                ledger: Ledger::load(data_dir).await,
                contexts: HashMap::new(),
                total_query_limit: 65536, // TODO: support pagination
                nonce: 0,
            };

            tokio::spawn(async move {
                let cc_stream = cc_stream.map(Box::new).map(Task::Chaincode);
                let mut task_stream = stream::select(cc_stream, task_rx);
                while let Some(msg) = task_stream.next().await {
                    match msg {
                        Task::Executor(cmd) => {
                            if let Err(e) = handler.handle_executor_cmd(cmd).await {
                                warn!("handle executor cmd error: `{}`", e);
                            }
                        }
                        Task::Chaincode(boxed) => match *boxed {
                            Ok(msg) => {
                                if let Err(e) = handler.handle_chaincode_msg(msg).await {
                                    warn!("handle chaincode msg error: `{}`", e);
                                }
                            }
                            Err(status) => {
                                warn!("recv error from chaincode msg stream: `{}`", status);
                                cc_registry.deregister(&cc_name).await;
                                return;
                            }
                        },
                    }
                }
            });

            Ok(resp_rx)
        } else {
            Err(Error::Other(
                "first msg from register stream must be register msg".to_string(),
            ))
        }
    }

    async fn handle_get_state(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let get_state = pb::GetState::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;
        let collection = &get_state.collection;
        let res = if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .get_private_data(namespace_id, collection, &get_state.key)
        } else {
            self.ledger.get_state(namespace_id, &get_state.key)
        }
        .cloned()
        .unwrap_or_default();

        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            txid: msg.txid,
            payload: res,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;

        Ok(())
    }

    async fn handle_get_private_data_hash(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let get_state = pb::GetState::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;
        let collection = &get_state.collection;
        if tx_context.is_init {
            return Err(Error::InvalidOperation(
                "private data APIs are not allowed in chaincode Init()",
            ));
        }
        let res = self
            .ledger
            .get_private_data_hash(namespace_id, collection, &get_state.key)
            .cloned()
            .unwrap_or_default();
        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            txid: msg.txid,
            payload: res,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    async fn handle_get_state_metadata(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let get_state_metadata = pb::GetStateMetadata::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;
        let collection = &get_state_metadata.collection;
        let metadata = if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .get_private_data_metadata(namespace_id, collection, &get_state_metadata.key)
        } else {
            self.ledger
                .get_state_metadata(namespace_id, &get_state_metadata.key)
        }
        .cloned()
        .unwrap_or_default();
        let metadata_res = pb::StateMetadataResult {
            entries: metadata
                .into_iter()
                .map(|(k, v)| pb::StateMetadata {
                    metakey: k,
                    value: v,
                })
                .collect::<Vec<_>>(),
        };

        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            payload: metadata_res.dump(),
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    // TODO: support pagination?
    async fn handle_get_state_by_range(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let get_state_by_range = pb::GetStateByRange::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;
        let collection = &get_state_by_range.collection;
        let query_result = if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger.get_private_data_by_range(
                namespace_id,
                collection,
                &get_state_by_range.start_key,
                &get_state_by_range.end_key,
            )
        } else {
            self.ledger.get_state_by_range(
                namespace_id,
                &get_state_by_range.start_key,
                &get_state_by_range.end_key,
            )
        }
        .map(|rb| pb::QueryResultBytes {
            result_bytes: rb.dump(),
        })
        .collect::<Vec<_>>();

        let query_resp = pb::QueryResponse {
            results: query_result,
            has_more: false,
            ..Default::default()
        };

        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            payload: query_resp.dump(),
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    // since we don't support pagination, this msg is unexpected
    async fn handle_query_state_next(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported(
            "pagination query `query_state_next` is not supported yet",
        ))
    }

    // Even without pagination, we still receive a close notification
    async fn handle_query_state_close(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let query_state_close = pb::QueryStateClose::decode(&msg.payload[..])?;
        let query_resp = pb::QueryResponse {
            has_more: false,
            id: query_state_close.id,
            ..Default::default()
        };
        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            payload: query_resp.dump(),
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    // This msg is to execute structural queries on db, which is not supported yet
    async fn handle_get_query_result(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported("get_query_result is not supported yet"))
    }

    async fn handle_get_history_for_key(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let get_history_for_key = pb::GetHistoryForKey::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;

        let query_result = self
            .ledger
            .get_history_for_key(namespace_id, &get_history_for_key.key)
            .map(|rb| pb::QueryResultBytes {
                result_bytes: rb.dump(),
            })
            .collect::<Vec<_>>();

        let query_resp = pb::QueryResponse {
            results: query_result,
            has_more: false,
            ..Default::default()
        };

        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            payload: query_resp.dump(),
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;

        Ok(())
    }

    async fn handle_put_state(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let put_state = pb::PutState::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;
        let collection = &put_state.collection;
        if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .set_private_data(namespace_id, collection, &put_state.key, put_state.value)
                .await;
        } else {
            self.ledger
                .set_state(namespace_id, &msg.txid, &put_state.key, put_state.value)
                .await;
        }

        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    async fn handle_put_state_metadata(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let put_state_metadata = pb::PutStateMetadata::decode(&msg.payload[..])?;
        let metadata = {
            if let Some(metadata) = put_state_metadata.metadata {
                metadata
            } else {
                return Err(Error::InvalidOperation(
                    "put_state_metadata with empty metadata",
                ));
            }
        };
        let namespace_id = &self.cc_name;
        let collection = &put_state_metadata.collection;
        if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .set_private_data_metadata(
                    namespace_id,
                    collection,
                    &put_state_metadata.key,
                    metadata,
                )
                .await;
        } else {
            self.ledger
                .set_state_metadata(namespace_id, &put_state_metadata.key, metadata)
                .await;
        }
        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    async fn handle_del_state(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let del_state = pb::DelState::decode(&msg.payload[..])?;
        let namespace_id = &self.cc_name;
        let collection = &del_state.collection;
        if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .delete_private_data(namespace_id, collection, &del_state.key)
                .await;
        } else {
            self.ledger
                .delete_state(namespace_id, &msg.txid, &del_state.key)
                .await;
        }
        let resp = ChaincodeMessage {
            r#type: ChaincodeMsgType::Response as i32,
            txid: msg.txid,
            channel_id: msg.channel_id,
            ..Default::default()
        };
        self.cc_side.send(resp).await?;
        Ok(())
    }

    // A chaincode may call other chaincode during the execution of a transaction,
    // it's not supported yet.
    async fn handle_invoke_chaincode(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported("invoke chaincode is not supported yet"))
    }

    async fn handle_completed(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let resp = pb::Response::decode(&msg.payload[..])?;
        let ctx = self.contexts.remove(&ctx_id(&msg)).unwrap();
        let result = String::from_utf8_lossy(&resp.payload);
        ctx.notifier
            .send(TransactionResult {
                msg: resp.message,
                result: result.to_string(),
            })
            .unwrap();
        Ok(())
    }

    async fn handle_error(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let ctx = self.contexts.remove(&ctx_id(&msg)).unwrap();
        let error_msg = String::from_utf8_lossy(&msg.payload);
        ctx.notifier
            .send(TransactionResult {
                msg: error_msg.to_string(),
                result: "".to_string(),
            })
            .unwrap();
        Ok(())
    }

    // TODO: what else should I do with this keepalive msg?
    async fn handle_keepalive(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Ok(())
    }

    async fn handle_chaincode_msg(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let msg_ty = match ChaincodeMsgType::from_i32(msg.r#type) {
            Some(ty) => ty,
            None => {
                return Err(super::error::Error::UnknownChaincodeMsg(msg));
            }
        };

        use ChaincodeMsgType::*;
        match msg_ty {
            PutState => self.handle_put_state(msg).await,
            DelState => self.handle_del_state(msg).await,
            GetState => self.handle_get_state(msg).await,
            GetStateByRange => self.handle_get_state_by_range(msg).await,
            GetQueryResult => self.handle_get_query_result(msg).await,
            GetHistoryForKey => self.handle_get_history_for_key(msg).await,
            QueryStateNext => self.handle_query_state_next(msg).await,
            QueryStateClose => self.handle_query_state_close(msg).await,
            GetPrivateDataHash => self.handle_get_private_data_hash(msg).await,
            GetStateMetadata => self.handle_get_state_metadata(msg).await,
            PutStateMetadata => self.handle_put_state_metadata(msg).await,
            InvokeChaincode => self.handle_invoke_chaincode(msg).await,
            Keepalive => self.handle_keepalive(msg).await,
            Completed => self.handle_completed(msg).await,
            Error => self.handle_error(msg).await,
            _unexpected => Err(super::error::Error::InvalidChaincodeMsg(msg)),
        }
    }

    async fn handle_executor_cmd(&mut self, cmd: ExecutorCommand) -> Result<()> {
        match cmd {
            ExecutorCommand::Execute { msg, notifier } => {
                let ctx = TransactionContext {
                    channel_id: msg.channel_id.clone(),
                    tx_id: msg.txid.clone(),
                    is_init: false,
                    notifier,
                };
                self.contexts.insert(ctx_id(&msg), ctx);
                self.cc_side.send(msg).await?;
            }
            ExecutorCommand::Put { tx_id, key, value } => {
                let tx_id = String::from_utf8_lossy(tx_id.as_slice());
                self.ledger
                    .set_state(&self.cc_name, &tx_id, &key, value)
                    .await;
                // Ledger will be synced later.
            }
            ExecutorCommand::Sync => self.ledger.sync().await,
        }
        Ok(())
    }
}

fn ctx_id(msg: &ChaincodeMessage) -> String {
    let channel_id = &msg.channel_id;
    let txid = &msg.txid;
    format!("{}/{}", channel_id, txid)
}
