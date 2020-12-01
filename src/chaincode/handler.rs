use super::ledger::Ledger;
use super::ExecutorCommand;
use super::MessageDump;
use super::Task;
use crate::protos as pb;
use crate::protos::chaincode_message::Type as ChaincodeMsgType;
use crate::protos::ChaincodeMessage;
use futures::channel::mpsc;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use futures::{future, stream};
use std::collections::HashMap;

use prost::Message;

use super::error::Error;
use super::error::Result;
use log::{info, warn};

#[derive(Debug)]
struct TransactionContext {
    pub channel_id: String,
    pub namespace_id: String,
    pub is_init: bool,
}

#[derive(Debug)]
pub struct Handler {
    cc_side: mpsc::Sender<ChaincodeMessage>,
    ledger: Ledger,
    contexts: HashMap<String, TransactionContext>,
    total_query_limit: usize,
}

pub struct Registry {
    pub name: String,
    pub handle: mpsc::Sender<Task>,
    pub resp_rx: mpsc::Receiver<ChaincodeMessage>,
}

impl Handler {
    pub async fn register<T>(mut cc_stream: T) -> Result<Registry>
    where
        T: Stream<Item = std::result::Result<ChaincodeMessage, tonic::Status>>
            + Unpin
            + Send
            + Sync
            + 'static,
    {
        if let Some(Ok(msg)) = cc_stream.next().await {
            if let Some(ChaincodeMsgType::Register) = ChaincodeMsgType::from_i32(msg.r#type) {
                let (mut cc_side, resp_rx) = mpsc::channel(64);

                let cc_name = String::from_utf8(msg.payload).unwrap();
                let registered_resp = ChaincodeMessage {
                    r#type: ChaincodeMsgType::Registered as i32,
                    ..Default::default()
                };
                cc_side.send(registered_resp).await.unwrap();

                let ready_req = ChaincodeMessage {
                    r#type: ChaincodeMsgType::Ready as i32,
                    ..Default::default()
                };
                cc_side.send(ready_req).await.unwrap();

                info!("Chaincode `{}` registered.", cc_name);

                let (task_tx, task_rx) = mpsc::channel(64);

                let mut handler = Self {
                    cc_side,
                    ledger: Ledger::new(),
                    contexts: HashMap::new(),
                    total_query_limit: 65536, // TODO: support pagination
                };

                tokio::spawn(async move {
                    let cc_stream = cc_stream.filter_map(|res| match res {
                        Ok(msg) => future::ready(Some(Task::Chaincode(msg))),
                        Err(e) => {
                            info!("Chaincode stream error: `{:?}`", e);
                            future::ready(None)
                        }
                    });
                    let mut task_stream = stream::select(cc_stream, task_rx);
                    while let Some(msg) = task_stream.next().await {
                        match msg {
                            Task::Chaincode(msg) => {
                                if let Err(e) = handler.handle_chaincode_msg(msg).await {
                                    warn!("handle chaincode msg error: `{}`", e);
                                }
                            }
                            Task::Executor(cmd) => handler.handle_executor_cmd(cmd).await.unwrap(),
                        }
                    }
                });

                Ok(Registry {
                    name: cc_name,
                    handle: task_tx,
                    resp_rx,
                })
            } else {
                Err(Error::NotRegistered)
            }
        } else {
            Err(Error::Other)
        }
    }

    async fn handle_get_state(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let get_state = pb::GetState::decode(&msg.payload[..])?;
        let namespace_id = &tx_context.namespace_id;
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
        let namespace_id = &tx_context.namespace_id;
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
        let namespace_id = &tx_context.namespace_id;
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
        let namespace_id = &tx_context.namespace_id;
        let collection = &get_state_by_range.collection;
        let query_result = if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger.get_private_data_range(
                namespace_id,
                collection,
                &get_state_by_range.start_key,
                &get_state_by_range.end_key,
            )
        } else {
            self.ledger.get_state_range(
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
        Err(Error::Unsupported("pagination query is not supported yet"))
    }

    // since we don't support pagination, this msg is unexpected
    async fn handle_query_state_close(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported("pagination query is not supported yet"))
    }

    async fn handle_get_query_result(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported("get_query_result is not supported yet"))
    }

    async fn handle_get_history_for_key(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
        let get_history_for_key = pb::GetHistoryForKey::decode(&msg.payload[..])?;
        let namespace_id = &tx_context.namespace_id;

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
        let namespace_id = &tx_context.namespace_id;
        let collection = &put_state.collection;
        if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .set_private_data(namespace_id, collection, &put_state.key, put_state.value);
        } else {
            self.ledger
                .set_state(namespace_id, &msg.txid, &put_state.key, put_state.value);
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
        let namespace_id = &tx_context.namespace_id;
        let collection = &put_state_metadata.collection;
        if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger.set_private_data_metadata(
                namespace_id,
                collection,
                &put_state_metadata.key,
                metadata,
            );
        } else {
            self.ledger
                .set_state_metadata(namespace_id, &put_state_metadata.key, metadata);
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
        let namespace_id = &tx_context.namespace_id;
        let collection = &del_state.collection;
        if !collection.is_empty() {
            if tx_context.is_init {
                return Err(Error::InvalidOperation(
                    "private data APIs are not allowed in chaincode Init()",
                ));
            }
            // TODO: check permission
            self.ledger
                .delete_private_data(namespace_id, collection, &del_state.key);
        } else {
            self.ledger
                .delete_state(namespace_id, &msg.txid, &del_state.key);
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

    async fn handle_invoke_chaincode(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported("invoke chaincode is not supported yet"))
    }

    async fn handle_completed(&mut self, msg: ChaincodeMessage) -> Result<()> {
        // TODO
        // 1. apply rwset
        // 2. notify
        // 3. clear tx ctx
        info!("tx completed: {:?}", msg);
        self.contexts.remove(&msg.txid);
        Ok(())
    }

    async fn handle_error(&mut self, msg: ChaincodeMessage) -> Result<()> {
        // TODO
        warn!("error: {:?}", msg);
        self.contexts.remove(&msg.txid);
        Ok(())
    }

    async fn handle_keepalive(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Ok(())
    }

    async fn handle_chaincode_msg(&mut self, msg: ChaincodeMessage) -> Result<()> {
        // TODO: register Txid to prevent overlapping handle messages from chaincode
        // let tx_context = self.contexts.get(&ctx_id(&msg)).expect("context no found");
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
            Error => self.handle_error(msg).await,
            Completed => self.handle_completed(msg).await,
            _unexpected => Err(super::error::Error::InvalidChaincodeMsg(msg)),
        }
    }

    // async fn handle_chaincode_msg(&mut self, msg: ChaincodeMessage) -> Result<()> {
    //     match ChaincodeMsgType::from_i32(msg.r#type) {
    //         Some(ty) => {
    //             match ty {
    //                 ChaincodeMsgType::Register => {
    //                     let cc_name = String::from_utf8(msg.payload).unwrap();
    //                     info!("Chaincode `{}` registered.", cc_name);
    //                     let registered_resp = ChaincodeMessage {
    //                         r#type: ChaincodeMsgType::Registered as i32,
    //                         ..Default::default()
    //                     };
    //                     self.cc_side.send(registered_resp).await.unwrap();

    //                     let ready_req = ChaincodeMessage {
    //                         r#type: ChaincodeMsgType::Ready as i32,
    //                         ..Default::default()
    //                     };
    //                     self.cc_side.send(ready_req).await.unwrap();

    //                     // let args = vec!["InitLedger".as_bytes().to_vec()];
    //                     // let input = ChaincodeInput {
    //                     //     args,
    //                     //     decorations: HashMap::new(),
    //                     //     is_init: false,
    //                     // };
    //                     // let payload = input.dump();
    //                     // let init_req = ChaincodeMessage {
    //                     //     r#type: ChaincodeMsgType::Init as i32,
    //                     //     payload,
    //                     //     ..Default::default()
    //                     // };
    //                     // self.cc_side.send(init_req).await;
    //                 }
    //                 ChaincodeMsgType::GetState => {
    //                     // info!(&self.ledger);
    //                     let get_state = GetState::decode(&msg.payload[..]).unwrap();
    //                     let key = get_state.key;
    //                     info!("key {}", &key);
    //                     let value = self.ledger.entry(key).or_default();
    //                     info!("len {}", &value.len());

    //                     let resp = ChaincodeMessage {
    //                         r#type: ChaincodeMsgType::Response as i32,
    //                         payload: value.clone(),
    //                         ..Default::default()
    //                     };
    //                     self.cc_side.send(resp).await.unwrap();
    //                 }
    //                 ChaincodeMsgType::PutState => {
    //                     let put_state = PutState::decode(&msg.payload[..]).unwrap();

    //                     let key = put_state.key;
    //                     info!("key {}", &key);
    //                     let value = put_state.value;
    //                     info!("len {}", &value.len());
    //                     self.ledger.insert(key, value);

    //                     let resp = ChaincodeMessage {
    //                         r#type: ChaincodeMsgType::Response as i32,
    //                         ..Default::default()
    //                     };
    //                     self.cc_side.send(resp).await.unwrap();
    //                 }
    //                 ChaincodeMsgType::GetStateByRange => {
    //                     let get_state_by_range = GetStateByRange::decode(&msg.payload[..]).unwrap();
    //                     let start_key = get_state_by_range.start_key;
    //                     let end_key = get_state_by_range.end_key;
    //                     let results = match (start_key.as_str(), end_key.as_str()) {
    //                         ("", "") => self.ledger.range::<String, _>(..),
    //                         (start_key, "") => self.ledger.range(start_key.to_owned()..),
    //                         (start_key, end_key) => {
    //                             self.ledger.range(start_key.to_owned()..end_key.to_owned())
    //                         }
    //                     };
    //                     let results = results
    //                         .map(|r| {
    //                             let query_result = Kv {
    //                                 key: r.0.clone(),
    //                                 value: r.1.clone(),
    //                                 ..Default::default()
    //                             };
    //                             QueryResultBytes {
    //                                 result_bytes: query_result.dump(),
    //                             }
    //                         })
    //                         .collect::<Vec<_>>();

    //                     let query_resp = QueryResponse {
    //                         results,
    //                         // metadata: meta_payload,
    //                         has_more: false,
    //                         ..Default::default()
    //                     };

    //                     let payload = query_resp.dump();
    //                     let resp = ChaincodeMessage {
    //                         r#type: ChaincodeMsgType::Response as i32,
    //                         payload,
    //                         ..Default::default()
    //                     };
    //                     self.cc_side.send(resp).await.unwrap();
    //                 }
    //                 ChaincodeMsgType::QueryStateClose => {
    //                     let resp = ChaincodeMessage {
    //                         r#type: ChaincodeMsgType::Response as i32,
    //                         ..Default::default()
    //                     };
    //                     self.cc_side.send(resp).await.unwrap();
    //                 }
    //                 _unknown => info!("recv unknown msg: `{:?}`", msg),
    //             }
    //         }
    //         None => return Err(Error::Unimplemented),
    //     }
    //     Ok(())
    // }

    async fn handle_executor_cmd(&mut self, cmd: ExecutorCommand) -> Result<()> {
        let req = ChaincodeMessage {
            r#type: ChaincodeMsgType::Transaction as i32,
            payload: cmd.payload,
            // txid: String::from_utf8_lossy(&cmd.tx_hash[..]).to_string(),
            // channel_id: "123456789".to_string(),
            ..Default::default()
        };
        self.cc_side.send(req).await.unwrap();

        // match cmd {
        //     ExecutorCommand::Create => {
        //         let args = vec![
        //             "CreateAsset",
        //             "asset8",
        //             "blue",
        //             "16",
        //             "Kelly",
        //             "750"
        //         ].iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        //         let input = ChaincodeInput {
        //             args,
        //             decorations: HashMap::new(),
        //             is_init: false,
        //         };
        //         let payload = input.dump();
        //         let req = ChaincodeMessage {
        //             r#type: ChaincodeMsgType::Transaction as i32,
        //             payload,
        //             ..Default::default()
        //         };
        //         self.cc_side.send(req).await;
        //     }
        //     ExecutorCommand::Read => {
        //         let args = vec![
        //             "ReadAsset",
        //             "asset8",
        //         ].iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        //         let input = ChaincodeInput {
        //             args,
        //             decorations: HashMap::new(),
        //             is_init: false,
        //         };
        //         let payload = input.dump();
        //         let req = ChaincodeMessage {
        //             r#type: ChaincodeMsgType::Transaction as i32,
        //             payload,
        //             ..Default::default()
        //         };
        //         self.cc_side.send(req).await;
        //     }
        //     ExecutorCommand::GetAll => {
        //         let args = vec![
        //             "GetAllAssets",
        //         ].iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        //         let input = ChaincodeInput {
        //             args,
        //             decorations: HashMap::new(),
        //             is_init: false,
        //         };
        //         let payload = input.dump();
        //         let req = ChaincodeMessage {
        //             r#type: ChaincodeMsgType::Transaction as i32,
        //             payload,
        //             ..Default::default()
        //         };
        //         self.cc_side.send(req).await;
        //     }
        //     ExecutorCommand::Transfer => {
        //         let args = vec![
        //             "TransferAsset",
        //             "asset1",
        //             "Alice",
        //         ].iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        //         let input = ChaincodeInput {
        //             args,
        //             decorations: HashMap::new(),
        //             is_init: false,
        //         };
        //         let payload = input.dump();
        //         let req = ChaincodeMessage {
        //             r#type: ChaincodeMsgType::Transaction as i32,
        //             payload,
        //             ..Default::default()
        //         };
        //         self.cc_side.send(req).await;
        //     }
        // }
        Ok(())
    }
}

fn ctx_id(msg: &ChaincodeMessage) -> String {
    let channel_id = &msg.channel_id;
    let txid = &msg.txid;
    format!("{}/{}", channel_id, txid)
}
