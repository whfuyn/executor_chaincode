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
    pub tx_id: String,
    pub is_init: bool,
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
                    cc_name: cc_name.clone(),
                    cc_side,
                    ledger: Ledger::new(),
                    contexts: HashMap::new(),
                    total_query_limit: 65536, // TODO: support pagination
                    nonce: 0,
                };

                tokio::spawn(async move {
                    let cc_stream = cc_stream.filter_map(|res| match res {
                        Ok(msg) => future::ready(Some(Task::Chaincode(msg))),
                        Err(e) => {
                            info!("Chaincode stream error: `{:?}`", e);
                            panic!("chaincode is down")
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
        info!("query_close_close, txid: {}", &msg.txid);
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

    // This msg is to execute queries on db, which is not supported
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
        // dbg!(&put_state.key);
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
        let namespace_id = &self.cc_name;
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

    // A chaincode may call other chaincode during the execution of a transaction,
    // it's not supported yet.
    async fn handle_invoke_chaincode(&mut self, _msg: ChaincodeMessage) -> Result<()> {
        Err(Error::Unsupported("invoke chaincode is not supported yet"))
    }

    async fn handle_completed(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let resp = pb::Response::decode(&msg.payload[..])?;
        self.contexts.remove(&msg.txid);
        let result = String::from_utf8_lossy(&resp.payload);
        info!("tx completed with msg: {}", resp.message);
        println!("tx completed with msg: {}", resp.message);
        info!("tx completed with result: {}", result);
        println!("tx completed with result: {}", result);
        Ok(())
    }

    async fn handle_error(&mut self, msg: ChaincodeMessage) -> Result<()> {
        let resp = pb::Response::decode(&msg.payload[..])?;
        self.contexts.remove(&msg.txid);
        let result = String::from_utf8_lossy(&resp.payload);
        info!("tx error with result: {}", &result);
        println!("tx error with result: {}", &result);
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
            Error => self.handle_error(msg).await,
            Completed => self.handle_completed(msg).await,
            _unexpected => Err(super::error::Error::InvalidChaincodeMsg(msg)),
        }
    }

    async fn handle_executor_cmd(&mut self, cmd: ExecutorCommand) -> Result<()> {
        let msg = ChaincodeMessage::decode(&cmd.payload[..])?;
        let ctx = TransactionContext {
            channel_id: msg.channel_id.clone(),
            tx_id: msg.txid.clone(),
            is_init: false,
        };
        self.contexts.insert(ctx_id(&msg), ctx);
        self.cc_side.send(msg).await.unwrap();

        Ok(())
    }
}

//     async fn handle_executor_cmd(&mut self, cmd: ExecutorCommand) -> Result<()> {
//         let namespace_id = "cita-cloud".to_string();
//         let channel_id = "123456789".to_string();
//         let tx_id = String::from_utf8_lossy(&cmd.tx_hash[..]).to_string();

//         let signed_proposal = {
//             let mspid = "cita-cloud".to_string();
//             // this cert is from fabric-samples, peer0.org1.example.com-cert.pem
//             let cert = "-----BEGIN CERTIFICATE-----
// MIICJzCCAc6gAwIBAgIQKxBV8QdNKmtS2wu7DExPWzAKBggqhkjOPQQDAjBzMQsw
// CQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZy
// YW5jaXNjbzEZMBcGA1UEChMQb3JnMS5leGFtcGxlLmNvbTEcMBoGA1UEAxMTY2Eu
// b3JnMS5leGFtcGxlLmNvbTAeFw0yMDEwMTIwODI5MDBaFw0zMDEwMTAwODI5MDBa
// MGoxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9ybmlhMRYwFAYDVQQHEw1T
// YW4gRnJhbmNpc2NvMQ0wCwYDVQQLEwRwZWVyMR8wHQYDVQQDExZwZWVyMC5vcmcx
// LmV4YW1wbGUuY29tMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEMutVyQ9OX0Ui
// 29Cn/E4+eq3SZl1LlSlqMNDup5KQqo9lVY2CKcNuWeKeV+YoDijQRPTLW7o2ZDuJ
// yn7ZvtOBXaNNMEswDgYDVR0PAQH/BAQDAgeAMAwGA1UdEwEB/wQCMAAwKwYDVR0j
// BCQwIoAg6WZDnHPhiJpYBVNBJTwE0YW45ThbtJt7qhk7WivY+AIwCgYIKoZIzj0E
// AwIDRwAwRAIgDNvR3C6j+SVncmmr0GvcomW3j3SqbQ4toRRMOiRa56ICIHHcMiAM
// S4u7BSot5a2st7igwkukLRk2e5TwFhECcZDA
// -----END CERTIFICATE-----
// ";
//             let signature_header = SignatureHeader {
//                 mspid,
//                 id_bytes: cert.as_bytes().to_vec(),
//                 nonce: self.nonce.to_be_bytes().to_vec(),
//             };
//             let channel_header = ChannelHeader {
//                 channel_id: channel_id.clone(),
//                 timestamp: get_timestamp(),
//                 tx_id: tx_id.clone(),
//             };
//             let header = Header {
//                 channel_header,
//                 signature_header,
//             };
//             let transient_map = {
//                 let mut map = HashMap::new();
//                 map.insert(
//                     "asset_properties".to_string(),
//                     "eyJvYmplY3RfdHlwZSI6ImFzc2V0X3Byb3BlcnRpZXMiLCJhc3NldF9pZCI6ImFzc2V0MSIsImNvbG9yIjoiYmx1ZSIsInNpemUiOjM1LCJzYWx0IjoiYTk0YThmZTVjY2IxOWJhNjFjNGMwODczZDM5MWU5ODc5ODJmYmJkMyJ9"
//                     .as_bytes().to_vec()
//                 );
//                 map
//             };
//             let proposal = Proposal {
//                 header,
//                 input: cmd.payload.clone(),
//                 transient_map,
//             };
//             SignedProposal {
//                 proposal,
//                 signature: vec![1, 2, 3, 4, 5, 6, 7],
//             }
//         };
//         let ctx = TransactionContext {
//             namespace_id,
//             channel_id: channel_id.clone(),
//             is_init: false,
//         };
//         let req = ChaincodeMessage {
//             r#type: ChaincodeMsgType::Transaction as i32,
//             payload: cmd.payload,
//             txid: tx_id,
//             channel_id,
//             proposal: Some(signed_proposal.get_protos_proposal()),
//             ..Default::default()
//         };
//         self.contexts.insert(ctx_id(&req), ctx);
//         self.cc_side.send(req).await.unwrap();

//         Ok(())
//     }
// }

fn ctx_id(msg: &ChaincodeMessage) -> String {
    let channel_id = &msg.channel_id;
    let txid = &msg.txid;
    format!("{}/{}", channel_id, txid)
}
