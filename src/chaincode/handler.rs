use super::ExecutorCommand;
use super::MessageDump;
use super::Task;
use crate::protos::chaincode_message::Type as ChaincodeMsgType;
use crate::protos::ChaincodeInput;
use crate::protos::ChaincodeMessage;
use crate::protos::GetState;
use crate::protos::GetStateByRange;
use crate::protos::PutState;
use crate::protos::QueryResponse;
use crate::protos::QueryResultBytes;
use crate::queryresult::Kv;
use futures::channel::mpsc;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use futures::{future, stream};
use std::collections::BTreeMap;

use prost::Message;

use super::error::ChaincodeError;
use log::{debug, info};

#[derive(Debug)]
pub struct Handler {
    cc_side: mpsc::Sender<ChaincodeMessage>,
    ledger: BTreeMap<String, Vec<u8>>,
}

pub struct Registry {
    pub name: String,
    pub handle: mpsc::Sender<Task>,
    pub resp_rx: mpsc::Receiver<ChaincodeMessage>,
}

impl Handler {
    pub async fn new<T>(mut cc_stream: T) -> Result<Registry, ChaincodeError>
    where
        T: Stream<Item = Result<ChaincodeMessage, tonic::Status>> + Unpin + Send + Sync + 'static,
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
                    ledger: BTreeMap::new(),
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
                                handler.handle_chaincode_msg(msg).await.unwrap()
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
                Err(ChaincodeError::Unimplemented)
            }
        } else {
            Err(ChaincodeError::Unimplemented)
        }
    }

    // pub fn new(task_flow: mpsc::Receiver<Task>, cc_side: mpsc::Sender<ChaincodeMessage>) -> Self {
    //     Self {
    //         task_flow,
    //         cc_side,
    //         ledger: BTreeMap::new(),
    //     }
    // }

    async fn handle_chaincode_msg(&mut self, msg: ChaincodeMessage) -> Result<(), ChaincodeError> {
        match ChaincodeMsgType::from_i32(msg.r#type) {
            Some(ty) => {
                match ty {
                    ChaincodeMsgType::Register => {
                        let cc_name = String::from_utf8(msg.payload).unwrap();
                        info!("Chaincode `{}` registered.", cc_name);
                        let registered_resp = ChaincodeMessage {
                            r#type: ChaincodeMsgType::Registered as i32,
                            ..Default::default()
                        };
                        self.cc_side.send(registered_resp).await.unwrap();

                        let ready_req = ChaincodeMessage {
                            r#type: ChaincodeMsgType::Ready as i32,
                            ..Default::default()
                        };
                        self.cc_side.send(ready_req).await.unwrap();

                        // let args = vec!["InitLedger".as_bytes().to_vec()];
                        // let input = ChaincodeInput {
                        //     args,
                        //     decorations: HashMap::new(),
                        //     is_init: false,
                        // };
                        // let payload = input.dump();
                        // let init_req = ChaincodeMessage {
                        //     r#type: ChaincodeMsgType::Init as i32,
                        //     payload,
                        //     ..Default::default()
                        // };
                        // self.cc_side.send(init_req).await;
                    }
                    ChaincodeMsgType::GetState => {
                        // info!(&self.ledger);
                        let get_state = GetState::decode(&msg.payload[..]).unwrap();
                        let key = get_state.key;
                        info!("key {}", &key);
                        let value = self.ledger.entry(key).or_default();
                        info!("len {}", &value.len());

                        let resp = ChaincodeMessage {
                            r#type: ChaincodeMsgType::Response as i32,
                            payload: value.clone(),
                            ..Default::default()
                        };
                        self.cc_side.send(resp).await.unwrap();
                    }
                    ChaincodeMsgType::PutState => {
                        let put_state = PutState::decode(&msg.payload[..]).unwrap();

                        let key = put_state.key;
                        info!("key {}", &key);
                        let value = put_state.value;
                        info!("len {}", &value.len());
                        self.ledger.insert(key, value);

                        let resp = ChaincodeMessage {
                            r#type: ChaincodeMsgType::Response as i32,
                            ..Default::default()
                        };
                        self.cc_side.send(resp).await.unwrap();
                    }
                    ChaincodeMsgType::GetStateByRange => {
                        let get_state_by_range = GetStateByRange::decode(&msg.payload[..]).unwrap();
                        let start_key = get_state_by_range.start_key;
                        let end_key = get_state_by_range.end_key;
                        let results = match (start_key.as_str(), end_key.as_str()) {
                            ("", "") => self.ledger.range::<String, _>(..),
                            (start_key, "") => self.ledger.range(start_key.to_owned()..),
                            (start_key, end_key) => {
                                self.ledger.range(start_key.to_owned()..end_key.to_owned())
                            }
                        };
                        let results = results
                            .map(|r| {
                                let query_result = Kv {
                                    key: r.0.clone(),
                                    value: r.1.clone(),
                                    ..Default::default()
                                };
                                QueryResultBytes {
                                    result_bytes: query_result.dump(),
                                }
                            })
                            .collect::<Vec<_>>();

                        let query_resp = QueryResponse {
                            results,
                            // metadata: meta_payload,
                            has_more: false,
                            ..Default::default()
                        };

                        let payload = query_resp.dump();
                        let resp = ChaincodeMessage {
                            r#type: ChaincodeMsgType::Response as i32,
                            payload,
                            ..Default::default()
                        };
                        self.cc_side.send(resp).await.unwrap();
                    }
                    ChaincodeMsgType::QueryStateClose => {
                        let resp = ChaincodeMessage {
                            r#type: ChaincodeMsgType::Response as i32,
                            ..Default::default()
                        };
                        self.cc_side.send(resp).await.unwrap();
                    }
                    _unknown => info!("recv unknown msg: `{:?}`", msg),
                }
            }
            None => return Err(ChaincodeError::Unimplemented),
        }
        Ok(())
    }

    async fn handle_executor_cmd(&mut self, cmd: ExecutorCommand) -> Result<(), ChaincodeError> {
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
