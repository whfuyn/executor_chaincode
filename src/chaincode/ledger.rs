use crate::protos::StateMetadata;
use crate::queryresult::KeyModification;
use crate::queryresult::Kv;
use std::collections::BTreeMap;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Ledger {
    state_store: BTreeMap<String, Vec<u8>>,
    state_metadata_store: BTreeMap<String, HashMap<String, Vec<u8>>>,
    state_history: HashMap<String, Vec<KeyModification>>,
    private_store: BTreeMap<String, Vec<u8>>,
    private_hash_store: BTreeMap<String, Vec<u8>>,
    private_metadata_store: BTreeMap<String, HashMap<String, Vec<u8>>>,
}

impl Ledger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_state(&mut self, namespace: &str, key: &str) -> Option<&Vec<u8>> {
        let state_key = make_key(namespace, key);
        self.state_store.get(&state_key)
    }

    pub fn get_state_metadata(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> Option<&HashMap<String, Vec<u8>>> {
        let state_key = make_key(namespace, key);
        self.state_metadata_store.get(&state_key)
    }

    pub fn get_state_range<'this: 'ret, 'ret>(
        &'this self,
        namespace: &str,
        start_key: &str,
        end_key: &str,
    ) -> Box<dyn Iterator<Item = Kv> + 'ret> {
        let sk = make_key(namespace, start_key);
        let ek = make_key(namespace, end_key);
        let iter = match (start_key, end_key) {
            ("", "") => self.state_store.range::<String, _>(..),
            (_start, "") => self.state_store.range(sk..),
            ("", _end) => self.state_store.range(..ek),
            (_start, _end) => self.state_store.range(sk..ek),
        };
        Box::new(iter.map(|(k, v)| Kv {
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }))
    }

    pub fn get_history_for_key<'this: 'ret, 'ret>(
        &'this self,
        namespace: &str,
        key: &str,
    ) -> Box<dyn Iterator<Item = KeyModification> + 'ret> {
        let state_key = make_key(namespace, key);
        Box::new(
            self.state_history
                .get(&state_key)
                .map(|h| h.iter())
                .into_iter()
                .flatten()
                .cloned(),
        )
    }

    pub fn get_private_data(
        &mut self,
        namespace: &str,
        collection: &str,
        key: &str,
    ) -> Option<&Vec<u8>> {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_store.get(&private_data_key)
    }

    pub fn get_private_data_hash(
        &mut self,
        namespace: &str,
        collection: &str,
        key: &str,
    ) -> Option<&Vec<u8>> {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_hash_store.get(&private_data_key)
    }

    pub fn get_private_data_metadata(
        &self,
        namespace: &str,
        collection: &str,
        key: &str,
    ) -> Option<&HashMap<String, Vec<u8>>> {
        let private_data_key = make_collection_key(&namespace, collection, key);
        self.private_metadata_store.get(&private_data_key)
    }

    pub fn get_private_data_range<'this: 'ret, 'ret>(
        &'this self,
        namespace: &str,
        collection: &str,
        start_key: &str,
        end_key: &str,
    ) -> Box<dyn Iterator<Item = Kv> + 'ret> {
        let sk = make_collection_key(namespace, collection, start_key);
        let ek = make_collection_key(namespace, collection, end_key);
        let iter = match (start_key, end_key) {
            ("", "") => self.private_store.range::<String, _>(..),
            (_start, "") => self.private_store.range(sk..),
            ("", _end) => self.private_store.range(..ek),
            (_start, _end) => self.private_store.range(sk..ek),
        };
        Box::new(iter.map(|(k, v)| Kv {
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }))
    }

    pub fn set_state(&mut self, namespace: &str, tx_id: &str, key: &str, value: Vec<u8>) {
        let state_key = make_key(namespace, key);
        self.state_store.insert(state_key.clone(), value.clone());
        let km = KeyModification {
            tx_id: tx_id.to_owned(),
            value,
            timestamp: get_timestamp(),
            is_delete: false,
        };
        self.state_history.entry(state_key).or_default().push(km);
    }

    pub fn set_state_metadata(&mut self, namespace: &str, key: &str, metadata: StateMetadata) {
        let state_key = make_key(namespace, key);
        let StateMetadata { metakey, value } = metadata;
        self.state_metadata_store
            .entry(state_key)
            .or_default()
            .insert(metakey, value);
    }

    pub fn set_private_data(
        &mut self,
        namespace: &str,
        collection: &str,
        key: &str,
        value: Vec<u8>,
    ) {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_store.insert(private_data_key, value);
    }

    pub fn set_private_data_metadata(
        &mut self,
        namespace: &str,
        collection: &str,
        key: &str,
        metadata: StateMetadata,
    ) {
        let private_data_key = make_collection_key(namespace, collection, key);
        let StateMetadata { metakey, value } = metadata;
        self.private_metadata_store
            .entry(private_data_key)
            .or_default()
            .insert(metakey, value);
    }

    // TODO: maybe delete other related data?
    pub fn delete_state(&mut self, namespace: &str, tx_id: &str, key: &str) {
        let state_key = make_key(namespace, key);
        self.state_store.remove(&state_key);
        let km = KeyModification {
            tx_id: tx_id.to_owned(),
            value: vec![],
            timestamp: get_timestamp(),
            is_delete: true,
        };
        self.state_history.entry(state_key).or_default().push(km);
    }

    // TODO: maybe delete other related data?
    pub fn delete_private_data(&mut self, namespace: &str, collection: &str, key: &str) {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_store.remove(&private_data_key);
    }
}

fn make_key(namespace: &str, key: &str) -> String {
    format!("{}/{}", namespace, key)
}

fn make_collection_key(namespace: &str, collection: &str, key: &str) -> String {
    format!("{}/{}/{}", namespace, collection, key)
}

fn get_timestamp() -> Option<prost_types::Timestamp> {
    use std::convert::TryFrom;
    use std::time::SystemTime;
    let now = SystemTime::now();
    prost_types::Timestamp::try_from(now).ok()
}
