use super::get_timestamp;
use crate::chaincode::MessageDump;
use crate::protos::StateMetadata;
use crate::queryresult::KeyModification;
use crate::queryresult::Kv;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use prost::Message;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use tokio::fs;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const STATE_FILE_NAME: &str = "state.data";
const STATE_METADATA_FILE_NAME: &str = "state_metadata.data";
const STATE_HISTORY_FILE_NAME: &str = "state_history.data";

const PRIVATE_FILE_NAME: &str = "private.data";
const PRIVATE_HASH_FILE_NAME: &str = "private_hash.data";
const PRIVATE_DATA_METADATA_FILE_NAME: &str = "private_data_metadata.data";

#[derive(Debug)]
pub struct Ledger {
    state_store: BTreeMap<String, Vec<u8>>,
    state_metadata_store: BTreeMap<String, HashMap<String, Vec<u8>>>,
    state_history: HashMap<String, Vec<Vec<u8>>>,

    private_store: BTreeMap<String, Vec<u8>>,
    private_hash_store: BTreeMap<String, Vec<u8>>,
    private_data_metadata_store: BTreeMap<String, HashMap<String, Vec<u8>>>,

    // files
    state_file: File,
    state_metadata_file: File,
    state_history_file: File,

    private_file: File,
    private_hash_file: File,
    private_data_metadata_file: File,
}

impl Ledger {
    // For now, this is only used by test
    #[cfg(test)]
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        Self::open(data_dir, true).await
    }

    pub async fn load<P: AsRef<Path>>(data_dir: P) -> Self {
        Self::open(data_dir, false).await
    }

    async fn open<P: AsRef<Path>>(data_dir: P, truncate: bool) -> Self {
        let path = data_dir.as_ref();
        if !path.exists() {
            fs::create_dir_all(path).await.unwrap();
        }
        let (state_store, state_file) = Self::load_state(path, truncate).await;
        let (state_metadata_store, state_metadata_file) =
            Self::load_state_metadata(path, truncate).await;
        let (state_history, state_history_file) = Self::load_state_history(path, truncate).await;

        let (private_store, private_file) = Self::load_private(path, truncate).await;
        let (private_hash_store, private_hash_file) = Self::load_private_hash(path, truncate).await;
        let (private_data_metadata_store, private_data_metadata_file) =
            Self::load_private_data_metadata(path, truncate).await;

        Self {
            state_store,
            state_metadata_store,
            state_history,

            private_store,
            private_hash_store,
            private_data_metadata_store,

            state_file,
            state_metadata_file,
            state_history_file,

            private_file,
            private_hash_file,
            private_data_metadata_file,
        }
    }

    async fn load_file<T, P>(path: P, truncate: bool) -> (T, File)
    where
        T: Default + DeserializeOwned,
        P: AsRef<Path>,
    {
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(path)
            .await
            .expect("open ledger file error");
        let mut buf = vec![];
        f.read_to_end(&mut buf).await.expect("read file failed");
        if buf.is_empty() {
            (T::default(), f)
        } else {
            (
                bincode::deserialize(&buf[..]).expect("deserialize failed"),
                f,
            )
        }
    }

    // TODO: those operations can be done concurrently.
    pub async fn sync(&mut self) {
        self.sync_state().await;
        self.sync_state_metadata().await;
        self.sync_state_history().await;
        self.sync_private().await;
        self.sync_private_data_metadata().await;
        self.sync_private_hash().await;
    }

    async fn sync_state(&mut self) {
        let encoded = bincode::serialize(&self.state_store).expect("serialize failed");
        self.state_file.seek(SeekFrom::Start(0)).await.unwrap();
        self.state_file.write_all(&encoded[..]).await.unwrap();
        self.state_file.sync_data().await.unwrap();
    }

    async fn sync_state_metadata(&mut self) {
        let encoded = bincode::serialize(&self.state_metadata_store).expect("serialize failed");
        self.state_metadata_file
            .seek(SeekFrom::Start(0))
            .await
            .unwrap();
        self.state_metadata_file
            .write_all(&encoded[..])
            .await
            .unwrap();
        self.state_metadata_file.sync_data().await.unwrap();
    }

    async fn sync_state_history(&mut self) {
        let encoded = bincode::serialize(&self.state_history).expect("serialize failed");
        self.state_history_file
            .seek(SeekFrom::Start(0))
            .await
            .unwrap();
        self.state_history_file
            .write_all(&encoded[..])
            .await
            .unwrap();
        self.state_history_file.sync_data().await.unwrap();
    }

    async fn sync_private(&mut self) {
        let encoded = bincode::serialize(&self.private_store).expect("serialize failed");
        self.private_file.seek(SeekFrom::Start(0)).await.unwrap();
        self.private_file.write_all(&encoded[..]).await.unwrap();
        self.private_file.sync_data().await.unwrap();
    }

    async fn sync_private_hash(&mut self) {
        let encoded = bincode::serialize(&self.private_hash_store).expect("serialize failed");
        self.private_hash_file
            .seek(SeekFrom::Start(0))
            .await
            .unwrap();
        self.private_hash_file
            .write_all(&encoded[..])
            .await
            .unwrap();
        self.private_hash_file.sync_data().await.unwrap();
    }

    async fn sync_private_data_metadata(&mut self) {
        let encoded =
            bincode::serialize(&self.private_data_metadata_store).expect("serialize failed");
        self.private_data_metadata_file
            .seek(SeekFrom::Start(0))
            .await
            .unwrap();
        self.private_data_metadata_file
            .write_all(&encoded[..])
            .await
            .unwrap();
        self.private_data_metadata_file.sync_data().await.unwrap();
    }

    async fn load_state<P: AsRef<Path>>(
        data_dir: P,
        truncate: bool,
    ) -> (BTreeMap<String, Vec<u8>>, File) {
        let path = data_dir.as_ref().join(STATE_FILE_NAME);
        Self::load_file(path, truncate).await
    }

    async fn load_state_metadata<P: AsRef<Path>>(
        data_dir: P,
        truncate: bool,
    ) -> (BTreeMap<String, HashMap<String, Vec<u8>>>, File) {
        let path = data_dir.as_ref().join(STATE_METADATA_FILE_NAME);
        Self::load_file(path, truncate).await
    }

    async fn load_state_history<P: AsRef<Path>>(
        data_dir: P,
        truncate: bool,
    ) -> (HashMap<String, Vec<Vec<u8>>>, File) {
        let path = data_dir.as_ref().join(STATE_HISTORY_FILE_NAME);
        Self::load_file(path, truncate).await
    }

    async fn load_private<P: AsRef<Path>>(
        data_dir: P,
        truncate: bool,
    ) -> (BTreeMap<String, Vec<u8>>, File) {
        let path = data_dir.as_ref().join(PRIVATE_FILE_NAME);
        Self::load_file(path, truncate).await
    }

    async fn load_private_hash<P: AsRef<Path>>(
        data_dir: P,
        truncate: bool,
    ) -> (BTreeMap<String, Vec<u8>>, File) {
        let path = data_dir.as_ref().join(PRIVATE_HASH_FILE_NAME);
        Self::load_file(path, truncate).await
    }

    async fn load_private_data_metadata<P: AsRef<Path>>(
        data_dir: P,
        truncate: bool,
    ) -> (BTreeMap<String, HashMap<String, Vec<u8>>>, File) {
        let path = data_dir.as_ref().join(PRIVATE_DATA_METADATA_FILE_NAME);
        Self::load_file(path, truncate).await
    }

    pub fn get_state(&self, namespace: &str, key: &str) -> Option<&Vec<u8>> {
        let state_key = make_key(namespace, key);
        self.state_store.get(&state_key)
    }

    pub fn get_state_metadata(
        &self,
        namespace: &str,
        key: &str,
    ) -> Option<&HashMap<String, Vec<u8>>> {
        let state_key = make_key(namespace, key);
        self.state_metadata_store.get(&state_key)
    }

    pub fn get_state_by_range<'this: 'ret, 'ret>(
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
        Box::new(iter.map(|(k, v)| {
            let mut tokens = k.split('/');
            Kv {
                namespace: tokens.next().unwrap().to_string(),
                key: tokens.next().unwrap().to_string(),
                value: v.clone(),
            }
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
                .map(|h| KeyModification::decode(&h[..]).unwrap()),
        )
    }

    pub fn get_private_data(
        &self,
        namespace: &str,
        collection: &str,
        key: &str,
    ) -> Option<&Vec<u8>> {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_store.get(&private_data_key)
    }

    pub fn get_private_data_hash(
        &self,
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
        self.private_data_metadata_store.get(&private_data_key)
    }

    pub fn get_private_data_by_range<'this: 'ret, 'ret>(
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
        Box::new(iter.map(|(k, v)| {
            let mut tokens = k.split('/');
            Kv {
                namespace: tokens.next().unwrap().to_string(),
                key: tokens.nth(1).unwrap().to_string(),
                value: v.clone(),
            }
        }))
    }

    pub async fn set_state(&mut self, namespace: &str, tx_id: &str, key: &str, value: Vec<u8>) {
        let state_key = make_key(namespace, key);
        self.state_store.insert(state_key.clone(), value.clone());
        let km = KeyModification {
            tx_id: tx_id.to_owned(),
            value,
            timestamp: get_timestamp(),
            is_delete: false,
        };
        self.state_history
            .entry(state_key)
            .or_default()
            .push(km.dump());
    }

    pub async fn set_state_metadata(
        &mut self,
        namespace: &str,
        key: &str,
        metadata: StateMetadata,
    ) {
        let state_key = make_key(namespace, key);
        let StateMetadata { metakey, value } = metadata;
        self.state_metadata_store
            .entry(state_key)
            .or_default()
            .insert(metakey, value);
    }

    pub async fn set_private_data(
        &mut self,
        namespace: &str,
        collection: &str,
        key: &str,
        value: Vec<u8>,
    ) {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_store
            .insert(private_data_key.clone(), value.clone());
        let hash = compute_private_data_hash(&value[..]);
        self.private_hash_store.insert(private_data_key, hash);
    }

    pub async fn set_private_data_metadata(
        &mut self,
        namespace: &str,
        collection: &str,
        key: &str,
        metadata: StateMetadata,
    ) {
        let private_data_key = make_collection_key(namespace, collection, key);
        let StateMetadata { metakey, value } = metadata;
        self.private_data_metadata_store
            .entry(private_data_key)
            .or_default()
            .insert(metakey, value);
    }

    // TODO: should I delete metadata associated with this key?
    pub async fn delete_state(&mut self, namespace: &str, tx_id: &str, key: &str) {
        let state_key = make_key(namespace, key);
        self.state_store.remove(&state_key);
        let km = KeyModification {
            tx_id: tx_id.to_owned(),
            value: vec![],
            timestamp: get_timestamp(),
            is_delete: true,
        };
        self.state_history
            .entry(state_key)
            .or_default()
            .push(km.dump());
    }

    // TODO: should I delete metadata and hash associated with this key?
    pub async fn delete_private_data(&mut self, namespace: &str, collection: &str, key: &str) {
        let private_data_key = make_collection_key(namespace, collection, key);
        self.private_store.remove(&private_data_key);
        self.private_data_metadata_store.remove(&private_data_key);
        self.private_hash_store.remove(&private_data_key);
    }
}

fn make_key(namespace: &str, key: &str) -> String {
    format!("{}/{}", namespace, key)
}

fn make_collection_key(namespace: &str, collection: &str, key: &str) -> String {
    format!("{}/{}/{}", namespace, collection, key)
}

fn compute_private_data_hash(value: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.input(value);
    let value_hash = hasher.result_str();

    hex_str_to_bytes(value_hash.as_str())
}

// turn "1f2b" to [0x1f, 0x2b]
fn hex_str_to_bytes(hash: &str) -> Vec<u8> {
    hash.as_bytes()
        .chunks_exact(2)
        .map(|cs| match *cs {
            [c1, c2] => {
                let hi = char::from(c1).to_digit(16).unwrap();
                let lo = char::from(c2).to_digit(16).unwrap();
                ((hi << 4) | lo) as u8
            }
            _ => unreachable!(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get_ledger() -> Ledger {
        use tempdir::TempDir;
        let data_dir = TempDir::new("test").unwrap();
        Ledger::new(data_dir.path()).await
    }

    #[test]
    fn test_make_key() {
        let namespace = "namespace";
        let key = "key";
        assert_eq!(make_key(namespace, key), String::from("namespace/key"));
    }

    #[test]
    fn test_make_collection_key() {
        let namespace = "namespace";
        let collection = "collection";
        let key = "key";
        assert_eq!(
            make_collection_key(namespace, collection, key),
            String::from("namespace/collection/key")
        );
    }

    #[test]
    fn test_compute_private_data_hash() {
        let data = "secret";
        let expect = b"+\xb8\rS{\x1d\xa3\xe3\x8b\xd3\x03a\xaa\x85V\x86\xbd\xe0\xea\xcdqb\xfe\xf6\xa2_\xe9{\xf5'\xa2[";
        assert_eq!(compute_private_data_hash(data.as_bytes()), expect.to_vec());
    }

    #[tokio::test]
    async fn test_get_set_state() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let tx_id = "123";
        let key = "key";
        let value = "value".as_bytes().to_vec();
        ledger.set_state(namespace, tx_id, key, value.clone()).await;
        assert_eq!(ledger.get_state(namespace, key), Some(&value));
    }

    #[tokio::test]
    async fn test_get_set_metadata() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let key = "key";
        let metakey = "metakey".to_string();
        let value = "value".as_bytes().to_vec();
        let metadata = StateMetadata {
            metakey: metakey.clone(),
            value: value.clone(),
        };
        ledger.set_state_metadata(namespace, key, metadata).await;
        let expect = {
            let mut meta_map = HashMap::new();
            meta_map.insert(metakey, value);
            meta_map
        };
        assert_eq!(ledger.get_state_metadata(namespace, key), Some(&expect));
    }

    #[tokio::test]
    async fn test_get_set_private_data() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let collection = "collection";
        let key = "key";
        let value = "value".as_bytes().to_vec();
        ledger
            .set_private_data(namespace, collection, key, value.clone())
            .await;
        assert_eq!(
            ledger.get_private_data(namespace, collection, key),
            Some(&value)
        );
    }

    #[tokio::test]
    async fn test_get_set_private_data_metadata() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let collection = "collection";
        let key = "key";
        let metakey = "metakey".to_string();
        let value = "value".as_bytes().to_vec();
        let metadata = StateMetadata {
            metakey: metakey.clone(),
            value: value.clone(),
        };
        ledger
            .set_private_data_metadata(namespace, collection, key, metadata)
            .await;
        let expect = {
            let mut meta_map = HashMap::new();
            meta_map.insert(metakey, value);
            meta_map
        };
        assert_eq!(
            ledger.get_private_data_metadata(namespace, collection, key),
            Some(&expect)
        );
    }

    #[tokio::test]
    async fn test_get_private_data_hash() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let collection = "collection";
        let key = "key";
        let value = "value".as_bytes().to_vec();
        ledger
            .set_private_data(namespace, collection, key, value.clone())
            .await;
        let expect = compute_private_data_hash(&value[..]);
        assert_eq!(
            ledger.get_private_data_hash(namespace, collection, key),
            Some(&expect)
        );
    }

    #[tokio::test]
    async fn test_get_state_by_range() {
        let mut ledger = get_ledger().await;
        let mut expect = vec![];
        let namespace = "namespace".to_string();
        for i in 0..10 {
            let tx_id = format!("123{}", i);
            let key = format!("key{}", i);
            let value = format!("value{}", i).as_bytes().to_vec();
            ledger
                .set_state(&namespace, &tx_id, &key, value.clone())
                .await;
            expect.push(Kv {
                namespace: namespace.clone(),
                key,
                value,
            });
        }
        assert_eq!(
            ledger
                .get_state_by_range(&namespace, "", "")
                .collect::<Vec<_>>(),
            expect[..].to_vec()
        );
        assert_eq!(
            ledger
                .get_state_by_range(&namespace, "key3", "")
                .collect::<Vec<_>>(),
            expect[3..].to_vec()
        );
        assert_eq!(
            ledger
                .get_state_by_range(&namespace, "", "key8")
                .collect::<Vec<_>>(),
            expect[..8].to_vec()
        );
        assert_eq!(
            ledger
                .get_state_by_range(&namespace, "key2", "key5")
                .collect::<Vec<_>>(),
            expect[2..5].to_vec()
        );
        assert_eq!(
            ledger
                .get_state_by_range(&namespace, "key0", "key9")
                .collect::<Vec<_>>(),
            expect[0..9].to_vec()
        );
    }

    #[tokio::test]
    async fn test_get_private_data_by_range() {
        let mut ledger = get_ledger().await;
        let mut expect = vec![];
        let namespace = "namespace".to_string();
        let collection = "collection".to_string();
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i).as_bytes().to_vec();
            ledger
                .set_private_data(&namespace, &collection, &key, value.clone())
                .await;
            expect.push(Kv {
                namespace: namespace.clone(),
                key,
                value,
            });
        }
        assert_eq!(
            ledger
                .get_private_data_by_range(&namespace, &collection, "", "")
                .collect::<Vec<_>>(),
            expect[..].to_vec()
        );
        assert_eq!(
            ledger
                .get_private_data_by_range(&namespace, &collection, "key3", "")
                .collect::<Vec<_>>(),
            expect[3..].to_vec()
        );
        assert_eq!(
            ledger
                .get_private_data_by_range(&namespace, &collection, "", "key8")
                .collect::<Vec<_>>(),
            expect[..8].to_vec()
        );
        assert_eq!(
            ledger
                .get_private_data_by_range(&namespace, &collection, "key2", "key5")
                .collect::<Vec<_>>(),
            expect[2..5].to_vec()
        );
        assert_eq!(
            ledger
                .get_private_data_by_range(&namespace, &collection, "key0", "key9")
                .collect::<Vec<_>>(),
            expect[0..9].to_vec()
        );
    }

    #[tokio::test]
    async fn test_get_history_for_key() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let key = "key";
        let mut expect = vec![];
        for i in 0..10 {
            let tx_id = format!("123{}", i);
            let value = format!("value{}", i).as_bytes().to_vec();
            ledger
                .set_state(namespace, &tx_id, key, value.clone())
                .await;
            expect.push(KeyModification {
                tx_id,
                value,
                timestamp: get_timestamp(),
                is_delete: false,
            });
        }
        let tx_id = "666".to_string();
        ledger.delete_state(namespace, &tx_id, key).await;
        expect.push(KeyModification {
            tx_id,
            value: vec![],
            timestamp: get_timestamp(),
            is_delete: true,
        });

        let tx_id = "667".to_string();
        let value = "value667".as_bytes().to_vec();
        ledger
            .set_state(namespace, &tx_id, key, value.clone())
            .await;
        expect.push(KeyModification {
            tx_id,
            value,
            timestamp: None,
            is_delete: false,
        });
        let res = ledger.get_history_for_key(&namespace, &key);
        assert!(res.zip(expect.into_iter()).all(|(r, e)| {
            // Skip timestamp comparison.
            r.tx_id == e.tx_id && r.value == e.value && r.is_delete == e.is_delete
        }));
    }

    #[tokio::test]
    async fn test_delete_state() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let key = "key";
        let tx_id = "123";
        let value = "value".as_bytes().to_vec();
        ledger.set_state(namespace, tx_id, key, value.clone()).await;
        assert_eq!(ledger.get_state(namespace, key), Some(&value));
        ledger.delete_state(namespace, tx_id, key).await;
        assert_eq!(ledger.get_state(&namespace, key), None);
    }

    #[tokio::test]
    async fn test_delete_private_data() {
        let mut ledger = get_ledger().await;
        let namespace = "namespace";
        let collection = "collection";
        let key = "key";
        let value = "value".as_bytes().to_vec();
        ledger
            .set_private_data(namespace, collection, key, value.clone())
            .await;
        assert_eq!(
            ledger.get_private_data(namespace, collection, key),
            Some(&value)
        );
        ledger.delete_private_data(namespace, collection, key).await;
        assert_eq!(ledger.get_private_data(namespace, collection, key), None);
    }
}
