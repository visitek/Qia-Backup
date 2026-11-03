//! Persistent blob index: maps content identifiers to their location and
//! tracks blob packs so retention can delete unreferenced packs safely.
//!
//! Scale note: the on-disk representation is still JSON. For multi-million
//! blob sets, loading and checkpointing the full map becomes the dominant
//! startup cost; a compact/binary index is the next major optimization.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::config;
use crate::core::error::{BackupError, IoContext, Result};
use crate::core::logging;
use crate::core::model::{AppConfig, BlobLoc};
use crate::core::store::BlobStore;

/// Current on-disk index/journal schema. Bump this when the representation
/// changes incompatibly and add a migration path; an older app must never
/// reinterpret or overwrite a newer index.
pub const CURRENT_INDEX_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackInfo {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobIndex {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub blobs: HashMap<String, BlobLoc>,
    #[serde(default)]
    pub packs: HashMap<String, PackInfo>,
    /// Number of journal segments merged into this index. Not persisted in the
    /// base object; used to decide when to compact journals back into it.
    #[serde(skip)]
    pub journal_segments: usize,
}

/// A small, append-only slice of the blob index uploaded after a checkpoint.
/// Loading a base index plus every journal segment reconstructs the full index
/// without rewriting the (potentially hundreds of MB) base object each time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexJournal {
    pub version: u32,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub blobs: HashMap<String, BlobLoc>,
    #[serde(default)]
    pub packs: HashMap<String, PackInfo>,
}

impl Default for BlobIndex {
    fn default() -> Self {
        Self {
            version: CURRENT_INDEX_VERSION,
            updated_at: Utc::now(),
            blobs: HashMap::new(),
            packs: HashMap::new(),
            journal_segments: 0,
        }
    }
}

impl BlobIndex {
    pub fn blob(&self, id: &str) -> Option<&BlobLoc> {
        self.blobs.get(id)
    }

    pub fn add_blob(&mut self, loc: BlobLoc) {
        self.blobs.insert(loc.blob_identifier.clone(), loc);
        self.updated_at = Utc::now();
    }

    pub fn add_pack(&mut self, path: String, size: u64) {
        self.packs.insert(path.clone(), PackInfo { path, size });
        self.updated_at = Utc::now();
    }

    pub fn remove_pack(&mut self, path: &str) -> Option<PackInfo> {
        self.updated_at = Utc::now();
        self.packs.remove(path)
    }

    pub fn referenced_blob_count(&self) -> usize {
        self.blobs.len()
    }

    pub fn total_pack_bytes(&self) -> u64 {
        self.packs.values().map(|p| p.size).sum()
    }

    fn namespace(config: &AppConfig) -> String {
        // Keep AWS cache names stable while separating B2 from every AWS set.
        let identity = match config.storage_provider {
            crate::core::model::StorageProvider::AwsS3 => {
                format!("{}|{}|{}", config.bucket, config.region, config.key_root())
            }
            crate::core::model::StorageProvider::BackblazeB2 => format!(
                "b2|{}|{}|{}|{}",
                config.endpoint.trim().trim_end_matches('/'),
                config.bucket,
                config.region,
                config.key_root()
            ),
        };
        let digest = crate::core::crypto::sha256_hex(identity.as_bytes());
        format!("{}-{}", config.backup_set_uuid, &digest[..16])
    }

    pub fn local_path(config: &AppConfig) -> PathBuf {
        config::app_support_dir()
            .join("index")
            .join(format!("{}.json", Self::namespace(config)))
    }

    pub fn load_local(config: &AppConfig) -> Option<Self> {
        let path = Self::local_path(config);
        let bytes = std::fs::read(&path).ok()?;
        let index: Self = serde_json::from_slice(&bytes).ok()?;
        // A newer local cache must not be interpreted with an older schema.
        (index.version <= CURRENT_INDEX_VERSION).then_some(index)
    }

    fn write_local_bytes(config: &AppConfig, bytes: &[u8]) -> Result<()> {
        let path = Self::local_path(config);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).at(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes).at(&tmp)?;
        std::fs::rename(&tmp, &path).at(&path)?;
        Ok(())
    }

    pub fn save_local(&self, config: &AppConfig) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        Self::write_local_bytes(config, &bytes)
    }

    pub fn remote_key(root: &str) -> String {
        format!("{}/blobindex.json.lz4", root.trim_end_matches('/'))
    }

    pub fn journal_prefix(root: &str) -> String {
        format!("{}/indexjournal/", root.trim_end_matches('/'))
    }

    fn journal_key(root: &str) -> String {
        format!(
            "{}{}-{}.json.lz4",
            Self::journal_prefix(root),
            Utc::now().timestamp_millis(),
            uuid::Uuid::new_v4()
        )
    }

    fn decode_index(bytes: &[u8]) -> Option<BlobIndex> {
        let plain = if bytes.len() >= 4 {
            crate::core::compression::lz4_decompress(bytes).unwrap_or_else(|_| bytes.to_vec())
        } else {
            bytes.to_vec()
        };
        serde_json::from_slice(&plain).ok()
    }

    fn decode_journal(bytes: &[u8]) -> Option<IndexJournal> {
        let plain = if bytes.len() >= 4 {
            crate::core::compression::lz4_decompress(bytes).unwrap_or_else(|_| bytes.to_vec())
        } else {
            bytes.to_vec()
        };
        serde_json::from_slice(&plain).ok()
    }

    fn merge_blobs(&mut self, blobs: HashMap<String, BlobLoc>) {
        for (id, loc) in blobs {
            self.blobs.entry(id).or_insert(loc);
        }
    }

    fn merge_packs(&mut self, packs: HashMap<String, PackInfo>) {
        for (path, pack) in packs {
            self.packs.entry(path).or_insert(pack);
        }
    }

    fn merge_index(&mut self, other: BlobIndex) {
        self.merge_blobs(other.blobs);
        self.merge_packs(other.packs);
        if other.updated_at > self.updated_at {
            self.updated_at = other.updated_at;
        }
    }

    fn merge_journal(&mut self, journal: IndexJournal) {
        self.merge_blobs(journal.blobs);
        self.merge_packs(journal.packs);
        if journal.updated_at > self.updated_at {
            self.updated_at = journal.updated_at;
        }
    }

    /// Load the base index (remote preferred over a stale local cache) and
    /// replay every journal segment uploaded by interrupted checkpoints.
    ///
    /// A backup must never start from a partial index: if the remote base or a
    /// journal cannot be read, this returns an error instead of silently
    /// continuing. Saving a partial base later would overwrite durable state
    /// and could leave records pointing at blobs the index no longer knows.
    /// The local cache is only merged when the remote base was read
    /// successfully; if the remote base is gone, the cache may describe
    /// objects from a cleared bucket and is unsafe to dedupe against.
    pub fn load(store: &dyn BlobStore, root: &str, config: &AppConfig) -> Result<Self> {
        let local = Self::load_local(config);
        let remote = match store.get_opt(&Self::remote_key(root)) {
            Ok(Some(bytes)) => match Self::decode_index(&bytes) {
                Some(index) if index.version <= CURRENT_INDEX_VERSION => Some(index),
                Some(index) => {
                    return Err(BackupError::Store(format!(
                        "blob index version {} is newer than supported version {CURRENT_INDEX_VERSION}",
                        index.version
                    )));
                }
                None => {
                    return Err(BackupError::Store(
                        "remote blob index is unreadable; refusing to start from a partial index"
                            .into(),
                    ));
                }
            },
            Ok(None) => None,
            Err(e) => return Err(e),
        };

        let mut ignored_local_without_base = false;
        let mut index = match (local, remote) {
            (Some(local), Some(remote)) => {
                if local.updated_at >= remote.updated_at {
                    let mut merged = local;
                    merged.merge_index(remote);
                    merged
                } else {
                    // The remote base is newer, so the local cache is a
                    // subset. Using it as the accumulator would double peak
                    // memory for a multi-million-blob index for no gain.
                    remote
                }
            }
            (Some(_), None) => {
                ignored_local_without_base = true;
                Self::default()
            }
            (None, Some(remote)) => remote,
            (None, None) => Self::default(),
        };

        index.journal_segments = 0;
        let prefix = Self::journal_prefix(root);
        let journals = store.list(&prefix)?;
        for object in journals {
            match store.get_opt(&object.key) {
                Ok(Some(bytes)) => match Self::decode_journal(&bytes) {
                    Some(journal) if journal.version <= CURRENT_INDEX_VERSION => {
                        index.merge_journal(journal);
                        index.journal_segments += 1;
                    }
                    Some(journal) => {
                        return Err(BackupError::Store(format!(
                            "index journal version {} is newer than supported version {CURRENT_INDEX_VERSION}",
                            journal.version
                        )));
                    }
                    None => {
                        return Err(BackupError::Store(format!(
                            "index journal {} is unreadable; refusing to drop it",
                            object.key
                        )));
                    }
                },
                Ok(None) => {
                    return Err(BackupError::Store(format!(
                        "index journal {} disappeared while loading",
                        object.key
                    )));
                }
                Err(e) => return Err(e),
            }
        }
        if ignored_local_without_base {
            if index.journal_segments > 0 {
                logging::warn(format!(
                    "remote blob index base is not written yet; recovered {} journal segment(s) from S3 and ignored the local cache until a base exists",
                    index.journal_segments
                ));
            } else {
                logging::warn(
                    "remote blob index is missing and no journal segments were found; ignoring local cache to avoid stale dedupe",
                );
            }
        }
        Ok(index)
    }

    /// Merge every journal that can be decoded, logging and skipping corrupt
    /// ones. This is for recovery only: normal backups use strict [`load`] so
    /// a transient read problem never causes a journal to be dropped
    /// silently.
    pub fn replay_journals_lenient(&mut self, store: &dyn BlobStore, root: &str) -> Result<usize> {
        let prefix = Self::journal_prefix(root);
        let journals = store.list(&prefix)?;
        let mut merged = 0;
        for object in journals {
            match store.get_opt(&object.key)? {
                Some(bytes) => match Self::decode_journal(&bytes) {
                    Some(journal) if journal.version <= CURRENT_INDEX_VERSION => {
                        self.merge_journal(journal);
                        merged += 1;
                    }
                    Some(journal) => {
                        return Err(BackupError::Store(format!(
                            "index journal version {} is newer than supported version {CURRENT_INDEX_VERSION}",
                            journal.version
                        )));
                    }
                    None => logging::warn(format!(
                        "recovery: skipping unreadable index journal {}",
                        object.key
                    )),
                },
                None => logging::warn(format!(
                    "recovery: index journal {} disappeared",
                    object.key
                )),
            }
        }
        Ok(merged)
    }

    /// Upload one small journal containing only entries confirmed since the
    /// previous checkpoint. The pending maps are cloned because the caller
    /// keeps ownership until the PUT succeeds.
    pub fn save_journal(
        store: &dyn BlobStore,
        root: &str,
        blobs: &HashMap<String, BlobLoc>,
        packs: &HashMap<String, PackInfo>,
    ) -> Result<()> {
        let journal = IndexJournal {
            version: CURRENT_INDEX_VERSION,
            updated_at: Utc::now(),
            blobs: blobs.clone(),
            packs: packs.clone(),
        };
        let json = serde_json::to_vec(&journal)?;
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        store.put(&Self::journal_key(root), &compressed)?;
        Ok(())
    }

    pub fn save(&self, store: &dyn BlobStore, root: &str, config: &AppConfig) -> Result<()> {
        // Serialize once and reuse the bytes for both the local cache and the
        // remote compressed base. On a multi-million-blob index this halves
        // the serialization CPU and peak allocation.
        let json = serde_json::to_vec(self)?;
        // The local file is only a cache. A full or unwritable local disk must
        // not prevent the durable remote index from being written and the
        // backup from completing.
        if let Err(e) = Self::write_local_bytes(config, &json) {
            logging::warn(format!("could not write local index cache: {e}"));
        }
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        let key = Self::remote_key(root);
        store.put(&key, &compressed)?;
        Ok(())
    }

    /// Write a full base index and then remove the journal segments it
    /// supersedes. Ordering matters: the base must be durable before a journal
    /// is deleted. If listing or deleting a journal fails, this returns an
    /// error so callers (notably garbage collection) abort before deleting
    /// objects that the leftover journal may still reference. Leftover
    /// journals after a crash are harmless duplicates.
    pub fn save_and_clear(
        &mut self,
        store: &dyn BlobStore,
        root: &str,
        config: &AppConfig,
    ) -> Result<()> {
        self.updated_at = Utc::now();
        self.save(store, root, config)?;
        let prefix = Self::journal_prefix(root);
        let journals = store.list(&prefix).map_err(|e| {
            BackupError::Store(format!("could not list index journals to clear: {e}"))
        })?;
        for object in journals {
            store.delete(&object.key).map_err(|e| {
                BackupError::Store(format!(
                    "could not delete old index journal {}: {e}",
                    object.key
                ))
            })?;
        }
        self.journal_segments = 0;
        Ok(())
    }

    /// Remove entries whose IDs are not in `keep`.
    ///
    /// Pack membership is derived from the indexed blobs instead of being
    /// stored separately: a pack is kept while at least one of its blobs is
    /// still referenced. Bytes of unreferenced blobs inside a surviving pack
    /// stay in the pack object; only the index entry is dropped.
    pub fn prune_to(&mut self, keep: &HashSet<String>) -> Vec<String> {
        let mut removed_objects = Vec::new();
        let mut remove_ids = Vec::new();
        let mut pack_kept: HashMap<String, u64> = HashMap::new();

        for (id, loc) in &self.blobs {
            if loc.is_packed {
                let kept = pack_kept.entry(loc.relative_path.clone()).or_insert(0);
                if keep.contains(id) {
                    *kept += 1;
                } else {
                    remove_ids.push(id.clone());
                }
            } else if !keep.contains(id) {
                remove_ids.push(id.clone());
                removed_objects.push(loc.relative_path.clone());
            }
        }

        for id in remove_ids {
            self.blobs.remove(&id);
        }
        // A pack is unreachable when no surviving blob points into it. This
        // also drops orphan pack records that have no indexed members.
        let pack_paths: Vec<String> = self.packs.keys().cloned().collect();
        for path in pack_paths {
            if pack_kept.get(&path).copied().unwrap_or(0) == 0 {
                self.packs.remove(&path);
                removed_objects.push(path);
            }
        }

        self.updated_at = Utc::now();
        removed_objects.sort();
        removed_objects.dedup();
        removed_objects
    }

    pub fn resolve_object_key(root: &str, relative_path: &str) -> String {
        crate::core::record::root_key(root, relative_path)
    }

    pub fn check_consistency(&self) -> Result<()> {
        for loc in self.blobs.values() {
            if loc.is_packed && !self.packs.contains_key(&loc.relative_path) {
                return Err(BackupError::Store(format!(
                    "index references unknown pack {}",
                    loc.relative_path
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::BlobLoc;

    fn loc(id: &str, packed: bool, path: &str) -> BlobLoc {
        BlobLoc {
            blob_identifier: id.into(),
            is_packed: packed,
            relative_path: path.into(),
            offset: 0,
            length: 1,
            stretch_encryption_key: false,
            compression_type: 0,
        }
    }

    #[test]
    fn cache_identity_separates_b2_and_preserves_aws() {
        let aws = AppConfig {
            bucket: "bucket".into(),
            region: "us-west-004".into(),
            backup_set_uuid: "set".into(),
            ..AppConfig::default()
        };
        let old_identity = format!("{}|{}|{}", aws.bucket, aws.region, aws.key_root());
        let digest = crate::core::crypto::sha256_hex(old_identity.as_bytes());
        assert_eq!(BlobIndex::namespace(&aws), format!("set-{}", &digest[..16]));
        let b2 = AppConfig {
            storage_provider: crate::core::model::StorageProvider::BackblazeB2,
            endpoint: "https://s3.us-west-004.backblazeb2.com".into(),
            ..aws.clone()
        };
        assert_ne!(BlobIndex::local_path(&aws), BlobIndex::local_path(&b2));
    }

    #[test]
    fn journals_replay_and_compact() {
        let dir = std::env::temp_dir().join(format!("index-journal-test-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();

        let mut index = BlobIndex::default();
        index.add_blob(loc("a", false, "standardobjects/aa/a"));
        index.save(&store, &root, &config).unwrap();

        let mut blobs = HashMap::new();
        blobs.insert("b".into(), loc("b", true, "blobpacks/p1"));
        let mut packs = HashMap::new();
        packs.insert(
            "blobpacks/p1".into(),
            PackInfo {
                path: "blobpacks/p1".into(),
                size: 123,
            },
        );
        BlobIndex::save_journal(&store, &root, &blobs, &packs).unwrap();

        let mut loaded = BlobIndex::load(&store, &root, &config).unwrap();
        assert!(loaded.blob("a").is_some());
        assert!(loaded.blob("b").is_some());
        assert!(loaded.packs.contains_key("blobpacks/p1"));
        assert_eq!(loaded.journal_segments, 1);

        loaded.save_and_clear(&store, &root, &config).unwrap();
        let compacted = BlobIndex::load(&store, &root, &config).unwrap();
        assert!(compacted.blob("a").is_some());
        assert!(compacted.blob("b").is_some());
        assert_eq!(compacted.journal_segments, 0);
        assert!(
            store
                .list(&BlobIndex::journal_prefix(&root))
                .unwrap()
                .is_empty()
        );

        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
    }

    #[test]
    fn prune_keeps_referenced_packs() {
        let mut idx = BlobIndex::default();
        idx.add_blob(loc("a", true, "blobpacks/p1"));
        idx.add_blob(loc("b", true, "blobpacks/p1"));
        idx.add_pack("blobpacks/p1".into(), 10);
        idx.add_blob(loc("c", false, "standardobjects/c"));
        let keep: HashSet<String> = ["a".into()].into_iter().collect();
        let removed = idx.prune_to(&keep);
        assert!(removed.contains(&"standardobjects/c".to_string()));
        assert!(idx.blobs.contains_key("a"));
        assert!(!idx.blobs.contains_key("b"));
        assert!(!idx.blobs.contains_key("c"));
        assert!(idx.packs.contains_key("blobpacks/p1"));
    }

    #[test]
    fn prune_removes_pack_without_referenced_blobs() {
        let mut idx = BlobIndex::default();
        idx.add_blob(loc("a", true, "blobpacks/p1"));
        idx.add_pack("blobpacks/p1".into(), 10);
        let keep: HashSet<String> = HashSet::new();
        let removed = idx.prune_to(&keep);
        assert!(removed.contains(&"blobpacks/p1".to_string()));
        assert!(idx.blobs.is_empty());
        assert!(idx.packs.is_empty());
    }

    #[test]
    fn pack_records_carry_no_duplicated_blob_ids() {
        let mut idx = BlobIndex::default();
        idx.add_blob(loc("a", true, "blobpacks/p1"));
        idx.add_pack("blobpacks/p1".into(), 10);
        let json = serde_json::to_string(&idx).unwrap();
        assert!(!json.contains("blob_ids"));
        assert!(!json.contains("blobIds"));
    }

    #[test]
    fn load_rejects_newer_index_version() {
        let dir = std::env::temp_dir().join(format!("index-newer-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        let mut index = BlobIndex::default();
        index.version = CURRENT_INDEX_VERSION + 1;
        index.save(&store, &root, &config).unwrap();

        let err = BlobIndex::load(&store, &root, &config).unwrap_err();
        assert!(matches!(err, BackupError::Store(_)), "got {err:?}");

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_rejects_newer_journal_version() {
        let dir = std::env::temp_dir().join(format!("journal-newer-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        BlobIndex::default().save(&store, &root, &config).unwrap();
        let journal = IndexJournal {
            version: CURRENT_INDEX_VERSION + 1,
            updated_at: Utc::now(),
            blobs: HashMap::new(),
            packs: HashMap::new(),
        };
        let json = serde_json::to_vec(&journal).unwrap();
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        store
            .put(
                &format!("{}newer.json.lz4", BlobIndex::journal_prefix(&root)),
                &compressed,
            )
            .unwrap();

        let err = BlobIndex::load(&store, &root, &config).unwrap_err();
        assert!(matches!(err, BackupError::Store(_)), "got {err:?}");

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lenient_replay_merges_readable_journals_and_skips_corrupt() {
        let dir = std::env::temp_dir().join(format!("index-lenient-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();

        let mut blobs = HashMap::new();
        blobs.insert("good".into(), loc("good", true, "blobpacks/p1"));
        BlobIndex::save_journal(&store, &root, &blobs, &HashMap::new()).unwrap();
        store
            .put(
                &format!("{}broken.json.lz4", BlobIndex::journal_prefix(&root)),
                b"not a valid journal",
            )
            .unwrap();

        let mut recovered = BlobIndex::default();
        let merged = recovered.replay_journals_lenient(&store, &root).unwrap();
        assert_eq!(merged, 1);
        assert!(recovered.blob("good").is_some());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_ignores_local_cache_when_remote_index_is_missing() {
        let dir = std::env::temp_dir().join(format!("index-missing-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();

        let mut local = BlobIndex::default();
        local.add_blob(loc("stale", false, "standardobjects/aa/stale"));
        local.save_local(&config).unwrap();
        assert!(BlobIndex::load_local(&config).is_some());

        let mut journal_blobs = HashMap::new();
        journal_blobs.insert(
            "fresh".into(),
            loc("fresh", false, "standardobjects/ff/fresh"),
        );
        BlobIndex::save_journal(&store, &root, &journal_blobs, &HashMap::new()).unwrap();

        let loaded = BlobIndex::load(&store, &root, &config).unwrap();
        assert!(
            loaded.blob("stale").is_none(),
            "a local cache from a cleared bucket must not be used for dedupe"
        );
        assert!(
            loaded.blob("fresh").is_some(),
            "journals must still be replayed when the base index is missing"
        );

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_rejects_unreadable_remote_index() {
        let dir = std::env::temp_dir().join(format!("index-bad-base-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        store
            .put(&BlobIndex::remote_key(&root), b"not a valid index")
            .unwrap();

        let err = BlobIndex::load(&store, &root, &config).unwrap_err();
        assert!(matches!(err, BackupError::Store(_)), "got {err:?}");

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_rejects_unreadable_journal() {
        let dir = std::env::temp_dir().join(format!("index-bad-journal-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        BlobIndex::default().save(&store, &root, &config).unwrap();
        store
            .put(
                &format!("{}broken.json.lz4", BlobIndex::journal_prefix(&root)),
                b"not a valid journal",
            )
            .unwrap();

        let err = BlobIndex::load(&store, &root, &config).unwrap_err();
        assert!(matches!(err, BackupError::Store(_)), "got {err:?}");

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    struct DeleteFailingStore<'a> {
        inner: &'a crate::core::store::LocalStore,
    }

    impl BlobStore for DeleteFailingStore<'_> {
        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get(key)
        }

        fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_opt(key)
        }

        fn head(&self, key: &str) -> Result<Option<crate::core::store::ObjectInfo>> {
            self.inner.head(key)
        }

        fn delete(&self, _key: &str) -> Result<()> {
            Err(BackupError::Store("delete blocked for test".into()))
        }

        fn list(&self, prefix: &str) -> Result<Vec<crate::core::store::ObjectInfo>> {
            self.inner.list(prefix)
        }

        fn describe(&self) -> String {
            self.inner.describe()
        }
    }

    #[test]
    fn save_and_clear_reports_journal_cleanup_failure() {
        let dir = std::env::temp_dir().join(format!("index-delete-fail-{}", uuid::Uuid::new_v4()));
        let inner = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        let mut index = BlobIndex::default();
        index.add_blob(loc("a", false, "standardobjects/aa/a"));
        index.save(&inner, &root, &config).unwrap();

        let mut blobs = HashMap::new();
        blobs.insert("b".into(), loc("b", true, "blobpacks/p1"));
        let mut packs = HashMap::new();
        packs.insert(
            "blobpacks/p1".into(),
            PackInfo {
                path: "blobpacks/p1".into(),
                size: 10,
            },
        );
        BlobIndex::save_journal(&inner, &root, &blobs, &packs).unwrap();

        let failing = DeleteFailingStore { inner: &inner };
        let err = index.save_and_clear(&failing, &root, &config).unwrap_err();
        assert!(
            err.to_string()
                .contains("could not delete old index journal"),
            "got {err:?}"
        );
        // The base was saved, but the journal remains. Callers must abort
        // object deletion so the journal can never point at a deleted object.
        assert_eq!(
            inner.list(&BlobIndex::journal_prefix(&root)).unwrap().len(),
            1
        );

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }
}
