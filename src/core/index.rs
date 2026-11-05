//! Persistent blob index: maps content identifiers to their location and
//! tracks blob packs so retention can delete unreferenced packs safely.
//!
//! The base index is stored in a compact binary format. Blob IDs are raw
//! SHA-256 bytes, pack/standalone paths are stored once in a path table, and
//! blobs sharing a path are grouped so the path is not repeated per chunk.
//! This matters because a multi-million-blob set used to serialize as a huge
//! JSON object that had to be downloaded on every run. Version-1 JSON indexes
//! remain readable and are rewritten as version 2 on the next compaction.

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
///
/// Version 3 adds deletion tombstones to journals so edits and garbage
/// collection can update the index without rewriting the full base object.
pub const CURRENT_INDEX_VERSION: u32 = 3;

/// Magic prefix for the compact binary base index. The version follows it as
/// a little-endian u32 so a future layout can keep the same prefix.
const INDEX_MAGIC: &[u8; 4] = b"QIDX";

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
    /// Blob identifiers removed since the last journal segment. Version 3+.
    #[serde(default)]
    pub removed_blobs: Vec<String>,
    /// Pack paths removed since the last journal segment. Version 3+.
    #[serde(default)]
    pub removed_packs: Vec<String>,
}

/// Result of pruning an index: the object paths that can now be deleted, plus
/// the exact index entries that were removed so they can be recorded in a
/// journal instead of rewriting the whole base index.
#[derive(Debug, Clone, Default)]
pub struct PruneDelta {
    pub removed_objects: Vec<String>,
    pub removed_blob_ids: Vec<String>,
    pub removed_pack_paths: Vec<String>,
}

/// Small sidecar next to the local index cache. It records which remote base
/// object the cache was derived from, so an unchanged remote index can be
/// confirmed with a cheap HEAD instead of downloading hundreds of megabytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalIndexMeta {
    etag: Option<String>,
    size: u64,
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift = shift.checked_add(7)?;
        if shift > 63 {
            return None;
        }
    }
}

fn take<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = pos.checked_add(len)?;
    let slice = bytes.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

fn read_u8(bytes: &[u8], pos: &mut usize) -> Option<u8> {
    Some(*take(bytes, pos, 1)?.first()?)
}

fn read_u32_le(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    let raw: [u8; 4] = take(bytes, pos, 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(raw))
}

fn read_i64_le(bytes: &[u8], pos: &mut usize) -> Option<i64> {
    let raw: [u8; 8] = take(bytes, pos, 8)?.try_into().ok()?;
    Some(i64::from_le_bytes(raw))
}

/// Most IDs are 64-character SHA-256 hex. Store those as 32 raw bytes; keep a
/// length-prefixed string fallback for legacy/imported identifiers.
fn write_blob_id(out: &mut Vec<u8>, id: &str) -> Result<()> {
    if id.len() == 64 {
        let mut raw = [0u8; 32];
        if hex::decode_to_slice(id, &mut raw).is_ok() {
            out.push(32);
            out.extend_from_slice(&raw);
            return Ok(());
        }
    }
    if id.len() > 0x7f {
        return Err(BackupError::Codec(format!(
            "blob identifier is too long ({} bytes)",
            id.len()
        )));
    }
    out.push(0x80 | id.len() as u8);
    out.extend_from_slice(id.as_bytes());
    Ok(())
}

fn read_blob_id(bytes: &[u8], pos: &mut usize) -> Option<String> {
    let header = read_u8(bytes, pos)?;
    if header & 0x80 != 0 {
        let len = (header & 0x7f) as usize;
        let raw = take(bytes, pos, len)?;
        return Some(std::str::from_utf8(raw).ok()?.to_string());
    }
    if header != 32 {
        return None;
    }
    Some(hex::encode(take(bytes, pos, 32)?))
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
        let identity = format!("{}|{}|{}", config.bucket, config.region, config.key_root());
        let digest = crate::core::crypto::sha256_hex(identity.as_bytes());
        format!("{}-{}", config.backup_set_uuid, &digest[..16])
    }

    pub fn local_path(config: &AppConfig) -> PathBuf {
        config::app_support_dir()
            .join("index")
            .join(format!("{}.json", Self::namespace(config)))
    }

    fn local_meta_path(config: &AppConfig) -> PathBuf {
        config::app_support_dir()
            .join("index")
            .join(format!("{}.meta.json", Self::namespace(config)))
    }

    pub fn load_local(config: &AppConfig) -> Option<Self> {
        let path = Self::local_path(config);
        let bytes = std::fs::read(&path).ok()?;
        let index = Self::decode_plain(&bytes)?;
        // A newer local cache must not be interpreted with an older schema.
        (index.version <= CURRENT_INDEX_VERSION).then_some(index)
    }

    fn load_local_meta(config: &AppConfig) -> Option<LocalIndexMeta> {
        let bytes = std::fs::read(Self::local_meta_path(config)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn write_local_meta(config: &AppConfig, meta: &LocalIndexMeta) {
        let path = Self::local_meta_path(config);
        let write = || -> Result<()> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).at(parent)?;
            }
            let tmp = path.with_extension("meta.tmp");
            std::fs::write(&tmp, serde_json::to_vec(meta)?).at(&tmp)?;
            std::fs::rename(&tmp, &path).at(&path)?;
            Ok(())
        };
        if let Err(e) = write() {
            logging::warn(format!("could not write local index metadata: {e}"));
        }
    }

    fn clear_local_meta(config: &AppConfig) {
        let _ = std::fs::remove_file(Self::local_meta_path(config));
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
        let bytes = self.encode_binary()?;
        Self::write_local_bytes(config, &bytes)
    }

    /// Compact binary encoding. Paths are interned once and blobs are grouped
    /// by path so a pack path (and its prefix) is not repeated for every chunk.
    fn encode_binary(&self) -> Result<Vec<u8>> {
        let mut paths: Vec<&str> = self.packs.keys().map(String::as_str).collect();
        paths.extend(self.blobs.values().map(|loc| loc.relative_path.as_str()));
        paths.sort_unstable();
        paths.dedup();
        let path_index: HashMap<&str, u32> = paths
            .iter()
            .enumerate()
            .map(|(index, path)| (*path, index as u32))
            .collect();

        let mut groups: Vec<(u32, bool, &BlobLoc)> = Vec::with_capacity(self.blobs.len());
        for (key, loc) in &self.blobs {
            if key != &loc.blob_identifier {
                return Err(BackupError::Codec(format!(
                    "blob index entry {key} has mismatched identifier {}",
                    loc.blob_identifier
                )));
            }
            let path = *path_index
                .get(loc.relative_path.as_str())
                .ok_or_else(|| BackupError::Codec("blob index path is not interned".into()))?;
            groups.push((path, loc.is_packed, loc));
        }
        groups.sort_unstable_by_key(|(path, packed, _)| (*path, *packed));

        let mut out = Vec::with_capacity(24 + self.blobs.len() * 44 + paths.len() * 24);
        out.extend_from_slice(INDEX_MAGIC);
        // Writers always emit the current layout. A loaded v1 index is
        // transparently upgraded the next time it is saved.
        out.extend_from_slice(&CURRENT_INDEX_VERSION.to_le_bytes());
        out.extend_from_slice(&self.updated_at.timestamp_millis().to_le_bytes());
        write_varint(&mut out, paths.len() as u64);
        for path in &paths {
            write_varint(&mut out, path.len() as u64);
            out.extend_from_slice(path.as_bytes());
        }

        let mut pack_entries: Vec<(&String, &PackInfo)> = self.packs.iter().collect();
        pack_entries.sort_unstable_by_key(|(_, pack)| path_index[pack.path.as_str()]);
        write_varint(&mut out, pack_entries.len() as u64);
        for (key, pack) in pack_entries {
            if key != &pack.path {
                return Err(BackupError::Codec(format!(
                    "pack index entry {key} has mismatched path {}",
                    pack.path
                )));
            }
            write_varint(&mut out, path_index[pack.path.as_str()] as u64);
            write_varint(&mut out, pack.size);
        }

        let mut group_count = 0usize;
        let mut cursor = 0usize;
        while cursor < groups.len() {
            group_count += 1;
            let (path, packed, _) = groups[cursor];
            cursor += 1;
            while cursor < groups.len() && groups[cursor].0 == path && groups[cursor].1 == packed {
                cursor += 1;
            }
        }
        write_varint(&mut out, group_count as u64);

        let mut cursor = 0usize;
        while cursor < groups.len() {
            let (path, packed, _) = groups[cursor];
            let start = cursor;
            cursor += 1;
            while cursor < groups.len() && groups[cursor].0 == path && groups[cursor].1 == packed {
                cursor += 1;
            }
            write_varint(&mut out, path as u64);
            out.push(if packed { 1 } else { 0 });
            write_varint(&mut out, (cursor - start) as u64);
            for (_, _, loc) in &groups[start..cursor] {
                write_blob_id(&mut out, &loc.blob_identifier)?;
                let compression = u8::try_from(loc.compression_type).map_err(|_| {
                    BackupError::Codec(format!(
                        "compression type {} does not fit the index format",
                        loc.compression_type
                    ))
                })?;
                out.push(if loc.stretch_encryption_key { 1 } else { 0 });
                out.push(compression);
                write_varint(&mut out, loc.offset);
                write_varint(&mut out, loc.length);
            }
        }
        Ok(out)
    }

    fn decode_binary(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 || &bytes[..4] != INDEX_MAGIC {
            return None;
        }
        let mut pos = 4;
        let version = read_u32_le(bytes, &mut pos)?;
        if version > CURRENT_INDEX_VERSION {
            // Let the caller report a precise "newer than supported" error.
            return Some(Self {
                version,
                updated_at: Utc::now(),
                blobs: HashMap::new(),
                packs: HashMap::new(),
                journal_segments: 0,
            });
        }
        let updated_at = DateTime::<Utc>::from_timestamp_millis(read_i64_le(bytes, &mut pos)?)
            .unwrap_or_else(Utc::now);

        let path_count = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
        let mut paths = Vec::with_capacity(path_count);
        for _ in 0..path_count {
            let len = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
            let raw = take(bytes, &mut pos, len)?;
            paths.push(std::str::from_utf8(raw).ok()?.to_string());
        }

        let pack_count = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
        let mut packs = HashMap::with_capacity(pack_count);
        for _ in 0..pack_count {
            let index = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
            let size = read_varint(bytes, &mut pos)?;
            let path = paths.get(index)?.clone();
            packs.insert(path.clone(), PackInfo { path, size });
        }

        let group_count = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
        let mut blobs = HashMap::new();
        for _ in 0..group_count {
            let index = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
            let packed = read_u8(bytes, &mut pos)? != 0;
            let count = usize::try_from(read_varint(bytes, &mut pos)?).ok()?;
            let path = paths.get(index)?.clone();
            for _ in 0..count {
                let id = read_blob_id(bytes, &mut pos)?;
                let flags = read_u8(bytes, &mut pos)?;
                let compression_type = read_u8(bytes, &mut pos)? as u32;
                let offset = read_varint(bytes, &mut pos)?;
                let length = read_varint(bytes, &mut pos)?;
                blobs.insert(
                    id.clone(),
                    BlobLoc {
                        blob_identifier: id,
                        is_packed: packed,
                        relative_path: path.clone(),
                        offset,
                        length,
                        stretch_encryption_key: flags & 1 != 0,
                        compression_type,
                    },
                );
            }
        }
        if pos != bytes.len() {
            return None;
        }
        Some(Self {
            version: CURRENT_INDEX_VERSION,
            updated_at,
            blobs,
            packs,
            journal_segments: 0,
        })
    }

    fn decode_plain(bytes: &[u8]) -> Option<Self> {
        if bytes.starts_with(INDEX_MAGIC) {
            Self::decode_binary(bytes)
        } else {
            serde_json::from_slice(bytes).ok()
        }
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
        Self::decode_plain(&plain)
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
        // Deletions are applied after additions so a tombstone in the same
        // segment always wins. Journals are replayed oldest-first.
        for id in journal.removed_blobs {
            self.blobs.remove(&id);
        }
        if !journal.removed_packs.is_empty() {
            let removed: HashSet<&str> = journal.removed_packs.iter().map(String::as_str).collect();
            self.packs
                .retain(|path, _| !removed.contains(path.as_str()));
            // Defensive: never leave blobs pointing at a pack that a journal
            // explicitly removed.
            self.blobs
                .retain(|_, loc| !removed.contains(loc.relative_path.as_str()));
        }
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
        let local_meta = Self::load_local_meta(config);
        let remote_key = Self::remote_key(root);
        let remote_head = store.head(&remote_key)?;

        // A local cache whose recorded ETag/size matches the remote object is
        // already the same base. The journals are still replayed below, so
        // checkpoints written after the last compaction are not missed. This
        // avoids downloading a very large base on every run.
        let local_matches_remote = local.is_some()
            && remote_head.as_ref().is_some_and(|head| {
                head.etag.is_some()
                    && local_meta
                        .as_ref()
                        .is_some_and(|meta| meta.etag == head.etag && meta.size == head.size)
            });

        let mut remote = None;
        if remote_head.is_some() && !local_matches_remote {
            match store.get_opt(&remote_key)? {
                Some(bytes) => match Self::decode_index(&bytes) {
                    Some(index) if index.version <= CURRENT_INDEX_VERSION => {
                        remote = Some(index);
                    }
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
                None => {
                    // The object disappeared between HEAD and GET. Treat it as
                    // a missing base and keep the safe local-cache rule below.
                }
            }
        }

        let mut ignored_local_without_base = false;
        let mut cache_needs_write = false;
        let mut index = if local_matches_remote {
            local.unwrap_or_default()
        } else {
            match (local, remote) {
                (Some(local), Some(remote)) => {
                    cache_needs_write = true;
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
                    cache_needs_write = true;
                    Self::default()
                }
                (None, Some(remote)) => {
                    cache_needs_write = true;
                    remote
                }
                (None, None) => Self::default(),
            }
        };

        index.journal_segments = 0;
        let prefix = Self::journal_prefix(root);
        let journals = store.list(&prefix)?;
        let mut decoded = Vec::with_capacity(journals.len());
        for object in journals {
            match store.get_opt(&object.key) {
                Ok(Some(bytes)) => match Self::decode_journal(&bytes) {
                    Some(journal) if journal.version <= CURRENT_INDEX_VERSION => {
                        decoded.push((object.key, journal));
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
        // Tombstones make replay order significant, so apply journals by
        // their recorded time rather than by object-name ordering.
        decoded.sort_by(|a, b| {
            a.1.updated_at
                .cmp(&b.1.updated_at)
                .then_with(|| a.0.cmp(&b.0))
        });
        for (_, journal) in decoded {
            index.merge_journal(journal);
            index.journal_segments += 1;
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

        // Keep the local cache aligned with the base we just read so the next
        // run can use HEAD instead of downloading it again. If writing the
        // cache fails, leave no metadata behind that could make a stale cache
        // look current.
        if cache_needs_write {
            match index.save_local(config) {
                Ok(()) => match &remote_head {
                    Some(head) if head.etag.is_some() => {
                        Self::write_local_meta(
                            config,
                            &LocalIndexMeta {
                                etag: head.etag.clone(),
                                size: head.size,
                            },
                        );
                    }
                    _ => Self::clear_local_meta(config),
                },
                Err(e) => {
                    logging::warn(format!("could not refresh local index cache: {e}"));
                    Self::clear_local_meta(config);
                }
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
        let mut decoded = Vec::with_capacity(journals.len());
        for object in journals {
            match store.get_opt(&object.key)? {
                Some(bytes) => match Self::decode_journal(&bytes) {
                    Some(journal) if journal.version <= CURRENT_INDEX_VERSION => {
                        decoded.push((object.key, journal));
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
        decoded.sort_by(|a, b| {
            a.1.updated_at
                .cmp(&b.1.updated_at)
                .then_with(|| a.0.cmp(&b.0))
        });
        let merged = decoded.len();
        for (_, journal) in decoded {
            self.merge_journal(journal);
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
            removed_blobs: Vec::new(),
            removed_packs: Vec::new(),
        };
        let json = serde_json::to_vec(&journal)?;
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        store.put(&Self::journal_key(root), &compressed)?;
        Ok(())
    }

    /// Upload a small journal that removes blobs and packs from the index.
    ///
    /// This is used after explicit edits and garbage collection so the index
    /// can be made consistent without re-uploading the full (potentially
    /// hundreds of megabytes) base object. The base is compacted on the next
    /// normal backup checkpoint.
    pub fn save_deletions(
        store: &dyn BlobStore,
        root: &str,
        removed_blobs: &[String],
        removed_packs: &[String],
    ) -> Result<()> {
        if removed_blobs.is_empty() && removed_packs.is_empty() {
            return Ok(());
        }
        let journal = IndexJournal {
            version: CURRENT_INDEX_VERSION,
            updated_at: Utc::now(),
            blobs: HashMap::new(),
            packs: HashMap::new(),
            removed_blobs: removed_blobs.to_vec(),
            removed_packs: removed_packs.to_vec(),
        };
        let json = serde_json::to_vec(&journal)?;
        let compressed = crate::core::compression::lz4_compress_with_prefix(&json);
        store.put(&Self::journal_key(root), &compressed)?;
        Ok(())
    }

    pub fn save(&self, store: &dyn BlobStore, root: &str, config: &AppConfig) -> Result<()> {
        // Encode once and reuse the bytes for both the local cache and the
        // remote compressed base. On a multi-million-blob index this halves
        // the serialization CPU and peak allocation.
        let binary = self.encode_binary()?;
        // Never overwrite the live S3 base with a new representation unless it
        // decodes back to exactly the same index. This turns a future encoder
        // bug into a failed backup instead of an unreadable dedupe index.
        let verified = Self::decode_binary(&binary)
            .ok_or_else(|| BackupError::Codec("encoded blob index failed its self-check".into()))?;
        let verified_ok = verified.blobs.len() == self.blobs.len()
            && verified.packs.len() == self.packs.len()
            && self
                .blobs
                .iter()
                .all(|(id, loc)| verified.blobs.get(id) == Some(loc))
            && self.packs.iter().all(|(path, pack)| {
                verified
                    .packs
                    .get(path)
                    .is_some_and(|other| other.path == pack.path && other.size == pack.size)
            });
        if !verified_ok {
            return Err(BackupError::Codec(
                "encoded blob index failed its self-check".into(),
            ));
        }
        // The local file is only a cache. A full or unwritable local disk must
        // not prevent the durable remote index from being written and the
        // backup from completing.
        let local_written = match Self::write_local_bytes(config, &binary) {
            Ok(()) => true,
            Err(e) => {
                logging::warn(format!("could not write local index cache: {e}"));
                false
            }
        };
        let compressed = crate::core::compression::lz4_compress_with_prefix(&binary);
        let key = Self::remote_key(root);
        store.put(&key, &compressed)?;
        // Remember which remote object this cache corresponds to. If HEAD
        // cannot confirm it, leave no metadata so the next load downloads
        // rather than trusting a possibly stale cache.
        if local_written {
            match store.head(&key) {
                Ok(Some(info)) if info.etag.is_some() => {
                    Self::write_local_meta(
                        config,
                        &LocalIndexMeta {
                            etag: info.etag,
                            size: info.size,
                        },
                    );
                }
                Ok(_) => Self::clear_local_meta(config),
                Err(e) => {
                    logging::warn(format!("could not record remote index metadata: {e}"));
                    Self::clear_local_meta(config);
                }
            }
        } else {
            Self::clear_local_meta(config);
        }
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
        self.prune_to_delta(keep).removed_objects
    }

    /// Like [`Self::prune_to`], but also reports the exact blob identifiers
    /// and pack paths that were dropped so they can be recorded in a small
    /// deletion journal instead of rewriting the full base index.
    pub fn prune_to_delta(&mut self, keep: &HashSet<String>) -> PruneDelta {
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

        for id in &remove_ids {
            self.blobs.remove(id);
        }
        // A pack is unreachable when no surviving blob points into it. This
        // also drops orphan pack records that have no indexed members.
        let pack_paths: Vec<String> = self.packs.keys().cloned().collect();
        let mut removed_pack_paths = Vec::new();
        for path in pack_paths {
            if pack_kept.get(&path).copied().unwrap_or(0) == 0 {
                self.packs.remove(&path);
                removed_objects.push(path.clone());
                removed_pack_paths.push(path);
            }
        }

        self.updated_at = Utc::now();
        removed_objects.sort();
        removed_objects.dedup();
        remove_ids.sort();
        remove_ids.dedup();
        removed_pack_paths.sort();
        removed_pack_paths.dedup();
        PruneDelta {
            removed_objects,
            removed_blob_ids: remove_ids,
            removed_pack_paths,
        }
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
    fn prune_delta_reports_blob_and_pack_tombstones() {
        let mut idx = BlobIndex::default();
        idx.add_blob(loc("a", true, "blobpacks/p1"));
        idx.add_blob(loc("b", true, "blobpacks/p2"));
        idx.add_pack("blobpacks/p1".into(), 10);
        idx.add_pack("blobpacks/p2".into(), 20);
        idx.add_blob(loc("c", false, "standardobjects/c"));
        let keep: HashSet<String> = ["a".into()].into_iter().collect();
        let delta = idx.prune_to_delta(&keep);
        assert_eq!(delta.removed_blob_ids, vec!["b", "c"]);
        assert_eq!(delta.removed_pack_paths, vec!["blobpacks/p2"]);
        assert!(
            delta
                .removed_objects
                .contains(&"standardobjects/c".to_string())
        );
        assert!(delta.removed_objects.contains(&"blobpacks/p2".to_string()));
    }

    #[test]
    fn deletion_journal_removes_blobs_and_packs_on_load() {
        let dir = std::env::temp_dir().join(format!("index-tombstone-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();
        let mut index = BlobIndex::default();
        index.add_blob(loc("a", false, "standardobjects/a"));
        index.add_blob(loc("b", true, "blobpacks/p1"));
        index.add_pack("blobpacks/p1".into(), 10);
        index.save(&store, &root, &config).unwrap();

        BlobIndex::save_deletions(
            &store,
            &root,
            &["b".to_string()],
            &["blobpacks/p1".to_string()],
        )
        .unwrap();

        let loaded = BlobIndex::load(&store, &root, &config).unwrap();
        assert!(loaded.blob("a").is_some());
        assert!(loaded.blob("b").is_none());
        assert!(!loaded.packs.contains_key("blobpacks/p1"));

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
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
        // Simulate a future binary layout without using the current writer,
        // which always emits CURRENT_INDEX_VERSION.
        let mut future = Vec::new();
        future.extend_from_slice(b"QIDX");
        future.extend_from_slice(&(CURRENT_INDEX_VERSION + 1).to_le_bytes());
        future.extend_from_slice(&0i64.to_le_bytes());
        store
            .put(
                &BlobIndex::remote_key(&root),
                &crate::core::compression::lz4_compress_with_prefix(&future),
            )
            .unwrap();

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
            removed_blobs: Vec::new(),
            removed_packs: Vec::new(),
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

    #[test]
    fn failed_save_does_not_overwrite_live_remote_index() {
        let dir = std::env::temp_dir().join(format!("index-failed-save-{}", uuid::Uuid::new_v4()));
        let store = crate::core::store::LocalStore::new(&dir).unwrap();
        let config = AppConfig {
            bucket: "bucket".into(),
            prefix: "qia-backups".into(),
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            ..AppConfig::default()
        };
        let root = config.key_root();

        let mut good = BlobIndex::default();
        good.add_blob(loc("good", false, "standardobjects/go/od/good"));
        good.save(&store, &root, &config).unwrap();
        let before = store.get(&BlobIndex::remote_key(&root)).unwrap();

        let mut corrupt = BlobIndex::default();
        corrupt.blobs.insert(
            "wrong-key".into(),
            loc("other-id", false, "standardobjects/ot/he/other-id"),
        );
        let err = corrupt.save(&store, &root, &config).unwrap_err();
        assert!(
            err.to_string().contains("mismatched identifier"),
            "got {err:?}"
        );

        let after = store.get(&BlobIndex::remote_key(&root)).unwrap();
        assert_eq!(
            before, after,
            "a failed migration must leave the live S3 base untouched"
        );

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_file(BlobIndex::local_meta_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn binary_index_roundtrips_flags_offsets_and_legacy_ids() {
        let mut index = BlobIndex::default();
        index.add_pack("blobpacks/p1".into(), 100);
        index.add_blob(BlobLoc {
            blob_identifier: format!("{:064x}", 1),
            is_packed: true,
            relative_path: "blobpacks/p1".into(),
            offset: 5,
            length: 7,
            stretch_encryption_key: true,
            compression_type: 2,
        });
        index.add_blob(BlobLoc {
            blob_identifier: "legacy-id".into(),
            is_packed: false,
            relative_path: "standardobjects/le/legacy-id".into(),
            offset: 0,
            length: 42,
            stretch_encryption_key: false,
            compression_type: 0,
        });

        let encoded = index.encode_binary().unwrap();
        let decoded = BlobIndex::decode_binary(&encoded).unwrap();
        assert_eq!(decoded.version, CURRENT_INDEX_VERSION);
        assert_eq!(decoded.blobs.len(), 2);
        assert_eq!(decoded.packs["blobpacks/p1"].size, 100);
        let packed = decoded.blob(&format!("{:064x}", 1)).unwrap();
        assert!(packed.is_packed);
        assert_eq!(packed.offset, 5);
        assert_eq!(packed.length, 7);
        assert!(packed.stretch_encryption_key);
        assert_eq!(packed.compression_type, 2);
        let standalone = decoded.blob("legacy-id").unwrap();
        assert!(!standalone.is_packed);
        assert_eq!(standalone.relative_path, "standardobjects/le/legacy-id");
        assert_eq!(standalone.length, 42);
    }

    #[test]
    fn binary_index_is_far_smaller_than_json() {
        let mut index = BlobIndex::default();
        index.add_pack("blobpacks/p1.blobpack".into(), 1 << 20);
        for i in 0..1000u64 {
            index.add_blob(BlobLoc {
                blob_identifier: format!("{i:064x}"),
                is_packed: true,
                relative_path: "blobpacks/p1.blobpack".into(),
                offset: i * 1024,
                length: 1024,
                stretch_encryption_key: false,
                compression_type: 2,
            });
        }
        let json = serde_json::to_vec(&index).unwrap();
        let binary = index.encode_binary().unwrap();
        assert!(
            binary.len() * 3 < json.len(),
            "binary index ({} bytes) should be much smaller than JSON ({} bytes)",
            binary.len(),
            json.len()
        );
    }

    struct CountingStore {
        inner: crate::core::store::LocalStore,
        etag: parking_lot::Mutex<String>,
        gets: std::sync::atomic::AtomicUsize,
    }

    impl BlobStore for CountingStore {
        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get(key)
        }

        fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.get_opt(key)
        }

        fn head(&self, key: &str) -> Result<Option<crate::core::store::ObjectInfo>> {
            Ok(self.inner.head(key)?.map(|mut info| {
                info.etag = Some(self.etag.lock().clone());
                info
            }))
        }

        fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<crate::core::store::ObjectInfo>> {
            self.inner.list(prefix)
        }

        fn describe(&self) -> String {
            self.inner.describe()
        }
    }

    #[test]
    fn unchanged_remote_index_is_not_downloaded_again() {
        let dir = std::env::temp_dir().join(format!("index-etag-test-{}", uuid::Uuid::new_v4()));
        let inner = crate::core::store::LocalStore::new(&dir).unwrap();
        let store = CountingStore {
            inner,
            etag: parking_lot::Mutex::new("etag-1".into()),
            gets: std::sync::atomic::AtomicUsize::new(0),
        };
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

        let loaded = BlobIndex::load(&store, &root, &config).unwrap();
        assert!(loaded.blob("a").is_some());
        assert_eq!(
            store.gets.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a matching local cache must be used without GET"
        );

        *store.etag.lock() = "etag-2".into();
        store.gets.store(0, std::sync::atomic::Ordering::SeqCst);
        let loaded = BlobIndex::load(&store, &root, &config).unwrap();
        assert!(loaded.blob("a").is_some());
        assert_eq!(
            store.gets.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a changed remote base must be downloaded"
        );

        store.gets.store(0, std::sync::atomic::Ordering::SeqCst);
        BlobIndex::load(&store, &root, &config).unwrap();
        assert_eq!(
            store.gets.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the refreshed cache must skip the next GET"
        );

        std::fs::remove_file(BlobIndex::local_path(&config)).ok();
        std::fs::remove_file(BlobIndex::local_meta_path(&config)).ok();
        std::fs::remove_dir_all(&dir).ok();
    }
}
