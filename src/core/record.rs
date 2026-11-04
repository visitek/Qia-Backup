//! Backup-record paths, encoding and decoding.

use base64::Engine;
use chrono::{TimeZone, Utc};

use crate::core::codec;
use crate::core::compression::{lz4_compress_with_prefix, lz4_decompress};
use crate::core::crypto::{ObjectEncryptor, decrypt_object, is_encrypted_object};
use crate::core::error::{BackupError, Result};
use crate::core::model::{BACKUP_FORMAT_VERSION, BackupRecord, NODE_TREE_VERSION, Node};
use crate::core::store::{BlobStore, normalize_key};

pub fn root_key(root: &str, relative: &str) -> String {
    normalize_key(root, relative)
}

pub fn backup_config_key(root: &str) -> String {
    root_key(root, "backupconfig.json")
}

pub fn backup_plan_key(root: &str) -> String {
    root_key(root, "backupplan.json")
}

pub fn backup_folders_key(root: &str) -> String {
    root_key(root, "backupfolders.json")
}

pub fn backup_folder_key(root: &str, folder_uuid: &str) -> String {
    root_key(
        root,
        &format!("backupfolders/{folder_uuid}/backupfolder.json"),
    )
}

/// Marker for an in-progress/aborted run. It lets the explorer verify that
/// local partial-tree data still corresponds to this S3 destination before
/// showing or restoring it, and a leftover marker tells the next successful
/// backup that the previous run may have left unjournaled objects behind.
pub fn partial_run_key(root: &str) -> String {
    root_key(root, "partialrun.json")
}

pub fn partial_run_exists(store: &dyn BlobStore, root: &str) -> Result<bool> {
    Ok(store.head(&partial_run_key(root))?.is_some())
}

/// Marker for orphaned uploads that still need an authorized cleanup. A
/// successful run that is not authorized to delete orphans leaves this marker
/// behind so a later user-confirmed run can finish the job.
pub fn cleanup_pending_key(root: &str) -> String {
    root_key(root, "cleanuppending.json")
}

pub fn cleanup_pending_exists(store: &dyn BlobStore, root: &str) -> Result<bool> {
    Ok(store.head(&cleanup_pending_key(root))?.is_some())
}

pub fn record_prefix(root: &str, folder_uuid: &str) -> String {
    root_key(root, &format!("backupfolders/{folder_uuid}/backuprecords"))
}

fn shard_for_epoch(epoch: i64) -> String {
    let day = epoch.div_euclid(86_400);
    format!("{:03}", day.rem_euclid(1000))
}

pub fn record_key(root: &str, folder_uuid: &str, epoch: i64) -> String {
    root_key(
        root,
        &format!(
            "backupfolders/{folder_uuid}/backuprecords/{}/{}.backuprecord",
            shard_for_epoch(epoch),
            epoch
        ),
    )
}

pub fn parse_record_epoch(key: &str) -> Option<i64> {
    let name = key.rsplit('/').next()?;
    let stem = name.strip_suffix(".backuprecord")?;
    stem.parse().ok()
}

pub fn encode_record(
    record: &BackupRecord,
    encryptor: Option<&mut ObjectEncryptor>,
) -> Result<Vec<u8>> {
    let mut json = serde_json::to_vec(record)?;
    json = lz4_compress_with_prefix(&json);
    if let Some(enc) = encryptor {
        json = enc.encrypt(&json)?;
    }
    Ok(json)
}

pub fn decode_record(
    bytes: &[u8],
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<BackupRecord> {
    let plain = if is_encrypted_object(bytes) {
        let keys = keys.ok_or_else(|| {
            BackupError::Crypto("backup record is encrypted but no password was supplied".into())
        })?;
        decrypt_object(keys, bytes)?
    } else {
        bytes.to_vec()
    };
    // Backup records are LZ4-compressed; be tolerant of raw JSON (for imported
    // records or future format variants).
    let json = match lz4_decompress(&plain) {
        Ok(json) => json,
        Err(_) => plain,
    };
    let record: BackupRecord = serde_json::from_slice(&json)?;
    Ok(record)
}

pub fn load_record(
    store: &dyn BlobStore,
    key: &str,
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<BackupRecord> {
    let bytes = store.get(key)?;
    decode_record(&bytes, keys)
}

fn decode_blob_bytes(
    raw: &[u8],
    loc: &crate::core::model::BlobLoc,
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<Vec<u8>> {
    let start = if loc.is_packed {
        0
    } else {
        loc.offset as usize
    };
    let end = start
        .checked_add(loc.length as usize)
        .ok_or_else(|| BackupError::Codec("blob range overflow".into()))?;
    if end > raw.len() {
        return Err(BackupError::Codec(format!(
            "blob {} range {}..{} exceeds object size {}",
            loc.blob_identifier,
            start,
            end,
            raw.len()
        )));
    }
    let slice = &raw[start..end];
    let unpacked = if loc.stretch_encryption_key {
        let keys = keys.ok_or_else(|| {
            BackupError::Crypto("blob is encrypted but no password was supplied".into())
        })?;
        decrypt_object(keys, slice)?
    } else {
        slice.to_vec()
    };
    let data = crate::core::compression::decompress_blob(&unpacked, loc.compression_type)?;
    // Verify the content address when the identifier is a SHA-256 hex string.
    if loc.blob_identifier.len() == 64 {
        let actual = crate::core::crypto::sha256_hex(&data);
        if !actual.eq_ignore_ascii_case(&loc.blob_identifier) {
            return Err(BackupError::Codec(format!(
                "blob hash mismatch for {} (got {actual})",
                loc.blob_identifier
            )));
        }
    }
    Ok(data)
}

fn tree_cache_path(id: &str) -> Option<std::path::PathBuf> {
    // Unit tests use temporary stores; caching there just pollutes the user
    // cache with throwaway objects.
    if cfg!(test) || id.len() < 4 {
        return None;
    }
    Some(
        crate::core::config::cache_dir()
            .join("treepacks")
            .join(&id[..2])
            .join(id),
    )
}

fn read_tree_cache(id: &str) -> Option<Vec<u8>> {
    std::fs::read(tree_cache_path(id)?).ok()
}

fn write_tree_cache(id: &str, bytes: &[u8]) {
    let Some(path) = tree_cache_path(id) else {
        return;
    };
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    if std::fs::write(&tmp, bytes).is_ok() && std::fs::rename(&tmp, &path).is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
}

fn remove_tree_cache(id: &str) {
    if let Some(path) = tree_cache_path(id) {
        let _ = std::fs::remove_file(path);
    }
}

/// Cache a tree object's stored (compressed/encrypted) bytes. Writing new
/// trees through to the cache avoids one S3 GET per directory on the first
/// incremental run after they were created.
pub fn cache_tree_blob(id: &str, stored: &[u8]) {
    write_tree_cache(id, stored);
}

pub fn load_node_blob(
    store: &dyn BlobStore,
    root: &str,
    loc: &crate::core::model::BlobLoc,
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<Vec<u8>> {
    let key = root_key(root, &loc.relative_path);
    // Fetch only the requested byte range for packed blobs. Downloading a
    // whole 64 MiB pack for a 1 MiB chunk makes restores of large backups
    // orders of magnitude slower.
    let raw = if loc.is_packed {
        store.get_range(&key, loc.offset, loc.length)?
    } else {
        store.get(&key)?
    };
    decode_blob_bytes(&raw, loc, keys)
}

/// Load and decode a tree blob, using the content-addressed local cache.
///
/// Trees are immutable and are read once per directory on every incremental
/// run and prune. Cache the stored (still compressed/encrypted) bytes so a
/// second run does not issue hundreds of thousands of S3 GETs. The cache works
/// for both packed and standalone trees because the blob id is the content
/// address and the cache stores exactly the blob's own byte range.
pub fn load_tree(
    store: &dyn BlobStore,
    root: &str,
    loc: &crate::core::model::BlobLoc,
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<crate::core::model::Tree> {
    let bytes = load_tree_bytes(store, root, loc, keys)?;
    codec::decode_tree(&bytes)
}

/// Load a tree only after confirming its backing object still exists in the
/// destination. Explorer/restore paths use this so a local tree cache can
/// never present a directory that was deleted from the bucket.
pub fn load_tree_checked(
    store: &dyn BlobStore,
    root: &str,
    loc: &crate::core::model::BlobLoc,
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<crate::core::model::Tree> {
    // Only a warm cache can mask a deleted object. When the cache is cold the
    // normal load already performs a remote read and fails if it is gone.
    if read_tree_cache(&loc.blob_identifier).is_some() {
        let key = root_key(root, &loc.relative_path);
        let info = match store.head(&key)? {
            Some(info) => info,
            None => {
                remove_tree_cache(&loc.blob_identifier);
                return Err(BackupError::Store(format!(
                    "backup object is missing from the destination: {key}"
                )));
            }
        };
        let end = loc
            .offset
            .checked_add(loc.length)
            .ok_or_else(|| BackupError::Codec("tree blob range overflow".into()))?;
        if info.size < end {
            remove_tree_cache(&loc.blob_identifier);
            return Err(BackupError::Store(format!(
                "backup object {key} is {} bytes but the record references bytes up to {end}",
                info.size
            )));
        }
    }
    load_tree(store, root, loc, keys)
}

fn load_tree_bytes(
    store: &dyn BlobStore,
    root: &str,
    loc: &crate::core::model::BlobLoc,
    keys: Option<&crate::core::crypto::MasterKeys>,
) -> Result<Vec<u8>> {
    if let Some(raw) = read_tree_cache(&loc.blob_identifier) {
        match decode_blob_bytes(&raw, loc, keys) {
            Ok(data) => return Ok(data),
            Err(_) => remove_tree_cache(&loc.blob_identifier),
        }
    }

    let key = root_key(root, &loc.relative_path);
    let raw = if loc.is_packed {
        store.get_range(&key, loc.offset, loc.length)?
    } else {
        store.get(&key)?
    };
    write_tree_cache(&loc.blob_identifier, &raw);
    decode_blob_bytes(&raw, loc, keys)
}

/// Build a record around a root node.
pub fn make_record(
    plan: &crate::core::model::BackupPlan,
    folder_uuid: &str,
    root_node: &Node,
    local_path: &str,
    local_mount_point: &str,
    volume_name: &str,
    disk_identifier: &str,
    source_kind: &str,
    snapshot_name: &str,
    files_count: u64,
    bytes_uploaded: u64,
    bytes_scanned: u64,
    duration_seconds: f64,
    errors: Vec<crate::core::model::BackupRecordError>,
) -> Result<BackupRecord> {
    let encoded = codec::encode_node(root_node, NODE_TREE_VERSION);
    let node = base64::engine::general_purpose::STANDARD.encode(encoded);
    let error_count = errors.len() as u64;
    Ok(BackupRecord {
        backup_plan_json: serde_json::to_value(plan)?,
        backup_folder_uuid: folder_uuid.to_string(),
        backup_plan_uuid: plan.plan_uuid.clone(),
        computer_os_type: 1,
        creation_date: Utc::now(),
        disk_identifier: disk_identifier.to_string(),
        error_count,
        backup_record_errors: errors,
        is_complete: error_count == 0,
        local_mount_point: local_mount_point.to_string(),
        local_path: local_path.to_string(),
        node,
        node_tree_version: NODE_TREE_VERSION,
        relative_path: String::new(),
        storage_class: "STANDARD".to_string(),
        version: BACKUP_FORMAT_VERSION,
        volume_name: volume_name.to_string(),
        source_kind: source_kind.to_string(),
        snapshot_name: snapshot_name.to_string(),
        files_count,
        bytes_uploaded,
        bytes_scanned,
        duration_seconds,
        included_folders: Vec::new(),
    })
}

pub fn list_record_keys(
    store: &dyn BlobStore,
    root: &str,
    folder_uuid: &str,
) -> Result<Vec<String>> {
    let prefix = record_prefix(root, folder_uuid);
    let mut keys: Vec<String> = store
        .list(&format!("{prefix}/"))?
        .into_iter()
        .filter(|o| o.key.ends_with(".backuprecord"))
        .map(|o| o.key)
        .collect();
    keys.sort();
    Ok(keys)
}

pub fn latest_record_key(
    store: &dyn BlobStore,
    root: &str,
    folder_uuid: &str,
) -> Result<Option<String>> {
    Ok(list_record_keys(store, root, folder_uuid)?
        .into_iter()
        .max_by_key(|k| parse_record_epoch(k).unwrap_or(0)))
}

pub fn record_timestamp(key: &str) -> Option<chrono::DateTime<Utc>> {
    parse_record_epoch(key).and_then(|epoch| Utc.timestamp_opt(epoch, 0).single())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_keys_are_stable() {
        let root = "prefix/set";
        let key = record_key(root, "folder", 1_700_000_000);
        assert_eq!(
            key,
            "prefix/set/backupfolders/folder/backuprecords/675/1700000000.backuprecord"
        );
        assert_eq!(parse_record_epoch(&key), Some(1_700_000_000));
    }

    #[test]
    fn record_roundtrip() {
        let node = Node::default();
        let encoded = crate::core::codec::encode_node(&node, 4);
        let record = BackupRecord {
            backup_plan_json: serde_json::json!({"name": "x"}),
            backup_folder_uuid: "f".into(),
            backup_plan_uuid: "p".into(),
            computer_os_type: 1,
            creation_date: Utc::now(),
            disk_identifier: String::new(),
            error_count: 0,
            backup_record_errors: vec![],
            is_complete: true,
            local_mount_point: "/".into(),
            local_path: "/".into(),
            node: base64::engine::general_purpose::STANDARD.encode(encoded),
            node_tree_version: 4,
            relative_path: String::new(),
            storage_class: "STANDARD".into(),
            version: 101,
            volume_name: "Data".into(),
            source_kind: "path".into(),
            snapshot_name: String::new(),
            files_count: 0,
            bytes_uploaded: 0,
            bytes_scanned: 0,
            duration_seconds: 0.0,
            included_folders: vec!["Users/me/Documents".into()],
        };
        let bytes = encode_record(&record, None).unwrap();
        let decoded = decode_record(&bytes, None).unwrap();
        assert_eq!(decoded.backup_folder_uuid, "f");
        assert_eq!(decoded.included_folders, vec!["Users/me/Documents"]);
        let node = decoded.root_node().unwrap();
        assert!(!node.is_tree);

        // Records must stay small: blob reachability comes from the
        // content-addressed tree walk, not a per-record manifest.
        let json = serde_json::to_string(&record).unwrap();
        assert!(!json.contains("blobIds"));
        assert!(!json.contains("blob_ids"));
        assert!(!json.contains("blobIdsComplete"));
    }
}
