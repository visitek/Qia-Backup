//! Restore a backup record from S3 / a local store.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use filetime::{FileTime, set_file_mtime};
use serde::Serialize;

use crate::core::config::{self, BackupListEntry};
use crate::core::content;
use crate::core::crypto::{MasterKeys, decrypt_keyset};
use crate::core::engine::CancelToken;
use crate::core::error::{BackupError, IoContext, Result};
use crate::core::logging;
use crate::core::model::{AppConfig, BackupRecord, BlobLoc, Node};
use crate::core::partial::PartialTree;
use crate::core::record;
use crate::core::store::BlobStore;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSummary {
    pub files: u64,
    pub directories: u64,
    pub bytes: u64,
    pub errors: u64,
    pub destination: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RestoreEvent {
    Phase { name: String, detail: String },
    File { path: String, size: u64 },
    Warning { message: String },
    Finished { summary: RestoreSummary },
}

/// One entry in a backed-up directory, used by the file explorer.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupEntry {
    pub name: String,
    /// Path relative to the backup root, using `/` separators.
    pub path: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub files_count: u64,
    pub mtime_sec: i64,
    /// True when this entry is excluded by the current ignore configuration.
    /// It can still be present in an older revision created before the rule.
    pub ignored: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupListing {
    pub record_key: String,
    pub path: String,
    /// True when the directory being browsed is itself ignored.
    pub ignored: bool,
    pub entries: Vec<BackupEntry>,
}

pub(crate) fn normalize_backup_path(path: &str) -> Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(BackupError::Codec(format!("invalid backup path: {path:?}")));
        }
        components.push(component);
    }
    Ok(components.join("/"))
}

pub(crate) fn path_components(path: &str) -> Result<Vec<String>> {
    let normalized = normalize_backup_path(path)?;
    if normalized.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(normalized
            .split('/')
            .map(std::string::ToString::to_string)
            .collect())
    }
}

pub(crate) fn resolve_record_key(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
) -> Result<String> {
    if let Some(key) = record_key {
        if !key.is_empty() && key != "latest" {
            return Ok(key.to_string());
        }
    }
    record::latest_record_key(store, &config.key_root(), &config.backup_set_uuid)?
        .ok_or_else(|| BackupError::Other("no backup records found".into()))
}

fn load_record_with_keys(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
) -> Result<(Option<MasterKeys>, BackupRecord, String)> {
    let root = config.key_root();
    let keys = load_keys(config, store, &root)?;
    let key = resolve_record_key(config, store, record_key)?;
    let record = record::load_record(store, &key, keys.as_ref())?;
    Ok((keys, record, key))
}

/// Resolve a path inside a record. Returns `None` when the path (or one of its
/// parents) is absent, while still surfacing real I/O or decoding errors.
pub(crate) fn find_node_opt(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    rec: &BackupRecord,
    path: &str,
) -> Result<Option<Node>> {
    let mut node = rec.root_node()?;
    for component in path_components(path)? {
        if !node.is_tree {
            return Ok(None);
        }
        let Some(loc) = node.tree_blob_loc.as_ref() else {
            return Ok(None);
        };
        let tree = record::load_tree_checked(store, root, loc, keys)?;
        match tree
            .children
            .into_iter()
            .find(|(name, _)| name == &component)
        {
            Some((_, child)) => node = child,
            None => return Ok(None),
        }
    }
    Ok(Some(node))
}

fn find_node(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    rec: &BackupRecord,
    path: &str,
) -> Result<Node> {
    find_node_opt(store, root, keys, rec, path)?
        .ok_or_else(|| BackupError::Other(format!("backup path not found: {path}")))
}

/// List the contents of a backed-up directory for the file explorer. Passing
/// `None` or `"latest"` resolves the current record; any other value selects a
/// specific revision.
pub fn list_entries(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
    path: &str,
) -> Result<BackupListing> {
    let root = config.key_root();
    let (keys, record, resolved_key) = load_record_with_keys(config, store, record_key)?;
    let normalized = normalize_backup_path(path)?;
    let node = find_node(store, &root, keys.as_ref(), &record, &normalized)?;
    if !node.is_tree {
        return Err(BackupError::Other(format!(
            "backup path is not a directory: {normalized}"
        )));
    }
    let loc = node
        .tree_blob_loc
        .as_ref()
        .ok_or_else(|| BackupError::Codec("directory node has no tree location".into()))?;
    let tree = record::load_tree_checked(store, &root, loc, keys.as_ref())?;
    let mut entries: Vec<BackupEntry> = tree
        .children
        .into_iter()
        .map(|(name, child)| backup_entry_from_node(&normalized, name, child))
        .collect();
    sort_backup_entries(&mut entries);
    let mut listing = BackupListing {
        record_key: resolved_key,
        path: normalized,
        ignored: false,
        entries,
    };
    mark_listing_ignored(&mut listing, config)?;
    Ok(listing)
}

fn backup_entry_from_node(parent: &str, name: String, child: Node) -> BackupEntry {
    BackupEntry {
        path: if parent.is_empty() {
            name.clone()
        } else {
            format!("{parent}/{name}")
        },
        name,
        is_dir: child.is_tree || Node::mode_is_dir(child.mac_st_mode),
        is_symlink: Node::mode_is_symlink(child.mac_st_mode),
        size: child.item_size,
        files_count: child.contained_files_count,
        mtime_sec: child.mtime_sec,
        ignored: false,
    }
}

/// Annotate a directory listing with the current ignore rules.
///
/// The backup walker matches a path either by its full relative path or by its
/// basename (see `engine::is_excluded`), so the explorer uses the same matcher.
/// When the browsed directory itself is ignored, every child is treated as
/// ignored because the walker would never descend into it.
fn mark_listing_ignored(listing: &mut BackupListing, config: &AppConfig) -> Result<()> {
    // A malformed exclude pattern must not make the explorer unusable; the
    // backup run will surface the configuration error when it starts.
    let Ok(matcher) = crate::core::engine::build_excludes(&config.exclude_patterns) else {
        return Ok(());
    };
    let parent_ignored = !listing.path.is_empty() && matcher.is_match(&listing.path);
    listing.ignored = parent_ignored;
    for entry in &mut listing.entries {
        entry.ignored =
            parent_ignored || matcher.is_match(&entry.path) || matcher.is_match(&entry.name);
    }
    Ok(())
}

fn sort_backup_entries(entries: &mut [BackupEntry]) {
    entries.sort_by(|a, b| {
        b.is_dir
            .cmp(&a.is_dir)
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
            .then_with(|| a.name.cmp(&b.name))
    });
}

/// Read one file's text content from a completed record for the viewer.
pub fn read_file_content(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
    path: &str,
) -> Result<content::FileContent> {
    let root = config.key_root();
    let (keys, record, _) = load_record_with_keys(config, store, record_key)?;
    let normalized = normalize_backup_path(path)?;
    let node = find_node(store, &root, keys.as_ref(), &record, &normalized)?;
    content::node_content(&normalized, &node, store, &root, keys.as_ref())
}

/// Restore one file or folder from a backup record into `destination`.
///
/// A selected folder is created inside `destination` under its own name; a
/// selected file is written as `destination/<file name>`. An empty path
/// restores the whole record's contents directly into `destination`.
pub fn restore_entry(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
    path: &str,
    destination: &Path,
    cancel: CancelToken,
    progress: &mut dyn FnMut(RestoreEvent),
) -> Result<RestoreSummary> {
    let root = config.key_root();
    let (keys, record, resolved_key) = load_record_with_keys(config, store, record_key)?;
    let normalized = normalize_backup_path(path)?;
    let node = find_node(store, &root, keys.as_ref(), &record, &normalized)?;

    let destination = destination.to_path_buf();
    fs::create_dir_all(&destination).at(&destination)?;
    let (target, rel) = if normalized.is_empty() {
        (destination.clone(), String::new())
    } else {
        let name = normalized
            .rsplit('/')
            .next()
            .ok_or_else(|| BackupError::Codec("empty restore path".into()))?
            .to_string();
        (safe_child(&destination, &name)?, name)
    };

    let mut summary = RestoreSummary {
        files: 0,
        directories: 0,
        bytes: 0,
        errors: 0,
        destination: destination.to_string_lossy().to_string(),
    };
    progress(RestoreEvent::Phase {
        name: "download".into(),
        detail: format!("record {resolved_key}"),
    });
    progress(RestoreEvent::Phase {
        name: "restore".into(),
        detail: if normalized.is_empty() {
            format!("whole backup -> {}", destination.display())
        } else {
            format!("{normalized} -> {}", destination.display())
        },
    });
    restore_node(
        store,
        &root,
        keys.as_ref(),
        &node,
        &target,
        &rel,
        &cancel,
        progress,
        &mut summary,
    )?;
    logging::info(format!(
        "entry restore complete: {normalized:?} -> {}, {} files, {} bytes, {} errors",
        destination.display(),
        summary.files,
        summary.bytes,
        summary.errors
    ));
    (progress)(RestoreEvent::Finished {
        summary: summary.clone(),
    });
    Ok(summary)
}

/// List a directory from the in-progress backup's live partial tree.
fn ensure_partial_destination(store: &dyn BlobStore, root: &str) -> Result<()> {
    if record::partial_run_exists(store, root)? {
        return Ok(());
    }
    Err(BackupError::Other(
        "the destination no longer contains this partial backup; local partial data was discarded"
            .into(),
    ))
}

pub fn list_partial_entries(
    config: &AppConfig,
    store: &dyn BlobStore,
    partial: &PartialTree,
    path: &str,
) -> Result<BackupListing> {
    let root = config.key_root();
    ensure_partial_destination(store, &root)?;
    let keys = load_keys(config, store, &root)?;
    let normalized = normalize_backup_path(path)?;
    let children = partial_dir_children(store, &root, keys.as_ref(), partial, &normalized)?;
    let mut entries: Vec<BackupEntry> = children
        .into_iter()
        .map(|(name, child)| backup_entry_from_node(&normalized, name, child))
        .collect();
    sort_backup_entries(&mut entries);
    let mut listing = BackupListing {
        record_key: "partial".into(),
        path: normalized,
        ignored: false,
        entries,
    };
    mark_listing_ignored(&mut listing, config)?;
    Ok(listing)
}

/// Read one file's text content from the live partial backup for the viewer.
pub fn read_partial_file_content(
    config: &AppConfig,
    store: &dyn BlobStore,
    partial: &PartialTree,
    path: &str,
) -> Result<content::FileContent> {
    let root = config.key_root();
    ensure_partial_destination(store, &root)?;
    let keys = load_keys(config, store, &root)?;
    let (normalized, node) = partial_resolve_node(store, &root, keys.as_ref(), partial, path)?;
    content::node_content(&normalized, &node, store, &root, keys.as_ref())
}

fn partial_dir_children(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    partial: &PartialTree,
    path: &str,
) -> Result<Vec<(String, Node)>> {
    let view = partial.dir_view(path).ok_or_else(|| {
        BackupError::Other(format!("partial backup path is not a directory: {path}"))
    })?;
    if !view.node.is_tree {
        return Err(BackupError::Other(format!(
            "partial backup path is not a directory: {path}"
        )));
    }
    if let Some(children) = view.children {
        return Ok(children);
    }
    let loc = view.node.tree_blob_loc.as_ref().ok_or_else(|| {
        BackupError::Codec("completed partial directory has no tree location".into())
    })?;
    Ok(record::load_tree_checked(store, root, loc, keys)?.children)
}

pub(crate) fn partial_resolve_node(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    partial: &PartialTree,
    path: &str,
) -> Result<(String, Node)> {
    let normalized = normalize_backup_path(path)?;
    if normalized.is_empty() {
        let view = partial
            .dir_view("")
            .ok_or_else(|| BackupError::Other("no partial backup data available".into()))?;
        return Ok((normalized, view.node));
    }
    let (parent, name) = normalized
        .rsplit_once('/')
        .map(|(parent, name)| (parent, name))
        .unwrap_or(("", normalized.as_str()));
    let children = partial_dir_children(store, root, keys, partial, parent)?;
    let node = children
        .into_iter()
        .find(|(child_name, _)| child_name == name)
        .map(|(_, child)| child)
        .ok_or_else(|| {
            BackupError::Other(format!("partial backup path not found: {normalized}"))
        })?;
    Ok((normalized, node))
}

fn restore_partial_node(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    partial: &PartialTree,
    path: &str,
    node: &Node,
    target: &Path,
    rel: &str,
    cancel: &CancelToken,
    progress: &mut dyn FnMut(RestoreEvent),
    summary: &mut RestoreSummary,
) -> Result<()> {
    cancel.check()?;
    if node.is_tree {
        fs::create_dir_all(target).at(target)?;
        let children = partial_dir_children(store, root, keys, partial, path)?;
        for (name, child) in &children {
            cancel.check()?;
            let child_target = safe_child(target, name)?;
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            let child_path = if path.is_empty() {
                name.clone()
            } else {
                format!("{path}/{name}")
            };
            if let Err(e) = restore_partial_node(
                store,
                root,
                keys,
                partial,
                &child_path,
                child,
                &child_target,
                &child_rel,
                cancel,
                progress,
                summary,
            ) {
                match e {
                    BackupError::Cancelled => return Err(e),
                    other => {
                        summary.errors += 1;
                        logging::warn(format!("partial restore {child_rel}: {other}"));
                        (progress)(RestoreEvent::Warning {
                            message: format!("{child_rel}: {other}"),
                        });
                    }
                }
            }
        }
        if let Some(loc) = &node.xattrs_blob_locs.first() {
            let json = record::load_node_blob(store, root, loc, keys)?;
            apply_xattrs(target, &parse_xattrs(&json));
        }
        apply_metadata(target, node);
        (progress)(RestoreEvent::File {
            path: rel.to_string(),
            size: 0,
        });
        Ok(())
    } else {
        restore_node(
            store, root, keys, node, target, rel, cancel, progress, summary,
        )
    }
}

/// Restore one file or folder from the live partial tree. This is best-effort:
/// a file whose blobs are still queued for upload will report an error.
pub fn restore_partial_entry(
    config: &AppConfig,
    store: &dyn BlobStore,
    partial: &PartialTree,
    path: &str,
    destination: &Path,
    cancel: CancelToken,
    progress: &mut dyn FnMut(RestoreEvent),
) -> Result<RestoreSummary> {
    let root = config.key_root();
    ensure_partial_destination(store, &root)?;
    let keys = load_keys(config, store, &root)?;
    let (normalized, node) = partial_resolve_node(store, &root, keys.as_ref(), partial, path)?;
    let destination = destination.to_path_buf();
    fs::create_dir_all(&destination).at(&destination)?;
    let (target, rel) = if normalized.is_empty() {
        (destination.clone(), String::new())
    } else {
        let name = normalized
            .rsplit('/')
            .next()
            .ok_or_else(|| BackupError::Codec("empty restore path".into()))?
            .to_string();
        (safe_child(&destination, &name)?, name)
    };

    let mut summary = RestoreSummary {
        files: 0,
        directories: 0,
        bytes: 0,
        errors: 0,
        destination: destination.to_string_lossy().to_string(),
    };
    progress(RestoreEvent::Phase {
        name: "download".into(),
        detail: "partial backup".into(),
    });
    progress(RestoreEvent::Phase {
        name: "restore".into(),
        detail: if normalized.is_empty() {
            format!("partial backup -> {}", destination.display())
        } else {
            format!("{normalized} -> {}", destination.display())
        },
    });
    restore_partial_node(
        store,
        &root,
        keys.as_ref(),
        partial,
        &normalized,
        &node,
        &target,
        &rel,
        &cancel,
        progress,
        &mut summary,
    )?;
    logging::info(format!(
        "partial restore complete: {normalized:?} -> {}, {} files, {} bytes, {} errors",
        destination.display(),
        summary.files,
        summary.bytes,
        summary.errors
    ));
    (progress)(RestoreEvent::Finished {
        summary: summary.clone(),
    });
    Ok(summary)
}

pub fn load_keys(
    config: &AppConfig,
    store: &dyn BlobStore,
    root: &str,
) -> Result<Option<MasterKeys>> {
    let key = record::root_key(root, "encryptedkeyset.dat");
    match store.get_opt(&key)? {
        Some(bytes) => {
            if config.encryption_password.is_empty() {
                return Err(BackupError::Config(
                    "the backup set is encrypted; enter its password".into(),
                ));
            }
            Ok(Some(decrypt_keyset(&bytes, &config.encryption_password)?))
        }
        // A new encrypted backup set has no keyset until its first backup;
        // listing records is still valid in that state.
        None => Ok(None),
    }
}

pub fn list_backups(
    store: &dyn BlobStore,
    root: &str,
    folder_uuid: &str,
    keys: Option<&MasterKeys>,
) -> Result<Vec<BackupListEntry>> {
    let keys_vec = record::list_record_keys(store, root, folder_uuid)?;
    let mut out = Vec::new();
    for key in keys_vec {
        let Some(timestamp) = record::record_timestamp(&key) else {
            continue;
        };
        let size = store.head(&key)?.map(|o| o.size).unwrap_or(0);
        let mut entry = BackupListEntry {
            timestamp,
            key: key.clone(),
            size,
            is_complete: None,
            files_count: None,
            bytes_uploaded: None,
            record_name: key.rsplit('/').next().unwrap_or(&key).to_string(),
        };
        // Records are small; decoding gives the UI useful summary fields.
        // Ignore corrupt records here and surface them only on restore.
        if let Ok(bytes) = store.get(&key) {
            if let Ok(record) = record::decode_record(&bytes, keys) {
                entry.is_complete = Some(record.is_complete);
                entry.files_count = Some(record.files_count);
                entry.bytes_uploaded = Some(record.bytes_uploaded);
            }
        }
        out.push(entry);
    }
    out.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    Ok(out)
}

fn safe_child(parent: &Path, name: &str) -> Result<PathBuf> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(BackupError::Codec(format!(
            "unsafe path component in backup record: {name:?}"
        )));
    }
    Ok(parent.join(name))
}

fn parse_xattrs(json: &[u8]) -> BTreeMap<String, Vec<u8>> {
    let raw: BTreeMap<String, String> = match serde_json::from_slice(json) {
        Ok(v) => v,
        Err(_) => return BTreeMap::new(),
    };
    let mut out = BTreeMap::new();
    for (name, encoded) in raw {
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded.as_bytes()) {
            out.insert(name, bytes);
        }
    }
    out
}

fn apply_metadata(path: &Path, node: &Node) {
    if node.mac_st_mode & libc::S_IFMT as u32 != libc::S_IFLNK as u32 {
        let mode = node.mac_st_mode & 0o7777;
        if mode != 0 {
            let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
        }
    }
    let mtime =
        FileTime::from_unix_time(node.mtime_sec, node.mtime_nsec.clamp(0, 999_999_999) as u32);
    let _ = set_file_mtime(path, mtime);
}

fn apply_xattrs(path: &Path, xattrs: &BTreeMap<String, Vec<u8>>) {
    for (name, value) in xattrs {
        if name.starts_with("__backup_") {
            continue;
        }
        if let Err(e) = xattr::set(path, name, value) {
            logging::debug(format!(
                "could not restore xattr {name} on {}: {e}",
                path.display()
            ));
        }
    }
}

const RESTORE_BATCH: usize = 8;
const RESTORE_BATCH_BYTES: u64 = 32 * 1024 * 1024;

/// Fetch a batch of blobs concurrently. The result order matches the input so
/// callers can stream the file in order without buffering the whole file.
fn fetch_blob_batch(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    locs: &[BlobLoc],
) -> Vec<Result<Vec<u8>>> {
    // Most files in a full OS are small and have a single chunk. Avoid
    // spawning a scoped thread for every one of them.
    if locs.len() == 1 {
        return vec![record::load_node_blob(store, root, &locs[0], keys)];
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = locs
            .iter()
            .map(|loc| scope.spawn(move || record::load_node_blob(store, root, loc, keys)))
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(BackupError::Other("restore worker panicked".into())))
            })
            .collect()
    })
}

fn next_batch_end(locs: &[BlobLoc], start: usize) -> usize {
    let mut end = start;
    let mut bytes = 0u64;
    while end < locs.len() && end - start < RESTORE_BATCH {
        let len = locs[end].length;
        if end > start && bytes.saturating_add(len) > RESTORE_BATCH_BYTES {
            break;
        }
        bytes = bytes.saturating_add(len);
        end += 1;
    }
    end.max(start + 1)
}

fn write_file_data(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    node: &Node,
    target: &Path,
) -> Result<u64> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).at(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(target)
        .at(target)?;

    if node.holes.is_empty() {
        // Stream chunks to disk as they arrive instead of buffering the whole
        // file, which used to OOM on multi-GB video/disk-image files.
        let mut written = 0u64;
        let mut start = 0;
        while start < node.data_blob_locs.len() {
            let end = next_batch_end(&node.data_blob_locs, start);
            let batch = &node.data_blob_locs[start..end];
            for chunk in fetch_blob_batch(store, root, keys, batch) {
                let chunk = chunk?;
                file.write_all(&chunk).at(target)?;
                written += chunk.len() as u64;
            }
            start = end;
        }
        file.flush().at(target)?;
        apply_metadata(target, node);
        return Ok(written);
    }

    // Sparse files are rare; keep the existing hole-aware layout and buffer
    // only the actual data blobs (not the logical hole bytes).
    let mut data = Vec::with_capacity(node.item_size.min(16 * 1024 * 1024) as usize);
    for loc in &node.data_blob_locs {
        data.extend_from_slice(&record::load_node_blob(store, root, loc, keys)?);
    }
    file.set_len(node.item_size).at(target)?;
    let mut holes = node.holes.clone();
    holes.sort();
    let mut data_pos = 0usize;
    let mut logical = 0u64;
    for (hole_offset, hole_len) in holes {
        if hole_offset > logical {
            let gap = (hole_offset - logical) as usize;
            let end = (data_pos + gap).min(data.len());
            if end > data_pos {
                file.seek(SeekFrom::Start(logical)).at(target)?;
                file.write_all(&data[data_pos..end]).at(target)?;
                data_pos = end;
            }
        }
        logical = hole_offset.saturating_add(hole_len);
    }
    if logical < node.item_size {
        file.seek(SeekFrom::Start(logical)).at(target)?;
        let remaining = &data[data_pos.min(data.len())..];
        file.write_all(remaining).at(target)?;
    }
    file.flush().at(target)?;
    apply_metadata(target, node);
    Ok(data.len() as u64)
}

fn restore_node(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&MasterKeys>,
    node: &Node,
    target: &Path,
    rel: &str,
    cancel: &CancelToken,
    progress: &mut dyn FnMut(RestoreEvent),
    summary: &mut RestoreSummary,
) -> Result<()> {
    cancel.check()?;
    if node.is_tree {
        fs::create_dir_all(target).at(target)?;
        let tree_loc: &BlobLoc = node
            .tree_blob_loc
            .as_ref()
            .ok_or_else(|| BackupError::Codec("directory node has no tree location".into()))?;
        let tree = record::load_tree_checked(store, root, tree_loc, keys)?;
        for (name, child) in &tree.children {
            cancel.check()?;
            let child_target = safe_child(target, name)?;
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if let Err(e) = restore_node(
                store,
                root,
                keys,
                child,
                &child_target,
                &child_rel,
                cancel,
                progress,
                summary,
            ) {
                match e {
                    BackupError::Cancelled => return Err(e),
                    other => {
                        summary.errors += 1;
                        logging::warn(format!("restore {child_rel}: {other}"));
                        (progress)(RestoreEvent::Warning {
                            message: format!("{child_rel}: {other}"),
                        });
                    }
                }
            }
        }
        if let Some(loc) = &node.xattrs_blob_locs.first() {
            let json = record::load_node_blob(store, root, loc, keys)?;
            apply_xattrs(target, &parse_xattrs(&json));
        }
        apply_metadata(target, node);
        (progress)(RestoreEvent::File {
            path: rel.to_string(),
            size: 0,
        });
        Ok(())
    } else if Node::mode_is_symlink(node.mac_st_mode) {
        let mut xattrs = BTreeMap::new();
        if let Some(loc) = &node.xattrs_blob_locs.first() {
            let json = record::load_node_blob(store, root, loc, keys)?;
            xattrs = parse_xattrs(&json);
        }
        let target_path = xattrs
            .get("__backup_symlink_target")
            .map(|v| String::from_utf8_lossy(v).to_string())
            .ok_or_else(|| BackupError::Codec("symlink node has no target".into()))?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).at(parent)?;
        }
        let _ = fs::remove_file(target);
        std::os::unix::fs::symlink(&target_path, target).at(target)?;
        summary.files += 1;
        (progress)(RestoreEvent::File {
            path: rel.to_string(),
            size: target_path.len() as u64,
        });
        Ok(())
    } else if Node::mode_is_dir(node.mac_st_mode) {
        fs::create_dir_all(target).at(target)?;
        apply_metadata(target, node);
        summary.directories += 1;
        Ok(())
    } else {
        let bytes = write_file_data(store, root, keys, node, target)?;
        if let Some(loc) = &node.xattrs_blob_locs.first() {
            let json = record::load_node_blob(store, root, loc, keys)?;
            apply_xattrs(target, &parse_xattrs(&json));
        }
        summary.files += 1;
        summary.bytes += bytes;
        (progress)(RestoreEvent::File {
            path: rel.to_string(),
            size: bytes,
        });
        Ok(())
    }
}

pub fn restore_backup(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: &str,
    destination: &Path,
    cancel: CancelToken,
    progress: &mut dyn FnMut(RestoreEvent),
) -> Result<RestoreSummary> {
    let root = config.key_root();
    let keys = load_keys(config, store, &root)?;
    progress(RestoreEvent::Phase {
        name: "download".into(),
        detail: format!("record {record_key}"),
    });
    let record = record::load_record(store, record_key, keys.as_ref())?;
    let root_node = record.root_node()?;
    let destination = destination.to_path_buf();
    fs::create_dir_all(&destination).at(&destination)?;
    let mut summary = RestoreSummary {
        files: 0,
        directories: 0,
        bytes: 0,
        errors: 0,
        destination: destination.to_string_lossy().to_string(),
    };
    progress(RestoreEvent::Phase {
        name: "restore".into(),
        detail: destination.to_string_lossy().to_string(),
    });
    restore_node(
        store,
        &root,
        keys.as_ref(),
        &root_node,
        &destination,
        "",
        &cancel,
        progress,
        &mut summary,
    )?;
    logging::info(format!(
        "restore complete: {} files, {} directories, {} bytes, {} errors",
        summary.files, summary.directories, summary.bytes, summary.errors
    ));
    (progress)(RestoreEvent::Finished {
        summary: summary.clone(),
    });
    Ok(summary)
}

/// Restore the latest record. Convenience for the CLI and UI.
pub fn restore_latest(
    config: &AppConfig,
    store: &dyn BlobStore,
    destination: &Path,
    cancel: CancelToken,
    progress: &mut dyn FnMut(RestoreEvent),
) -> Result<RestoreSummary> {
    let root = config.key_root();
    let key = record::latest_record_key(store, &root, &config.backup_set_uuid)?
        .ok_or_else(|| BackupError::Other("no backup records found".into()))?;
    restore_backup(config, store, &key, destination, cancel, progress)
}

/// Load a record for display in the UI without restoring it.
pub fn load_record_detail(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: &str,
) -> Result<BackupRecord> {
    let root = config.key_root();
    let keys = load_keys(config, store, &root)?;
    record::load_record(store, record_key, keys.as_ref())
}

pub fn default_restore_destination() -> PathBuf {
    config::home_dir()
        .join("Downloads")
        .join("Qia Backup Restore")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::engine::{CancelToken, run_backup};
    use crate::core::model::AppConfig;
    use crate::core::store::LocalStore;

    #[test]
    fn restore_roundtrip() {
        let src = std::env::temp_dir().join(format!("restore-src-{}", uuid::Uuid::new_v4()));
        let store_dir =
            std::env::temp_dir().join(format!("restore-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("restore-dst-{}", uuid::Uuid::new_v4()));
        let dst_whole =
            std::env::temp_dir().join(format!("restore-whole-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("a.txt"), b"alpha").unwrap();
        std::fs::write(src.join("nested/b.bin"), vec![3u8; 50_000]).unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "b".into();
        cfg.region = "r".into();
        cfg.access_key_id = "a".into();
        cfg.secret_access_key = "s".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;

        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let summary = restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(summary.files >= 2);
        assert_eq!(std::fs::read(dst.join("a.txt")).unwrap(), b"alpha");
        assert_eq!(
            std::fs::read(dst.join("nested/b.bin")).unwrap(),
            vec![3u8; 50_000]
        );

        // The Files view restores a whole record through `restore_entry` with
        // an empty path; keep that path covered now that the separate Restore
        // page is gone.
        restore_entry(
            &cfg,
            &store,
            None,
            "",
            &dst_whole,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(std::fs::read(dst_whole.join("a.txt")).unwrap(), b"alpha");
        assert_eq!(
            std::fs::read(dst_whole.join("nested/b.bin")).unwrap(),
            vec![3u8; 50_000]
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
        std::fs::remove_dir_all(&dst).ok();
        std::fs::remove_dir_all(&dst_whole).ok();
    }

    #[test]
    fn explorer_lists_and_restores_specific_revision() {
        let src = std::env::temp_dir().join(format!("explore-src-{}", uuid::Uuid::new_v4()));
        let store_dir =
            std::env::temp_dir().join(format!("explore-store-{}", uuid::Uuid::new_v4()));
        let dst_old = std::env::temp_dir().join(format!("explore-old-{}", uuid::Uuid::new_v4()));
        let dst_cur = std::env::temp_dir().join(format!("explore-cur-{}", uuid::Uuid::new_v4()));
        let dst_dir = std::env::temp_dir().join(format!("explore-dir-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("keep.txt"), b"first revision").unwrap();
        std::fs::write(src.join("nested/old.txt"), b"old nested").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "b".into();
        cfg.region = "r".into();
        cfg.access_key_id = "a".into();
        cfg.secret_access_key = "s".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        std::fs::write(src.join("keep.txt"), b"second revision").unwrap();
        std::fs::write(src.join("nested/new.txt"), b"new nested").unwrap();
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        let keys = record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid).unwrap();
        let latest = record::latest_record_key(&store, &cfg.key_root(), &cfg.backup_set_uuid)
            .unwrap()
            .unwrap();
        let older = keys.into_iter().find(|key| key != &latest).unwrap();

        let root_listing = list_entries(&cfg, &store, None, "").unwrap();
        assert_eq!(root_listing.record_key, latest);
        assert!(root_listing.entries.iter().any(|e| e.name == "keep.txt"));
        assert!(
            root_listing
                .entries
                .iter()
                .any(|e| e.name == "nested" && e.is_dir)
        );
        assert!(root_listing.entries[0].is_dir, "folders sort before files");

        let old_nested = list_entries(&cfg, &store, Some(&older), "nested").unwrap();
        assert!(old_nested.entries.iter().any(|e| e.name == "old.txt"));
        assert!(!old_nested.entries.iter().any(|e| e.name == "new.txt"));

        restore_entry(
            &cfg,
            &store,
            Some(&older),
            "keep.txt",
            &dst_old,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(
            std::fs::read(dst_old.join("keep.txt")).unwrap(),
            b"first revision"
        );

        restore_entry(
            &cfg,
            &store,
            None,
            "keep.txt",
            &dst_cur,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(
            std::fs::read(dst_cur.join("keep.txt")).unwrap(),
            b"second revision"
        );

        restore_entry(
            &cfg,
            &store,
            None,
            "nested",
            &dst_dir,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(
            std::fs::read(dst_dir.join("nested/new.txt")).unwrap(),
            b"new nested"
        );
        assert_eq!(
            std::fs::read(dst_dir.join("nested/old.txt")).unwrap(),
            b"old nested"
        );
        assert!(
            !dst_dir.join("keep.txt").exists(),
            "restoring a folder must not restore unrelated root files"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
        std::fs::remove_dir_all(&dst_old).ok();
        std::fs::remove_dir_all(&dst_cur).ok();
        std::fs::remove_dir_all(&dst_dir).ok();
    }

    #[test]
    fn explorer_rejects_unsafe_paths() {
        let src = std::env::temp_dir().join(format!("explore-safe-src-{}", uuid::Uuid::new_v4()));
        let store_dir =
            std::env::temp_dir().join(format!("explore-safe-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("explore-safe-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"safe").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "b".into();
        cfg.region = "r".into();
        cfg.access_key_id = "a".into();
        cfg.secret_access_key = "s".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        assert!(list_entries(&cfg, &store, None, "..").is_err());
        assert!(
            restore_entry(
                &cfg,
                &store,
                None,
                "../escape",
                &dst,
                CancelToken::new(),
                &mut |_| {},
            )
            .is_err()
        );
        assert!(!dst.join("escape").exists());

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }
}

#[cfg(test)]
mod destination_truth_tests {
    use super::*;
    use crate::core::engine::{CancelToken, run_backup};
    use crate::core::model::AppConfig;
    use crate::core::partial::PartialTree;
    use crate::core::store::{BlobStore, LocalStore};

    fn temp(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("{label}-{}", uuid::Uuid::new_v4()))
    }

    fn config(src: &Path) -> AppConfig {
        let mut config = AppConfig::default();
        config.bucket = "b".into();
        config.region = "r".into();
        config.access_key_id = "a".into();
        config.secret_access_key = "s".into();
        config.source_kind = "path".into();
        config.source_path = src.to_string_lossy().to_string();
        config.schedule_minutes = 0;
        config
    }

    #[test]
    fn listing_fails_when_the_record_tree_object_is_gone() {
        let src = temp("truth-src");
        let store_dir = temp("truth-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = config(&src);
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let key = record::latest_record_key(&store, &cfg.key_root(), &cfg.backup_set_uuid)
            .unwrap()
            .unwrap();
        let rec = record::load_record(&store, &key, None).unwrap();
        let loc = rec
            .root_node()
            .unwrap()
            .tree_blob_loc
            .expect("root tree location");
        let object_key = record::root_key(&cfg.key_root(), &loc.relative_path);
        store.delete(&object_key).unwrap();

        let err = list_entries(&cfg, &store, None, "").unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("missing from the destination") || message.contains("No such file"),
            "unexpected error: {err}"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
    }

    #[test]
    fn partial_listing_is_rejected_after_the_destination_is_cleared() {
        let src = temp("truth-partial-src");
        let store_dir = temp("truth-partial-store");
        std::fs::create_dir_all(&src).unwrap();
        let store = LocalStore::new(&store_dir).unwrap();
        let cfg = config(&src);
        let partial = PartialTree::new();
        partial.begin();
        partial.publish_dir(
            "",
            crate::core::model::Node {
                is_tree: true,
                ..crate::core::model::Node::default()
            },
            &[],
            false,
        );
        partial.publish_child(
            "",
            "a.txt",
            crate::core::model::Node {
                item_size: 1,
                ..crate::core::model::Node::default()
            },
        );
        store
            .put(&record::partial_run_key(&cfg.key_root()), b"marker")
            .unwrap();
        assert!(list_partial_entries(&cfg, &store, &partial, "").is_ok());

        store
            .delete(&record::partial_run_key(&cfg.key_root()))
            .unwrap();
        let err = list_partial_entries(&cfg, &store, &partial, "").unwrap_err();
        assert!(
            err.to_string()
                .contains("no longer contains this partial backup"),
            "unexpected error: {err}"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
    }
}
