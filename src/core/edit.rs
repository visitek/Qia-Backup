//! Editing a backup set from the file explorer.
//!
//! Removing an entry rewrites only the selected record's ancestor tree chain;
//! ignoring a path only changes future backup exclusions. The two operations
//! are deliberately independent: removing from an old revision must not
//! silently change what a future backup scans.

use globset::escape;

use crate::core::error::{BackupError, Result};
use crate::core::model::AppConfig;
use crate::core::restore;

/// Convert a relative backup path into a literal exclude pattern.
///
/// `globset` treats `*`, `?`, `[`, `]`, `{` and `}` specially. Escaping them is
/// essential: a file named `report[final].txt` must be ignored by its literal
/// name, not by an accidental character class.
pub fn ignore_pattern_for_path(path: &str) -> Result<String> {
    let normalized = restore::normalize_backup_path(path)?;
    if normalized.is_empty() {
        return Err(BackupError::Other(
            "cannot ignore the backup root; choose a file or folder".into(),
        ));
    }
    if normalized.contains(['\n', '\r']) {
        return Err(BackupError::Other(
            "cannot ignore a path containing a newline".into(),
        ));
    }
    Ok(escape(&normalized))
}

/// Add a literal ignore pattern for `path` to `config.exclude_patterns`.
///
/// Returns the patterns that were added (empty when the path was already
/// ignored). Existing backups are not touched; the caller saves the config.
pub fn add_ignore_path(config: &mut AppConfig, path: &str) -> Result<Vec<String>> {
    let pattern = ignore_pattern_for_path(path)?;
    if config
        .exclude_patterns
        .iter()
        .any(|existing| existing == &pattern)
    {
        return Ok(Vec::new());
    }
    config.exclude_patterns.push(pattern.clone());
    Ok(vec![pattern])
}

#[cfg(test)]
mod tests {
    use super::*;
    use globset::Glob;

    #[test]
    fn ignore_pattern_is_literal_and_normalized() {
        let pattern = ignore_pattern_for_path("/Users/me/report[final]*.txt/").unwrap();
        assert_eq!(pattern, "Users/me/report[[]final[]][*].txt");
        let matcher = Glob::new(&pattern).unwrap().compile_matcher();
        assert!(matcher.is_match("Users/me/report[final]*.txt"));
        assert!(!matcher.is_match("Users/me/reportf.txt"));
    }

    #[test]
    fn add_ignore_path_is_idempotent() {
        let mut config = AppConfig::default();
        config.exclude_patterns.clear();

        let added = add_ignore_path(&mut config, "Users/me/cache").unwrap();
        assert_eq!(added, vec!["Users/me/cache"]);
        assert_eq!(config.exclude_patterns, vec!["Users/me/cache"]);

        let added_again = add_ignore_path(&mut config, "/Users/me/cache/").unwrap();
        assert!(added_again.is_empty());
        assert_eq!(config.exclude_patterns, vec!["Users/me/cache"]);
    }

    #[test]
    fn cannot_ignore_backup_root() {
        let mut config = AppConfig::default();
        assert!(add_ignore_path(&mut config, "/").is_err());
    }
}

use base64::Engine as _;
use serde::Serialize;

use crate::core::codec;
use crate::core::compression::compress_blob;
use crate::core::crypto::{ObjectEncryptor, sha256_hex};
use crate::core::engine::CancelToken;
use crate::core::index::BlobIndex;
use crate::core::logging;
use crate::core::model::{BlobLoc, Node, Tree};
use crate::core::record;
use crate::core::store::BlobStore;

/// Result of removing one entry from a completed record.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveSummary {
    pub record_key: String,
    pub path: String,
    pub removed_files: u64,
    /// False when the record was updated but the new tree blobs could not be
    /// persisted in the index. The record remains valid and recoverable.
    pub index_saved: bool,
}

/// Result of removing every ignored entry from a completed record.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveIgnoredSummary {
    pub record_key: String,
    /// Number of files removed, including files inside removed directories.
    pub removed_files: u64,
    /// Number of top-level ignored entries removed from their parent directory.
    pub removed_entries: u64,
    /// False when the record was updated but the new tree blobs could not be
    /// persisted in the index. The record remains valid and recoverable.
    pub index_saved: bool,
}

struct TreeWriter<'a> {
    store: &'a dyn BlobStore,
    root: String,
    index: BlobIndex,
    encryptor: Option<ObjectEncryptor>,
}

impl TreeWriter<'_> {
    /// Persist one replacement tree as a standalone content-addressed object.
    /// The number of replacement trees is the depth of the removed path, so
    /// packing them would add complexity without a meaningful object saving.
    fn store_tree(&mut self, tree: &Tree) -> Result<BlobLoc> {
        let plaintext = codec::encode_tree(tree);
        let id = sha256_hex(&plaintext);
        if let Some(loc) = self.index.blob(&id) {
            return Ok(loc.clone());
        }

        let compressed = compress_blob(&plaintext);
        let encryption_enabled = self.encryptor.is_some();
        let stored = match self.encryptor.as_mut() {
            Some(enc) => enc.encrypt(&compressed.bytes)?,
            None => compressed.bytes,
        };
        let relative = format!("treepacks/{id}");
        let key = record::root_key(&self.root, &relative);
        self.store.put(&key, &stored)?;
        record::cache_tree_blob(&id, &stored);
        let loc = BlobLoc {
            blob_identifier: id.clone(),
            is_packed: false,
            relative_path: relative,
            offset: 0,
            length: stored.len() as u64,
            stretch_encryption_key: encryption_enabled,
            compression_type: compressed.compression_type,
        };
        self.index.add_blob(loc.clone());
        Ok(loc)
    }
}

/// Remove one file or folder from a completed backup record.
///
/// Only the ancestor tree chain is rewritten: the selected child is dropped
/// from its parent tree, then each parent up to the record root is stored again
/// with adjusted counts. Other entries and other records are untouched.
pub fn remove_entry(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
    path: &str,
    cancel: &CancelToken,
) -> Result<RemoveSummary> {
    cancel.check()?;
    let root = config.key_root();
    let normalized = restore::normalize_backup_path(path)?;
    if normalized.is_empty() {
        return Err(BackupError::Other(
            "cannot remove the backup root; choose a file or folder".into(),
        ));
    }
    if record_key == Some("partial") {
        return Err(BackupError::Other(
            "partial backups have no completed record to remove from".into(),
        ));
    }

    let record_key = restore::resolve_record_key(config, store, record_key)?;
    let keys = restore::load_keys(config, store, &root)?;
    let mut record = record::load_record(store, &record_key, keys.as_ref())?;
    let root_node = record.root_node()?;
    if !root_node.is_tree {
        return Err(BackupError::Codec(
            "backup record root is not a directory".into(),
        ));
    }

    // A partially loaded index must never be saved over a complete one. If the
    // index is empty while records exist, rebuild it first, exactly like a
    // normal backup does. This keeps later garbage collection safe.
    let mut index = BlobIndex::load(store, &root, config)?;
    if index.blobs.is_empty() {
        index =
            crate::core::recover::rebuild_index(config, store, keys.as_ref(), cancel, |detail| {
                logging::debug(detail)
            })?;
    }

    let mut writer = TreeWriter {
        store,
        root: root.clone(),
        index,
        encryptor: keys.clone().map(ObjectEncryptor::new),
    };
    let components = restore::path_components(&normalized)?;
    let (new_root, removed_files) = remove_child(
        store,
        &root,
        keys.as_ref(),
        &mut writer,
        &root_node,
        &components,
        &normalized,
    )?;

    record.node = base64::engine::general_purpose::STANDARD
        .encode(codec::encode_node(&new_root, record.node_tree_version));
    record.files_count = record.files_count.saturating_sub(removed_files);

    let bytes = record::encode_record(&record, writer.encryptor.as_mut())?;
    store.put(&record_key, &bytes)?;

    let index_saved = match writer.index.save_and_clear(store, &root, config) {
        Ok(()) => true,
        Err(e) => {
            logging::warn(format!(
                "removed {normalized:?} from {record_key}, but saving the blob index failed: {e}"
            ));
            false
        }
    };
    logging::info(format!(
        "removed {normalized:?} from record {record_key} ({removed_files} files)"
    ));
    Ok(RemoveSummary {
        record_key,
        path: normalized,
        removed_files,
        index_saved,
    })
}

fn remove_child(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&crate::core::crypto::MasterKeys>,
    writer: &mut TreeWriter<'_>,
    node: &Node,
    components: &[String],
    full_path: &str,
) -> Result<(Node, u64)> {
    if components.is_empty() {
        return Err(BackupError::Other("empty removal path".into()));
    }
    if !node.is_tree {
        return Err(BackupError::Other(format!(
            "backup path is not a directory: {full_path}"
        )));
    }
    let loc = node
        .tree_blob_loc
        .as_ref()
        .ok_or_else(|| BackupError::Codec("directory node has no tree location".into()))?;
    let mut tree = record::load_tree(store, root, loc, keys)?;
    let name = &components[0];
    let position = tree
        .children
        .iter()
        .position(|(child_name, _)| child_name == name)
        .ok_or_else(|| BackupError::Other(format!("backup path not found: {full_path}")))?;

    let removed_files = if components.len() == 1 {
        let (_, removed) = tree.children.remove(position);
        count_node_files(&removed)
    } else {
        let child = tree.children[position].1.clone();
        let (new_child, removed_files) = remove_child(
            store,
            root,
            keys,
            writer,
            &child,
            &components[1..],
            full_path,
        )?;
        tree.children[position].1 = new_child;
        removed_files
    };

    let mut updated = node.clone();
    updated.contained_files_count = updated.contained_files_count.saturating_sub(removed_files);
    updated.tree_blob_loc = Some(writer.store_tree(&tree)?);
    Ok((updated, removed_files))
}

fn count_node_files(node: &Node) -> u64 {
    if node.is_tree || Node::mode_is_dir(node.mac_st_mode) {
        node.contained_files_count
    } else if Node::mode_is_symlink(node.mac_st_mode) {
        0
    } else if node.mac_st_mode & libc::S_IFMT as u32 == libc::S_IFREG as u32 {
        1
    } else {
        0
    }
}

/// Remove every path that the current ignore rules match from a completed
/// record.
///
/// Directories are filtered recursively; when a directory matches, its whole
/// subtree is removed without being visited. Ancestor trees are rewritten only
/// when something below them changed. Other revisions are untouched.
pub fn remove_ignored_entries(
    config: &AppConfig,
    store: &dyn BlobStore,
    record_key: Option<&str>,
    cancel: &CancelToken,
) -> Result<RemoveIgnoredSummary> {
    cancel.check()?;
    let root = config.key_root();
    if record_key == Some("partial") {
        return Err(BackupError::Other(
            "partial backups have no completed record to remove from".into(),
        ));
    }

    let record_key = restore::resolve_record_key(config, store, record_key)?;
    let keys = restore::load_keys(config, store, &root)?;
    let mut record = record::load_record(store, &record_key, keys.as_ref())?;
    let root_node = record.root_node()?;
    if !root_node.is_tree {
        return Err(BackupError::Codec(
            "backup record root is not a directory".into(),
        ));
    }

    let matcher = crate::core::engine::build_excludes(&config.exclude_patterns)?;

    // A partially loaded index must never be saved over a complete one. If the
    // index is empty while records exist, rebuild it first, exactly like a
    // normal backup does. This keeps later garbage collection safe.
    let mut index = BlobIndex::load(store, &root, config)?;
    if index.blobs.is_empty() {
        index =
            crate::core::recover::rebuild_index(config, store, keys.as_ref(), cancel, |detail| {
                logging::debug(detail)
            })?;
    }

    let mut writer = TreeWriter {
        store,
        root: root.clone(),
        index,
        encryptor: keys.clone().map(ObjectEncryptor::new),
    };
    let (new_root, removed_files, removed_entries) = remove_ignored_from_node(
        store,
        &root,
        keys.as_ref(),
        &mut writer,
        &root_node,
        "",
        &matcher,
        cancel,
    )?;

    if removed_entries == 0 {
        return Ok(RemoveIgnoredSummary {
            record_key,
            removed_files: 0,
            removed_entries: 0,
            index_saved: true,
        });
    }

    record.node = base64::engine::general_purpose::STANDARD
        .encode(codec::encode_node(&new_root, record.node_tree_version));
    record.files_count = record.files_count.saturating_sub(removed_files);

    let bytes = record::encode_record(&record, writer.encryptor.as_mut())?;
    store.put(&record_key, &bytes)?;

    let index_saved = match writer.index.save_and_clear(store, &root, config) {
        Ok(()) => true,
        Err(e) => {
            logging::warn(format!(
                "removed ignored entries from {record_key}, but saving the blob index failed: {e}"
            ));
            false
        }
    };
    logging::info(format!(
        "removed {removed_entries} ignored entr{} ({removed_files} files) from record {record_key}",
        if removed_entries == 1 { "y" } else { "ies" }
    ));
    Ok(RemoveIgnoredSummary {
        record_key,
        removed_files,
        removed_entries,
        index_saved,
    })
}

#[allow(clippy::too_many_arguments)]
fn remove_ignored_from_node(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&crate::core::crypto::MasterKeys>,
    writer: &mut TreeWriter<'_>,
    node: &Node,
    parent_path: &str,
    matcher: &globset::GlobSet,
    cancel: &CancelToken,
) -> Result<(Node, u64, u64)> {
    cancel.check()?;
    if !node.is_tree {
        return Ok((node.clone(), 0, 0));
    }
    let loc = node
        .tree_blob_loc
        .as_ref()
        .ok_or_else(|| BackupError::Codec("directory node has no tree location".into()))?;
    let tree = record::load_tree(store, root, loc, keys)?;
    let mut kept = Vec::with_capacity(tree.children.len());
    let mut removed_files = 0;
    let mut removed_entries = 0;
    for (name, child) in tree.children {
        let child_path = if parent_path.is_empty() {
            name.clone()
        } else {
            format!("{parent_path}/{name}")
        };
        if matcher.is_match(&child_path) || matcher.is_match(&name) {
            removed_files += count_node_files(&child);
            removed_entries += 1;
            continue;
        }
        if child.is_tree || Node::mode_is_dir(child.mac_st_mode) {
            let (new_child, child_files, child_entries) = remove_ignored_from_node(
                store,
                root,
                keys,
                writer,
                &child,
                &child_path,
                matcher,
                cancel,
            )?;
            if child_entries > 0 {
                removed_files += child_files;
                removed_entries += child_entries;
                kept.push((name, new_child));
            } else {
                kept.push((name, child));
            }
        } else {
            kept.push((name, child));
        }
    }

    if removed_entries == 0 {
        return Ok((node.clone(), 0, 0));
    }
    let mut updated = node.clone();
    updated.contained_files_count = updated.contained_files_count.saturating_sub(removed_files);
    updated.tree_blob_loc = Some(writer.store_tree(&Tree {
        version: tree.version,
        children: kept,
    })?);
    Ok((updated, removed_files, removed_entries))
}

#[cfg(test)]
mod remove_tests {
    use super::*;
    use crate::core::engine::run_backup;
    use crate::core::model::AppConfig;
    use crate::core::restore;
    use crate::core::store::LocalStore;
    use std::path::Path;

    fn temp_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("{label}-{}", uuid::Uuid::new_v4()))
    }

    fn test_config(src: &Path) -> AppConfig {
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

    fn backup(config: &mut AppConfig, store: &LocalStore) {
        run_backup(config, store, CancelToken::new(), &mut |_| {}).unwrap();
    }

    fn list(config: &AppConfig, store: &LocalStore, key: &str, path: &str) -> Vec<String> {
        restore::list_entries(config, store, Some(key), path)
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .collect()
    }

    fn latest(config: &AppConfig, store: &LocalStore) -> String {
        record::latest_record_key(store, &config.key_root(), &config.backup_set_uuid)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn removes_a_file_and_preserves_siblings() {
        let src = temp_path("edit-src");
        let dst = temp_path("edit-store");
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("keep.txt"), b"keep").unwrap();
        std::fs::write(src.join("remove.txt"), b"remove me").unwrap();
        std::fs::write(src.join("nested/child.txt"), b"child").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let key = latest(&config, &store);
        assert!(list(&config, &store, &key, "").contains(&"remove.txt".to_string()));

        let summary = remove_entry(
            &config,
            &store,
            Some(&key),
            "remove.txt",
            &CancelToken::new(),
        )
        .unwrap();
        assert_eq!(summary.removed_files, 1);
        assert!(summary.index_saved);

        let names = list(&config, &store, &key, "");
        assert!(!names.contains(&"remove.txt".to_string()));
        assert!(names.contains(&"keep.txt".to_string()));
        assert!(names.contains(&"nested".to_string()));

        let restore_dir = temp_path("edit-restore");
        restore::restore_entry(
            &config,
            &store,
            Some(&key),
            "",
            &restore_dir,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert!(restore_dir.join("keep.txt").exists());
        assert!(!restore_dir.join("remove.txt").exists());
        assert!(restore_dir.join("nested/child.txt").exists());

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
        std::fs::remove_dir_all(&restore_dir).ok();
    }

    #[test]
    fn removes_a_folder_and_preserves_other_entries() {
        let src = temp_path("edit-dir-src");
        let dst = temp_path("edit-dir-store");
        std::fs::create_dir_all(src.join("drop/inside")).unwrap();
        std::fs::create_dir_all(src.join("keep")).unwrap();
        std::fs::write(src.join("drop/inside/a.txt"), b"a").unwrap();
        std::fs::write(src.join("drop/b.txt"), b"b").unwrap();
        std::fs::write(src.join("keep/c.txt"), b"c").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let key = latest(&config, &store);

        let summary =
            remove_entry(&config, &store, Some(&key), "drop", &CancelToken::new()).unwrap();
        assert_eq!(summary.removed_files, 2);

        let names = list(&config, &store, &key, "");
        assert!(!names.contains(&"drop".to_string()));
        assert!(names.contains(&"keep".to_string()));
        assert!(list(&config, &store, &key, "keep").contains(&"c.txt".to_string()));

        let restore_dir = temp_path("edit-dir-restore");
        restore::restore_entry(
            &config,
            &store,
            Some(&key),
            "",
            &restore_dir,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert!(!restore_dir.join("drop").exists());
        assert!(restore_dir.join("keep/c.txt").exists());

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
        std::fs::remove_dir_all(&restore_dir).ok();
    }

    #[test]
    fn removal_targets_only_the_selected_revision() {
        let src = temp_path("edit-version-src");
        let dst = temp_path("edit-version-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();
        std::fs::write(src.join("b.txt"), b"b").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let first = latest(&config, &store);
        backup(&mut config, &store);
        let second = latest(&config, &store);
        assert_ne!(first, second);

        remove_entry(&config, &store, Some(&first), "a.txt", &CancelToken::new()).unwrap();
        assert!(!list(&config, &store, &first, "").contains(&"a.txt".to_string()));
        assert!(list(&config, &store, &second, "").contains(&"a.txt".to_string()));

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn ignored_paths_are_excluded_from_the_next_backup() {
        let src = temp_path("edit-ignore-src");
        let dst = temp_path("edit-ignore-store");
        std::fs::create_dir_all(src.join("drop")).unwrap();
        std::fs::write(src.join("drop/a.txt"), b"a").unwrap();
        std::fs::write(src.join("remove.txt"), b"remove").unwrap();
        std::fs::write(src.join("keep.txt"), b"keep").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        add_ignore_path(&mut config, "drop").unwrap();
        add_ignore_path(&mut config, "remove.txt").unwrap();
        backup(&mut config, &store);
        let key = latest(&config, &store);

        let names = list(&config, &store, &key, "");
        assert!(!names.contains(&"drop".to_string()));
        assert!(!names.contains(&"remove.txt".to_string()));
        assert!(names.contains(&"keep.txt".to_string()));

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn removes_an_entry_from_an_encrypted_record() {
        let src = temp_path("edit-enc-src");
        let dst = temp_path("edit-enc-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("secret.txt"), b"encrypted secret").unwrap();
        std::fs::write(src.join("keep.txt"), b"keep").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        config.encryption_enabled = true;
        config.encryption_password = "test password".into();
        backup(&mut config, &store);
        let key = latest(&config, &store);

        remove_entry(
            &config,
            &store,
            Some(&key),
            "secret.txt",
            &CancelToken::new(),
        )
        .unwrap();
        assert!(!list(&config, &store, &key, "").contains(&"secret.txt".to_string()));

        let restore_dir = temp_path("edit-enc-restore");
        restore::restore_entry(
            &config,
            &store,
            Some(&key),
            "",
            &restore_dir,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert!(!restore_dir.join("secret.txt").exists());
        assert_eq!(
            std::fs::read(restore_dir.join("keep.txt")).unwrap(),
            b"keep"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
        std::fs::remove_dir_all(&restore_dir).ok();
    }

    #[test]
    fn rejects_root_and_partial_removals() {
        let src = temp_path("edit-reject-src");
        let dst = temp_path("edit-reject-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();
        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let key = latest(&config, &store);

        assert!(remove_entry(&config, &store, Some(&key), "/", &CancelToken::new()).is_err());
        assert!(
            remove_entry(
                &config,
                &store,
                Some("partial"),
                "a.txt",
                &CancelToken::new()
            )
            .is_err()
        );
        assert!(
            remove_entry(
                &config,
                &store,
                Some(&key),
                "missing.txt",
                &CancelToken::new()
            )
            .is_err()
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn garbage_collection_reclaims_objects_after_the_last_reference_is_removed() {
        let src = temp_path("edit-gc-only-src");
        let dst = temp_path("edit-gc-only-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("only.txt"), b"only content").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let key = latest(&config, &store);
        let pack_prefix = format!("{}/blobpacks/", config.key_root());
        let packs_before = store.list(&pack_prefix).unwrap();
        assert!(!packs_before.is_empty());

        remove_entry(&config, &store, Some(&key), "only.txt", &CancelToken::new()).unwrap();
        let summary =
            crate::core::retention::prune_unreferenced(&config, &store, &CancelToken::new())
                .unwrap();
        assert!(summary.objects_deleted > 0);
        assert!(
            store.list(&pack_prefix).unwrap().is_empty(),
            "the old data pack should be reclaimed once no record references it"
        );
        assert!(list(&config, &store, &key, "").is_empty());

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn garbage_collection_keeps_blobs_referenced_by_other_records() {
        let src = temp_path("edit-gc-src");
        let dst = temp_path("edit-gc-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"shared content").unwrap();
        std::fs::write(src.join("b.txt"), b"other").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let first = latest(&config, &store);
        backup(&mut config, &store);
        let second = latest(&config, &store);

        remove_entry(&config, &store, Some(&first), "a.txt", &CancelToken::new()).unwrap();
        let summary =
            crate::core::retention::prune_unreferenced(&config, &store, &CancelToken::new())
                .unwrap();
        assert_eq!(summary.records_deleted, 0);
        // The second record still references a.txt, so its content must remain
        // restorable after garbage collection.
        let restore_dir = temp_path("edit-gc-restore");
        restore::restore_entry(
            &config,
            &store,
            Some(&second),
            "",
            &restore_dir,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert!(restore_dir.join("a.txt").exists());
        assert!(restore_dir.join("b.txt").exists());

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
        std::fs::remove_dir_all(&restore_dir).ok();
    }

    #[test]
    fn removes_all_ignored_entries_recursively_and_keeps_the_rest() {
        let src = temp_path("edit-ignored-src");
        let dst = temp_path("edit-ignored-store");
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::create_dir_all(src.join("cache/deep")).unwrap();
        std::fs::write(src.join("keep.txt"), b"keep").unwrap();
        std::fs::write(src.join("drop.log"), b"drop").unwrap();
        std::fs::write(src.join("nested/child.txt"), b"child").unwrap();
        std::fs::write(src.join("cache/one.bin"), b"one").unwrap();
        std::fs::write(src.join("cache/deep/two.bin"), b"two").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let key = latest(&config, &store);

        add_ignore_path(&mut config, "drop.log").unwrap();
        add_ignore_path(&mut config, "cache").unwrap();

        let listing = restore::list_entries(&config, &store, Some(&key), "").unwrap();
        assert!(!listing.ignored);
        assert!(
            listing
                .entries
                .iter()
                .find(|entry| entry.name == "drop.log")
                .unwrap()
                .ignored
        );
        assert!(
            listing
                .entries
                .iter()
                .find(|entry| entry.name == "cache")
                .unwrap()
                .ignored
        );
        let cache = restore::list_entries(&config, &store, Some(&key), "cache").unwrap();
        assert!(cache.ignored, "the browsed ignored directory is flagged");
        assert!(cache.entries.iter().all(|entry| entry.ignored));

        let summary =
            remove_ignored_entries(&config, &store, Some(&key), &CancelToken::new()).unwrap();
        assert_eq!(summary.record_key, key);
        assert_eq!(summary.removed_entries, 2);
        assert_eq!(summary.removed_files, 3);
        assert!(summary.index_saved);

        let names = list(&config, &store, &key, "");
        assert_eq!(names, vec!["nested", "keep.txt"]);
        assert!(
            restore::list_entries(&config, &store, Some(&key), "cache").is_err(),
            "the ignored directory was removed from this revision"
        );
        assert_eq!(list(&config, &store, &key, "nested"), vec!["child.txt"]);

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn removing_ignored_entries_is_a_noop_when_nothing_matches() {
        let src = temp_path("edit-ignored-noop-src");
        let dst = temp_path("edit-ignored-noop-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("keep.txt"), b"keep").unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut config = test_config(&src);
        backup(&mut config, &store);
        let key = latest(&config, &store);

        let summary =
            remove_ignored_entries(&config, &store, Some(&key), &CancelToken::new()).unwrap();
        assert_eq!(summary.removed_entries, 0);
        assert_eq!(summary.removed_files, 0);
        assert_eq!(list(&config, &store, &key, ""), vec!["keep.txt"]);

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }
}
