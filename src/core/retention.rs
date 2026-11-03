//! Retention and garbage collection.
//!
//! Policy: keep the newest record, all records inside `retain_hours`, one per
//! hour for the daily window, one per week for the weekly window, and one per
//! month for the monthly window. Anything older is deleted. Garbage collection
//! is conservative: records are parsed and all referenced blobs are marked
//! before any object is removed. If any kept record cannot be read, GC aborts.

use std::collections::HashSet;

use chrono::{Datelike, Utc};
use serde::Serialize;

use crate::core::engine::CancelToken;
use crate::core::error::{BackupError, Result};
use crate::core::index::BlobIndex;
use crate::core::logging;
use crate::core::model::{AppConfig, BackupRecord, Node};
use crate::core::record;
use crate::core::restore::load_keys;
use crate::core::store::BlobStore;

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PruneSummary {
    pub records_before: usize,
    pub records_kept: usize,
    pub records_deleted: usize,
    pub objects_deleted: usize,
    pub objects_scanned: usize,
    pub blobs_referenced: usize,
    pub dry_run: bool,
}

/// Only these prefixes contain content-addressed data objects. Metadata,
/// records, the index, journals and the partial-run marker are never scanned.
const ORPHAN_GC_PREFIXES: &[&str] = &["blobpacks/", "standardobjects/", "treepacks/"];
const ORPHAN_LIST_PAGE: u32 = 1000;
const ORPHAN_DELETE_WORKERS: usize = 8;

pub fn prune(
    config: &AppConfig,
    store: &dyn BlobStore,
    cancel: &CancelToken,
    dry_run: bool,
) -> Result<PruneSummary> {
    let root = config.key_root();
    let keys = record::list_record_keys(store, &root, &config.backup_set_uuid)?;
    if keys.is_empty() {
        return Ok(PruneSummary {
            dry_run,
            ..PruneSummary::default()
        });
    }
    let now = Utc::now().timestamp();
    let keys = select_kept(&keys, config, now);
    let mut summary = PruneSummary {
        records_before: record::list_record_keys(store, &root, &config.backup_set_uuid)?.len(),
        records_kept: keys.kept.len(),
        records_deleted: keys.removed.len(),
        dry_run,
        ..PruneSummary::default()
    };

    // Nothing aged out, so no blob can have become unreferenced since the last
    // full GC. Skipping the index load keeps hourly prunes cheap even for
    // multi-million-blob backup sets.
    if keys.removed.is_empty() {
        logging::info(format!(
            "prune: kept {} records, nothing to delete",
            keys.kept.len()
        ));
        return Ok(summary);
    }

    let master_keys = load_keys(config, store, &root)?;
    let mut referenced: HashSet<String> = HashSet::new();
    // Trees are content-addressed, so identical subtrees recur across records.
    // Walking each unique tree once makes multi-record mark-and-sweep cheap
    // without storing a per-record blob manifest.
    let mut visited_trees: HashSet<String> = HashSet::new();
    for key in &keys.kept {
        cancel.check()?;
        let rec = record::load_record(store, key, master_keys.as_ref()).map_err(|e| {
            BackupError::Store(format!(
                "cannot safely prune: failed to read kept record {key}: {e}"
            ))
        })?;
        let node = rec.root_node()?;
        collect_node_refs(
            store,
            &root,
            master_keys.as_ref(),
            &node,
            Some(&mut referenced),
            None,
            &mut visited_trees,
            cancel,
        )?;
    }
    summary.blobs_referenced = referenced.len();

    if dry_run {
        // Simulate GC so the summary reports what would be deleted.
        let mut index = BlobIndex::load(store, &root, config)?;
        let removed = index.prune_to(&referenced);
        summary.objects_deleted = removed.len();
        return Ok(summary);
    }

    for key in &keys.removed {
        cancel.check()?;
        logging::info(format!("deleting old backup record {key}"));
        store.delete(key)?;
    }

    let mut index = BlobIndex::load(store, &root, config)?;
    let removed = index.prune_to(&referenced);
    if keys.removed.is_empty() && removed.is_empty() {
        // The common incremental case removes nothing. Rewriting a
        // multi-hundred-MB base index (and re-uploading it) on every no-op
        // prune would dominate the cost of an hourly backup.
        summary.objects_deleted = 0;
        logging::info(format!(
            "prune: kept {} records, nothing to delete",
            summary.records_kept
        ));
        return Ok(summary);
    }
    // Persist the pruned index before deleting objects. If a delete fails,
    // the next run sees a consistent index and the leftovers are merely
    // unreferenced storage rather than a dedupe source pointing at missing
    // objects.
    index.updated_at = Utc::now();
    index.save_and_clear(store, &root, config)?;
    let mut deleted_count = 0;
    for relative in &removed {
        cancel.check()?;
        let key = record::root_key(&root, relative);
        logging::info(format!("deleting unreferenced object {key}"));
        store.delete(&key)?;
        deleted_count += 1;
    }
    summary.objects_deleted = deleted_count;
    logging::info(format!(
        "prune: kept {} records, deleted {} records and {} objects",
        summary.records_kept, summary.records_deleted, summary.objects_deleted
    ));
    Ok(summary)
}

/// Delete objects that are no longer referenced by any backup record without
/// deleting any records. This is used after an explicit edit of one record so
/// data removed from that revision can be reclaimed when no other revision
/// still references it.
pub fn prune_unreferenced(
    config: &AppConfig,
    store: &dyn BlobStore,
    cancel: &CancelToken,
) -> Result<PruneSummary> {
    let root = config.key_root();
    let keys = record::list_record_keys(store, &root, &config.backup_set_uuid)?;
    if keys.is_empty() {
        return Ok(PruneSummary::default());
    }
    let master_keys = load_keys(config, store, &root)?;
    let mut referenced: HashSet<String> = HashSet::new();
    let mut visited_trees: HashSet<String> = HashSet::new();
    for key in &keys {
        cancel.check()?;
        let rec = record::load_record(store, key, master_keys.as_ref()).map_err(|e| {
            BackupError::Store(format!(
                "cannot safely collect garbage: failed to read kept record {key}: {e}"
            ))
        })?;
        let node = rec.root_node()?;
        collect_node_refs(
            store,
            &root,
            master_keys.as_ref(),
            &node,
            Some(&mut referenced),
            None,
            &mut visited_trees,
            cancel,
        )?;
    }

    let mut index = BlobIndex::load(store, &root, config)?;
    let removed = index.prune_to(&referenced);
    let mut summary = PruneSummary {
        records_before: keys.len(),
        records_kept: keys.len(),
        records_deleted: 0,
        objects_deleted: 0,
        blobs_referenced: referenced.len(),
        ..PruneSummary::default()
    };
    if removed.is_empty() {
        return Ok(summary);
    }

    index.updated_at = Utc::now();
    index.save_and_clear(store, &root, config)?;
    for relative in &removed {
        cancel.check()?;
        let key = record::root_key(&root, relative);
        logging::info(format!("deleting unreferenced object {key}"));
        store.delete(&key)?;
        summary.objects_deleted += 1;
    }
    logging::info(format!(
        "garbage collection: kept {} records and deleted {} unreferenced objects",
        summary.records_kept, summary.objects_deleted
    ));
    Ok(summary)
}

/// Remove objects under the data prefixes that are not referenced by the base
/// index, its journals, or any current backup record.
///
/// This closes the gap left by normal retention: an upload that completed just
/// before a crash was never journaled, so it is absent from the index and
/// invisible to [`BlobIndex::prune_to`]. Such objects are still present in the
/// bucket and can accumulate after repeated interruptions.
///
/// Callers must ensure no backup or upload is running. The scan is
/// memory-bounded: it pages through the store and keeps only distinct object
/// paths (roughly one per pack), never the full blob map or object listing.
pub fn prune_orphans(
    config: &AppConfig,
    store: &dyn BlobStore,
    cancel: &CancelToken,
    dry_run: bool,
) -> Result<PruneSummary> {
    let root = config.key_root();
    let index = BlobIndex::load(store, &root, config)?;
    prune_orphans_with_index(config, store, &index, cancel, dry_run)
}

/// Variant used after a backup, where the just-saved index is already in
/// memory. This avoids re-reading and re-parsing a potentially multi-million
/// entry index from storage.
pub fn prune_orphans_with_index(
    config: &AppConfig,
    store: &dyn BlobStore,
    index: &BlobIndex,
    cancel: &CancelToken,
    dry_run: bool,
) -> Result<PruneSummary> {
    let root = config.key_root();
    index.check_consistency()?;

    let record_keys = record::list_record_keys(store, &root, &config.backup_set_uuid)?;
    let master_keys = if record_keys.is_empty() {
        None
    } else {
        load_keys(config, store, &root)?
    };

    // Live object paths, not blob IDs. Packed blobs share one path per pack, so
    // this set stays proportional to pack count even for millions of blobs.
    let mut live_paths: HashSet<String> = HashSet::with_capacity(index.packs.len());
    for path in index.packs.keys() {
        let normalized = path.trim_start_matches('/');
        if !live_paths.contains(normalized) {
            live_paths.insert(normalized.to_string());
        }
    }
    // Unpacked blobs (large standalone objects and edited trees) have their own
    // object path. Packed blobs are already covered by index.packs above.
    for loc in index.blobs.values() {
        if loc.is_packed {
            continue;
        }
        let normalized = loc.relative_path.trim_start_matches('/');
        if !live_paths.contains(normalized) {
            live_paths.insert(normalized.to_string());
        }
    }

    // Records are authoritative too: the index keeps one location per content
    // ID, but an older record can reference the same content at another
    // physical path (for example after rebuilding from records). Protect every
    // path a record actually stores.
    let mut visited_trees = HashSet::new();
    for key in &record_keys {
        cancel.check()?;
        let rec = record::load_record(store, key, master_keys.as_ref()).map_err(|e| {
            BackupError::Store(format!(
                "cannot safely collect orphaned objects: failed to read record {key}: {e}"
            ))
        })?;
        let node = rec.root_node()?;
        collect_node_refs(
            store,
            &root,
            master_keys.as_ref(),
            &node,
            None,
            Some(&mut live_paths),
            &mut visited_trees,
            cancel,
        )?;
    }

    let mut summary = PruneSummary {
        records_before: record_keys.len(),
        records_kept: record_keys.len(),
        dry_run,
        ..PruneSummary::default()
    };
    let root_prefix = format!("{}/", root.trim_end_matches('/'));

    for prefix in ORPHAN_GC_PREFIXES {
        let full_prefix = record::root_key(&root, prefix);
        let mut token: Option<String> = None;
        loop {
            cancel.check()?;
            let page = store.list_page(&full_prefix, token.as_deref(), ORPHAN_LIST_PAGE)?;
            let mut orphans = Vec::new();
            for object in &page.objects {
                summary.objects_scanned = summary.objects_scanned.saturating_add(1);
                let Some(relative) = object.key.strip_prefix(&root_prefix) else {
                    // Never touch an object outside this backup set, even if a
                    // store implementation returned an unexpected key.
                    continue;
                };
                if !live_paths.contains(relative) {
                    orphans.push(object.key.clone());
                }
            }
            if dry_run {
                summary.objects_deleted = summary.objects_deleted.saturating_add(orphans.len());
            } else if !orphans.is_empty() {
                summary.objects_deleted = summary
                    .objects_deleted
                    .saturating_add(delete_orphan_objects(store, &orphans, cancel)?);
            }
            match page.next {
                Some(next) if !next.is_empty() => token = Some(next),
                _ => break,
            }
        }
    }

    logging::info(format!(
        "orphan cleanup: scanned {} objects against {} live object paths; {} {} orphaned objects",
        summary.objects_scanned,
        live_paths.len(),
        if dry_run { "would delete" } else { "deleted" },
        summary.objects_deleted
    ));
    Ok(summary)
}

fn delete_orphan_objects(
    store: &dyn BlobStore,
    keys: &[String],
    cancel: &CancelToken,
) -> Result<usize> {
    let mut deleted = 0usize;
    for batch in keys.chunks(ORPHAN_DELETE_WORKERS) {
        cancel.check()?;
        let results: Vec<Result<()>> = std::thread::scope(|scope| {
            let handles: Vec<_> = batch
                .iter()
                .map(|key| {
                    scope.spawn(move || {
                        cancel.check()?;
                        store.delete(key)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(BackupError::Store(
                            "orphan cleanup delete worker panicked".into(),
                        ))
                    })
                })
                .collect()
        });
        for result in results {
            result?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

struct KeptKeys {
    kept: Vec<String>,
    removed: Vec<String>,
}

fn select_kept(keys: &[String], config: &AppConfig, now: i64) -> KeptKeys {
    let mut sorted: Vec<(String, i64)> = keys
        .iter()
        .filter_map(|k| record::parse_record_epoch(k).map(|e| (k.clone(), e)))
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    let mut kept = Vec::new();
    let mut removed = Vec::new();
    let mut day_buckets = HashSet::new();
    let mut week_buckets = HashSet::new();
    let mut month_buckets = HashSet::new();
    for (index, (key, epoch)) in sorted.iter().enumerate() {
        let age = now.saturating_sub(*epoch);
        if config.retain_all || index == 0 {
            kept.push(key.clone());
            // The newest record survives regardless, but it also occupies its
            // daily/weekly/monthly bucket so older records from the same
            // bucket do not create duplicates.
            if age > config.retain_hours as i64 * 3600 {
                if config.retain_days > 0 && age <= config.retain_days as i64 * 86_400 {
                    day_buckets.insert(epoch.div_euclid(86_400));
                } else if config.retain_weeks > 0 && age <= config.retain_weeks as i64 * 7 * 86_400
                {
                    week_buckets.insert(epoch.div_euclid(7 * 86_400));
                } else if config.retain_months > 0
                    && age <= config.retain_months as i64 * 31 * 86_400
                {
                    let dt = chrono::DateTime::from_timestamp(*epoch, 0);
                    let bucket = match dt {
                        Some(dt) => (dt.year() as i64) * 12 + dt.month() as i64,
                        None => epoch.div_euclid(31 * 86_400),
                    };
                    month_buckets.insert(bucket);
                }
            }
            continue;
        }
        if config.retain_hours > 0 && age <= config.retain_hours as i64 * 3600 {
            kept.push(key.clone());
            continue;
        }
        if config.retain_days > 0 && age <= config.retain_days as i64 * 86_400 {
            if day_buckets.insert(epoch.div_euclid(86_400)) {
                kept.push(key.clone());
            } else {
                removed.push(key.clone());
            }
            continue;
        }
        if config.retain_weeks > 0 && age <= config.retain_weeks as i64 * 7 * 86_400 {
            if week_buckets.insert(epoch.div_euclid(7 * 86_400)) {
                kept.push(key.clone());
            } else {
                removed.push(key.clone());
            }
            continue;
        }
        if config.retain_months > 0 && age <= config.retain_months as i64 * 31 * 86_400 {
            let bucket = {
                let dt = chrono::DateTime::from_timestamp(*epoch, 0);
                match dt {
                    Some(dt) => (dt.year() as i64) * 12 + dt.month() as i64,
                    None => epoch.div_euclid(31 * 86_400),
                }
            };
            if month_buckets.insert(bucket) {
                kept.push(key.clone());
            } else {
                removed.push(key.clone());
            }
            continue;
        }
        removed.push(key.clone());
    }
    KeptKeys { kept, removed }
}

fn collect_node_refs(
    store: &dyn BlobStore,
    root: &str,
    keys: Option<&crate::core::crypto::MasterKeys>,
    node: &Node,
    mut blobs: Option<&mut HashSet<String>>,
    mut object_paths: Option<&mut HashSet<String>>,
    visited_trees: &mut HashSet<String>,
    cancel: &CancelToken,
) -> Result<()> {
    cancel.check()?;
    for loc in &node.data_blob_locs {
        mark_loc(loc, &mut blobs, &mut object_paths);
    }
    if let Some(loc) = &node.acl_blob_loc {
        mark_loc(loc, &mut blobs, &mut object_paths);
    }
    for loc in &node.xattrs_blob_locs {
        mark_loc(loc, &mut blobs, &mut object_paths);
    }
    if node.is_tree {
        if let Some(loc) = &node.tree_blob_loc {
            // Always protect this physical path, even when the same tree
            // content was already visited at a different location.
            mark_loc(loc, &mut blobs, &mut object_paths);
            // The same directory tree appears in every backup where it did not
            // change. Its children are identical, so walk it only once.
            if !visited_trees.insert(loc.blob_identifier.clone()) {
                return Ok(());
            }
            let tree = record::load_tree(store, root, loc, keys)?;
            for (_, child) in &tree.children {
                collect_node_refs(
                    store,
                    root,
                    keys,
                    child,
                    blobs.as_deref_mut(),
                    object_paths.as_deref_mut(),
                    visited_trees,
                    cancel,
                )?;
            }
        }
    }
    Ok(())
}

fn mark_loc(
    loc: &crate::core::model::BlobLoc,
    blobs: &mut Option<&mut HashSet<String>>,
    object_paths: &mut Option<&mut HashSet<String>>,
) {
    if let Some(blobs) = blobs.as_deref_mut() {
        blobs.insert(loc.blob_identifier.clone());
    }
    if let Some(paths) = object_paths.as_deref_mut() {
        // Most locs are packed and share a path already present from
        // index.packs; avoid allocating a String for every repeated pack.
        let normalized = loc.relative_path.trim_start_matches('/');
        if !paths.contains(normalized) {
            paths.insert(normalized.to_string());
        }
    }
}

/// Delete everything in the backup set. Does not delete the bucket.
pub fn delete_backup_set(config: &AppConfig, store: &dyn BlobStore) -> Result<usize> {
    let root = config.key_root();
    // S3Store::delete_prefix pages through the listing, so this works for
    // backup sets with millions of objects instead of dying at the 1M cap of
    // the in-memory list helper.
    let deleted = store.delete_prefix(&format!("{}/", root))?;
    let local_index = BlobIndex::local_path(config);
    if local_index.exists() {
        if let Err(e) = std::fs::remove_file(&local_index) {
            logging::warn(format!(
                "could not remove local index {}: {e}",
                local_index.display()
            ));
        }
    }
    logging::warn(format!("deleted backup set {root} ({deleted} objects)"));
    Ok(deleted)
}

pub fn record_summary(record: &BackupRecord) -> String {
    format!(
        "{} files, {} bytes uploaded, complete={}",
        record.files_count, record.bytes_uploaded, record.is_complete
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::engine::CancelToken;
    use crate::core::model::AppConfig;
    use crate::core::store::LocalStore;

    fn cfg() -> AppConfig {
        let mut c = AppConfig::default();
        c.bucket = "b".into();
        c.region = "r".into();
        c.access_key_id = "a".into();
        c.secret_access_key = "s".into();
        c.source_kind = "path".into();
        c.retain_all = true;
        c
    }

    #[test]
    fn retain_all_keeps_everything() {
        let keys: Vec<String> = (0..10)
            .map(|i| record::record_key("root", "f", 1_700_000_000 + i * 100))
            .collect();
        let kept = select_kept(&keys, &cfg(), 1_700_100_000);
        assert_eq!(kept.kept.len(), 10);
        assert!(kept.removed.is_empty());
    }

    #[test]
    fn daily_window_keeps_one_per_day() {
        let now = 1_700_000_000i64;
        let offsets = [
            10 * 3600,
            11 * 3600,
            3 * 86_400,
            4 * 86_400,
            8 * 86_400,
            40 * 86_400,
        ];
        let keys: Vec<String> = offsets
            .iter()
            .map(|o| record::record_key("root", "f", now - o))
            .collect();
        let mut cfg = cfg();
        cfg.retain_all = false;
        cfg.retain_hours = 5;
        cfg.retain_days = 30;
        cfg.retain_weeks = 8;
        cfg.retain_months = 12;
        let kept = select_kept(&keys, &cfg, now);
        assert_eq!(kept.kept.len(), 5);
        // The two records 10h and 11h old share a day bucket, so only the
        // newest survives in the daily window.
        assert_eq!(kept.removed.len(), 1);
        let removed = select_kept(
            &[
                record::record_key("root", "f", now - 10 * 3600),
                record::record_key("root", "f", now - 10 * 3600 - 1800),
                record::record_key("root", "f", now - 11 * 3600),
            ],
            &cfg,
            now,
        );
        assert_eq!(removed.kept.len(), 1);
        assert_eq!(removed.removed.len(), 2);
    }

    #[test]
    fn prune_removes_unreferenced_objects() {
        let src = std::env::temp_dir().join(format!("prune-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("prune-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("x"), vec![1u8; 10_000]).unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut c = cfg();
        c.source_path = src.to_string_lossy().to_string();
        c.schedule_minutes = 0;
        crate::core::engine::run_backup(&mut c, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let keys = record::list_record_keys(&store, &c.key_root(), &c.backup_set_uuid).unwrap();
        assert_eq!(keys.len(), 1);
        // Same record, with a short retention window, should remain because
        // the newest record is always kept.
        c.retain_all = false;
        c.retain_hours = 0;
        c.retain_days = 0;
        c.retain_weeks = 0;
        c.retain_months = 0;
        let summary = prune(&c, &store, &CancelToken::new(), false).unwrap();
        assert_eq!(summary.records_kept, 1);
        assert_eq!(summary.records_deleted, 0);
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn prune_walks_packed_trees_and_drops_unreferenced_blobs() {
        let src = std::env::temp_dir().join(format!("prune-tree-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("prune-tree-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("prune-tree-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), b"old contents").unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut c = cfg();
        c.source_path = src.to_string_lossy().to_string();
        c.schedule_minutes = 0;

        crate::core::engine::run_backup(&mut c, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let root = c.key_root();
        let old_key = record::list_record_keys(&store, &root, &c.backup_set_uuid)
            .unwrap()
            .pop()
            .expect("first record");
        let old_record = record::load_record(&store, &old_key, None).unwrap();
        let old_tree_id = old_record
            .root_node()
            .unwrap()
            .tree_blob_loc
            .expect("root tree location")
            .blob_identifier;

        // Age the first record so the newest-only retention policy removes it.
        let old_bytes = store.get(&old_key).unwrap();
        let old_epoch = record::parse_record_epoch(&old_key).unwrap();
        let aged_key = record::record_key(&root, &c.backup_set_uuid, old_epoch - 3 * 86_400);
        store.put(&aged_key, &old_bytes).unwrap();
        store.delete(&old_key).unwrap();

        std::fs::write(src.join("file.txt"), b"new contents").unwrap();
        crate::core::engine::run_backup(&mut c, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let new_key = record::list_record_keys(&store, &root, &c.backup_set_uuid)
            .unwrap()
            .pop()
            .expect("second record");
        let new_tree_id = record::load_record(&store, &new_key, None)
            .unwrap()
            .root_node()
            .unwrap()
            .tree_blob_loc
            .expect("root tree location")
            .blob_identifier;
        assert_ne!(old_tree_id, new_tree_id);

        c.retain_all = false;
        c.retain_hours = 0;
        c.retain_days = 0;
        c.retain_weeks = 0;
        c.retain_months = 0;
        let summary = prune(&c, &store, &CancelToken::new(), false).unwrap();
        assert_eq!(summary.records_kept, 1);
        assert_eq!(summary.records_deleted, 1);
        assert!(summary.blobs_referenced > 0);

        let index = BlobIndex::load(&store, &root, &c).unwrap();
        index.check_consistency().unwrap();
        assert!(
            index.blob(&old_tree_id).is_none(),
            "prune must drop the removed record's tree blob"
        );
        assert!(
            index.blob(&new_tree_id).is_some(),
            "prune must keep the surviving record's tree blob"
        );

        crate::core::restore::restore_latest(&c, &store, &dst, CancelToken::new(), &mut |_| {})
            .unwrap();
        assert_eq!(
            std::fs::read(dst.join("file.txt")).unwrap(),
            b"new contents"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn orphan_cleanup_removes_only_untracked_data_objects() {
        let src = std::env::temp_dir().join(format!("orphan-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("orphan-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("keep.txt"), b"keep me").unwrap();

        let store = LocalStore::new(&dir).unwrap();
        let mut c = cfg();
        c.source_path = src.to_string_lossy().to_string();
        c.schedule_minutes = 0;
        crate::core::engine::run_backup(&mut c, &store, CancelToken::new(), &mut |_| {}).unwrap();

        let root = c.key_root();
        let live_pack = store
            .list(&format!("{}/blobpacks/", root))
            .unwrap()
            .first()
            .expect("backup pack")
            .key
            .clone();
        let orphan_pack = format!(
            "{}/blobpacks/orphan-{}.blobpack",
            root,
            uuid::Uuid::new_v4()
        );
        let orphan_std = format!("{}/standardobjects/aa/orphan", root);
        let orphan_tree = format!("{}/treepacks/orphan", root);
        store.put(&orphan_pack, b"orphan").unwrap();
        store.put(&orphan_std, b"orphan").unwrap();
        store.put(&orphan_tree, b"orphan").unwrap();

        // A journal-only object is live dedupe progress even though no record
        // references it yet.
        let indexed_only = format!("{}/standardobjects/ff/indexed-only", root);
        store.put(&indexed_only, b"kept").unwrap();
        let mut blobs = std::collections::HashMap::new();
        blobs.insert(
            "indexed-only".into(),
            crate::core::model::BlobLoc::unpacked(
                "indexed-only".into(),
                "standardobjects/ff/indexed-only".into(),
                4,
            ),
        );
        BlobIndex::save_journal(&store, &root, &blobs, &std::collections::HashMap::new()).unwrap();

        let dry = prune_orphans(&c, &store, &CancelToken::new(), true).unwrap();
        assert_eq!(dry.objects_deleted, 3);
        assert!(store.head(&orphan_pack).unwrap().is_some());

        let summary = prune_orphans(&c, &store, &CancelToken::new(), false).unwrap();
        assert_eq!(summary.objects_deleted, 3);
        assert!(store.head(&orphan_pack).unwrap().is_none());
        assert!(store.head(&orphan_std).unwrap().is_none());
        assert!(store.head(&orphan_tree).unwrap().is_none());
        assert!(store.head(&live_pack).unwrap().is_some());
        assert!(store.head(&indexed_only).unwrap().is_some());

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn orphan_cleanup_keeps_record_paths_even_with_an_empty_index() {
        let src = std::env::temp_dir().join(format!("orphan-rec-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("orphan-rec-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("keep.txt"), b"record references this").unwrap();

        let store = LocalStore::new(&dir).unwrap();
        let mut c = cfg();
        c.source_path = src.to_string_lossy().to_string();
        c.schedule_minutes = 0;
        crate::core::engine::run_backup(&mut c, &store, CancelToken::new(), &mut |_| {}).unwrap();

        let root = c.key_root();
        let mut before = Vec::new();
        for prefix in ORPHAN_GC_PREFIXES {
            before.extend(
                store
                    .list(&record::root_key(&root, prefix))
                    .unwrap()
                    .into_iter()
                    .map(|object| object.key),
            );
        }
        assert!(!before.is_empty());

        // Simulate a lost/incomplete index: the record alone must still protect
        // every physical object it references.
        BlobIndex::default().save(&store, &root, &c).unwrap();
        let summary = prune_orphans(&c, &store, &CancelToken::new(), false).unwrap();
        assert_eq!(
            summary.objects_deleted, 0,
            "record-referenced objects must never be deleted"
        );
        for key in before {
            assert!(store.head(&key).unwrap().is_some(), "{key} was deleted");
        }

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }
}
