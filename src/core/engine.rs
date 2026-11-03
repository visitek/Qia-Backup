//! Backup engine: walks a source tree, chunks files, deduplicates blobs,
//! uploads packs, and writes backup records.
//!
//! Scale note: walking, chunking and hashing are currently sequential. For
//! multi-million-file sources the next throughput win is a bounded parallel
//! scan/chunk/hash pipeline. Sparse-file holes are also read as zeros today;
//! using SEEK_HOLE/SEEK_DATA would cut both read and upload volume.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Read;
#[cfg(target_os = "macos")]
use std::os::macos::fs::MetadataExt as MacMetadataExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use chrono::Utc;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};

use crate::core::chunker::{Chunker, ChunkerConfig};
use crate::core::codec;
use crate::core::compression::compress_blob;
use crate::core::config;
use crate::core::crypto::{MasterKeys, ObjectEncryptor, encrypt_keyset, sha256_hex};
use crate::core::error::{BackupError, IoContext, Result};
use crate::core::index::{BlobIndex, PackInfo};
use crate::core::logging;
use crate::core::model::{
    ALWAYS_IGNORED_METADATA, AppConfig, BACKUP_FORMAT_VERSION, BackupConfig, BackupFolder,
    BackupFolders, BackupRecordError, BlobLoc, NODE_TREE_VERSION, Node, Tree,
};
use crate::core::partial::PartialTree;
use crate::core::record;
use crate::core::recover;
use crate::core::retention;
use crate::core::snapshot::{self, PreparedSource};
use crate::core::store::BlobStore;
use crate::core::upload::{MAX_UPLOAD_THREADS, UploadDone, UploadKind, UploadPool};

/// Blobs up to one pack's worth are appended to a blobpack; larger ones are
/// uploaded standalone. Trees are packed too, which keeps S3 object counts low
/// for a large first backup. This does not change the 1 MiB chunking, so blob
/// IDs already uploaded by an interrupted run remain reusable.
const MAX_PACKED_BLOB: usize = 64 * 1024 * 1024;
const ERROR_LIMIT: usize = 10_000;
/// Persist a durable journal checkpoint after this much new file data, or this
/// long since the previous checkpoint. Checkpoints contain only entries
/// confirmed since the last one, so they stay small as the index grows.
#[cfg(not(test))]
const CHECKPOINT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
// Small enough that the unit tests exercise journal creation and replay.
#[cfg(test)]
const CHECKPOINT_BYTES: u64 = 64 * 1024;
const CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
/// Compact journals into the base index after this many segments. This bounds
/// startup work for a run that keeps getting interrupted.
const JOURNAL_COMPACT_SEGMENTS: usize = 64;

#[derive(Clone, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            logging::warn("cancellation requested");
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            logging::warn("cancellation acknowledged; stopping at the next safe point");
            Err(BackupError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSummary {
    pub record_key: String,
    pub started_at: chrono::DateTime<Utc>,
    pub finished_at: chrono::DateTime<Utc>,
    pub files: u64,
    pub directories: u64,
    pub bytes_scanned: u64,
    pub bytes_uploaded: u64,
    pub blobs_new: u64,
    pub blobs_reused: u64,
    pub errors: u64,
    pub duration_seconds: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ProgressEvent {
    Phase {
        name: String,
        detail: String,
    },
    File {
        files: u64,
        bytes_scanned: u64,
    },
    /// Totals from the previous backup record, used to estimate progress.
    /// Zero means unknown (for example during the first backup).
    Totals {
        files: u64,
        bytes: u64,
    },
    Upload {
        key: String,
        bytes: u64,
    },
    /// Live upload pool statistics for the transfer UI.
    Transfer {
        /// Live aggregate estimate (active workers x per-worker rate).
        speed_bps: u64,
        /// Aggregate throughput measured over the meter window.
        measured_bps: u64,
        /// Single-worker throughput from bytes / active-worker-seconds.
        per_worker_bps: u64,
        /// Fraction of the worker target that was actually transferring.
        utilization: f64,
        target_threads: u32,
        max_threads: u32,
        in_flight: u32,
    },
    Warning {
        message: String,
    },
    Finished {
        summary: BackupSummary,
    },
}

#[derive(Debug, Default, Clone)]
struct Stats {
    files: u64,
    directories: u64,
    bytes_scanned: u64,
    bytes_uploaded: u64,
    blobs_new: u64,
    blobs_reused: u64,
    errors: u64,
}

struct PackBuilder {
    path: String,
    bytes: Vec<u8>,
    blob_ids: Vec<String>,
}

impl PackBuilder {
    fn new() -> Self {
        Self {
            path: format!("blobpacks/{}.blobpack", uuid::Uuid::new_v4()),
            bytes: Vec::new(),
            blob_ids: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn reset(&mut self) {
        self.path = format!("blobpacks/{}.blobpack", uuid::Uuid::new_v4());
        self.bytes = Vec::new();
        self.blob_ids = Vec::new();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlobClass {
    Data,
    Tree,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncludeDecision {
    Skip,
    /// The child is an ancestor directory on the way to a selected folder.
    Descend,
    /// The child is selected, or is inside a selected folder.
    All,
}

/// Decides whether a path should be walked for the current included-folder
/// configuration. Ancestor directories are kept so the record remains
/// navigable, but their non-included children are skipped.
#[derive(Debug, Clone)]
struct IncludeFilter {
    root: bool,
    paths: Vec<String>,
}

impl IncludeFilter {
    fn new(config: &AppConfig) -> Result<Self> {
        let normalized = config::normalize_included_folders(&config.included_folders)?;
        if normalized.is_empty() {
            // A path source without explicit includes still means its whole
            // selected path. Snapshot sources are rejected before reaching
            // this point by validate_backup_selection.
            return Ok(Self {
                root: config.source_kind == "path",
                paths: Vec::new(),
            });
        }
        if normalized.iter().any(String::is_empty) {
            return Ok(Self {
                root: true,
                paths: Vec::new(),
            });
        }
        Ok(Self {
            root: false,
            paths: normalized,
        })
    }

    /// True when the current directory is a selected folder or inside one, so
    /// every child can be included without further filtering.
    fn includes_all(&self, rel: &str) -> bool {
        if self.root {
            return true;
        }
        self.paths
            .iter()
            .any(|path| rel == path || (!rel.is_empty() && rel.starts_with(&format!("{path}/"))))
    }

    fn child_decision(&self, rel: &str, is_dir: bool) -> IncludeDecision {
        if self.includes_all(rel) {
            return IncludeDecision::All;
        }
        if is_dir
            && self
                .paths
                .iter()
                .any(|path| path.starts_with(&format!("{rel}/")))
        {
            return IncludeDecision::Descend;
        }
        IncludeDecision::Skip
    }
}

pub struct BackupEngine<'a> {
    store: &'a dyn BlobStore,
    root: String,
    config: &'a AppConfig,
    index: BlobIndex,
    keys: Option<MasterKeys>,
    encryptor: Option<ObjectEncryptor>,
    pack: PackBuilder,
    pack_limit: usize,
    stats: Stats,
    errors: Vec<BackupRecordError>,
    excludes: GlobSet,
    includes: IncludeFilter,
    cancel: CancelToken,
    progress: &'a mut dyn FnMut(ProgressEvent),
    source_dev: u64,
    hardlinks: HashMap<(u64, u64), Vec<BlobLoc>>,
    root_metadata: Option<fs::Metadata>,
    /// Blobs uploaded since the last journal checkpoint, keyed by identifier.
    pending_blobs: HashSet<String>,
    /// Pack objects successfully uploaded since the last journal checkpoint.
    pending_packs: HashSet<String>,
    /// New blobs that are still queued/in flight and must not be journaled yet.
    unconfirmed_blobs: HashSet<String>,
    bytes_since_checkpoint: u64,
    last_checkpoint: Instant,
    in_checkpoint: bool,
    uploads: UploadPool,
    upload_error: Option<String>,
    /// Live tree for browsing an in-progress run before its record exists.
    partial: Option<PartialTree>,
    last_transfer_emit: Instant,
    last_upload_target: usize,
}

impl<'a> BackupEngine<'a> {
    fn new(
        store: &'a dyn BlobStore,
        config: &'a AppConfig,
        index: BlobIndex,
        keys: Option<MasterKeys>,
        cancel: CancelToken,
        uploads: UploadPool,
        partial: Option<PartialTree>,
        progress: &'a mut dyn FnMut(ProgressEvent),
    ) -> Result<Self> {
        let excludes = build_excludes(&config.exclude_patterns)?;
        let includes = IncludeFilter::new(config)?;
        let pack_limit = config
            .pack_size_bytes
            .clamp(1024 * 1024, 5 * 1024 * 1024 * 1024) as usize;
        let encryptor = keys.as_ref().map(|k| ObjectEncryptor::new(k.clone()));
        Ok(Self {
            store,
            root: config.key_root(),
            config,
            index,
            keys,
            encryptor,
            pack: PackBuilder::new(),
            pack_limit,
            stats: Stats::default(),
            errors: Vec::new(),
            excludes,
            includes,
            cancel,
            progress,
            source_dev: 0,
            hardlinks: HashMap::new(),
            root_metadata: None,
            pending_blobs: HashSet::new(),
            pending_packs: HashSet::new(),
            unconfirmed_blobs: HashSet::new(),
            bytes_since_checkpoint: 0,
            last_checkpoint: Instant::now(),
            in_checkpoint: false,
            uploads,
            upload_error: None,
            partial,
            last_transfer_emit: Instant::now(),
            last_upload_target: 0,
        })
    }

    fn phase(&mut self, name: &str, detail: impl Into<String>) {
        (self.progress)(ProgressEvent::Phase {
            name: name.to_string(),
            detail: detail.into(),
        });
    }

    fn add_error(
        &mut self,
        abs: &Path,
        rel: &str,
        severity: u32,
        message: impl Into<String>,
        error_code: i64,
    ) {
        self.stats.errors += 1;
        let message = message.into();
        if self.errors.len() < ERROR_LIMIT {
            self.errors.push(BackupRecordError {
                error_message: message.clone(),
                local_path: abs.to_string_lossy().to_string(),
                relative_path: rel.to_string(),
                volume_name: String::new(),
                disk_identifier: String::new(),
                path_is_directory: false,
                severity,
                error_domain: "Qia Backup".to_string(),
                error_code,
            });
        }
        logging::warn(format!("{rel}: {message}"));
        (self.progress)(ProgressEvent::Warning {
            message: format!("{rel}: {message}"),
        });
    }

    fn ensure_metadata(&mut self) -> Result<()> {
        let config_key = record::backup_config_key(&self.root);
        let existing: Option<BackupConfig> = match self.store.get_opt(&config_key)? {
            Some(bytes) => Some(
                serde_json::from_slice(&bytes)
                    .map_err(|e| BackupError::Config(format!("invalid backupconfig.json: {e}")))?,
            ),
            None => None,
        };
        if let Some(existing) = &existing {
            if existing.blob_identifier_type != 2 {
                return Err(BackupError::Config(format!(
                    "unsupported blob identifier type {} (expected SHA-256)",
                    existing.blob_identifier_type
                )));
            }
            if existing.version > BACKUP_FORMAT_VERSION {
                return Err(BackupError::Config(format!(
                    "backup set uses newer format version {} (this app supports up to {BACKUP_FORMAT_VERSION}); upgrade the app before writing to it",
                    existing.version
                )));
            }
            if existing.is_encrypted != self.config.encryption_enabled {
                return Err(BackupError::Config(format!(
                    "backup set encryption setting ({}) does not match configuration ({})",
                    existing.is_encrypted, self.config.encryption_enabled
                )));
            }
        }

        let keyset_key = record::root_key(&self.root, "encryptedkeyset.dat");
        if self.config.encryption_enabled {
            if self.config.encryption_password.is_empty() {
                return Err(BackupError::Config(
                    "encryption is enabled but the password is empty".into(),
                ));
            }
            let keys = match self.store.get_opt(&keyset_key)? {
                Some(bytes) => {
                    crate::core::crypto::decrypt_keyset(&bytes, &self.config.encryption_password)?
                }
                None => {
                    if existing.as_ref().is_some_and(|c| c.is_encrypted) {
                        return Err(BackupError::Config(
                            "backup set is marked encrypted but encryptedkeyset.dat is missing"
                                .into(),
                        ));
                    }
                    if !self.index.blobs.is_empty() || !self.index.packs.is_empty() {
                        return Err(BackupError::Config(
                            "an interrupted unencrypted backup exists at this destination; \
                             disable encryption to resume it, or start a new backup set"
                                .into(),
                        ));
                    }
                    let keys = crate::core::crypto::new_master_keys();
                    let bytes = encrypt_keyset(&keys, &self.config.encryption_password)?;
                    self.store.put(&keyset_key, &bytes)?;
                    logging::info("created encrypted master keyset");
                    keys
                }
            };
            self.encryptor = Some(ObjectEncryptor::new(keys.clone()));
            self.keys = Some(keys);
        } else {
            if self.store.get_opt(&keyset_key)?.is_some() {
                return Err(BackupError::Config(
                    "the backup set is encrypted; enter its password (or create a new backup set)"
                        .into(),
                ));
            }
            self.keys = None;
            self.encryptor = None;
        }

        if existing.is_none() {
            let backup_config = BackupConfig {
                is_encrypted: self.config.encryption_enabled,
                ..BackupConfig::new(
                    self.config.display_name(),
                    config::computer_name(),
                    config::computer_serial(),
                )
            };
            self.store
                .put(&config_key, &serde_json::to_vec_pretty(&backup_config)?)?;
            logging::info(format!("created backupconfig at {config_key}"));
        }

        let plan_key = record::backup_plan_key(&self.root);
        if self.store.get_opt(&plan_key)?.is_none() {
            let plan = self.config.plan();
            self.store
                .put(&plan_key, &serde_json::to_vec_pretty(&plan)?)?;
        }

        let folders_key = record::backup_folders_key(&self.root);
        if self.store.get_opt(&folders_key)?.is_none() {
            let folders = BackupFolders::new(&self.root);
            self.store
                .put(&folders_key, &serde_json::to_vec_pretty(&folders)?)?;
        }

        let folder_key = record::backup_folder_key(&self.root, &self.config.backup_set_uuid);
        if self.store.get_opt(&folder_key)?.is_none() {
            let folder = BackupFolder {
                local_path: self.config.effective_source(),
                local_mount_point: self.config.effective_source(),
                name: self.config.display_name(),
                source_path: self.config.source_path.clone(),
                source_kind: self.config.source_kind.clone(),
                ..BackupFolder::new(
                    self.config.backup_set_uuid.clone(),
                    self.config.effective_source(),
                )
            };
            self.store
                .put(&folder_key, &serde_json::to_vec_pretty(&folder)?)?;
        }
        Ok(())
    }

    fn store_blob(&mut self, plaintext: &[u8], class: BlobClass) -> Result<BlobLoc> {
        self.cancel.check()?;
        if let Some(e) = &self.upload_error {
            return Err(BackupError::Store(e.clone()));
        }
        let id = sha256_hex(plaintext);
        if let Some(loc) = self.index.blob(&id) {
            self.stats.blobs_reused += 1;
            return Ok(loc.clone());
        }

        let compressed = compress_blob(plaintext);
        let compression_type = compressed.compression_type;
        let encrypted = self.encryptor.is_some();
        let stored = match self.encryptor.as_mut() {
            Some(enc) => enc.encrypt(&compressed.bytes)?,
            None => compressed.bytes,
        };

        let stored_len = stored.len();
        // Cache the stored bytes now so the next incremental run can read
        // unchanged trees locally instead of issuing one S3 GET per directory.
        // Content addressing makes a stale entry safe (the same id always
        // decodes to the same bytes). Trees are packed like data blobs unless a
        // single encoded tree is larger than the pack limit.
        if class == BlobClass::Tree {
            crate::core::record::cache_tree_blob(&id, &stored);
        }
        let loc = if stored_len > MAX_PACKED_BLOB {
            let first = &id[..2.min(id.len())];
            let relative = format!("standardobjects/{first}/{id}");
            let key = record::root_key(&self.root, &relative);
            if let Err(e) = self.uploads.submit(
                key,
                stored,
                UploadKind::Standalone {
                    blob_id: id.clone(),
                },
            ) {
                return Err(BackupError::Store(e.to_string()));
            }
            self.pump_uploads();
            BlobLoc {
                blob_identifier: id.clone(),
                is_packed: false,
                relative_path: relative,
                offset: 0,
                length: stored_len as u64,
                stretch_encryption_key: encrypted,
                compression_type,
            }
        } else {
            if !self.pack.is_empty() && self.pack.bytes.len() + stored_len > self.pack_limit {
                self.flush_pack()?;
            }
            let offset = self.pack.bytes.len() as u64;
            let relative = self.pack.path.clone();
            self.pack.bytes.extend_from_slice(&stored);
            self.pack.blob_ids.push(id.clone());
            BlobLoc {
                blob_identifier: id.clone(),
                is_packed: true,
                relative_path: relative,
                offset,
                length: stored_len as u64,
                stretch_encryption_key: encrypted,
                compression_type,
            }
        };

        self.stats.blobs_new += 1;
        self.index.add_blob(loc.clone());
        // Until the containing upload completes this blob must not appear in a
        // journal checkpoint: a crash must never leave the index pointing at
        // an object that was never written.
        self.unconfirmed_blobs.insert(id);
        Ok(loc)
    }

    /// Drop the blobs of a pack that will never be uploaded from the index, so
    /// a later checkpoint cannot point at a missing object.
    fn forget_pack(&mut self, path: &str, ids: &[String]) {
        for id in ids {
            self.index.blobs.remove(id);
            self.unconfirmed_blobs.remove(id);
            self.pending_blobs.remove(id);
        }
        self.index.packs.remove(path);
        self.pending_packs.remove(path);
    }

    fn discard_open_pack(&mut self) {
        if self.pack.is_empty() {
            return;
        }
        let path = std::mem::take(&mut self.pack.path);
        let ids = std::mem::take(&mut self.pack.blob_ids);
        self.forget_pack(&path, &ids);
        self.pack.reset();
    }

    fn flush_pack(&mut self) -> Result<()> {
        if self.pack.is_empty() {
            self.pack.reset();
            return Ok(());
        }
        if let Some(e) = self.upload_error.clone() {
            self.discard_open_pack();
            return Err(BackupError::Store(e));
        }
        // Do not start another potentially slow upload after cancellation;
        // already-checkpointed packs remain reusable on the next run.
        if let Err(e) = self.cancel.check() {
            self.discard_open_pack();
            return Err(e);
        }
        let path = self.pack.path.clone();
        let bytes = std::mem::take(&mut self.pack.bytes);
        let ids = std::mem::take(&mut self.pack.blob_ids);
        let key = record::root_key(&self.root, &path);
        let size = bytes.len() as u64;
        self.phase("uploading", format!("{path} ({size} bytes)"));
        let submitted = self.uploads.submit(
            key,
            bytes,
            UploadKind::Pack {
                path: path.clone(),
                blob_ids: ids.clone(),
            },
        );
        self.pack.reset();
        if let Err(e) = submitted {
            // The pack was never queued, so its index entries must not be
            // checkpointed.
            self.forget_pack(&path, &ids);
            self.upload_error = Some(e.to_string());
            return Err(BackupError::Store(e.to_string()));
        }
        self.pump_uploads();
        Ok(())
    }

    /// Apply a completed upload to the index and statistics. Failures are
    /// remembered and reported by `drain_uploads`; entries that were never
    /// uploaded are removed so a checkpoint cannot reference missing objects.
    fn handle_upload_done(&mut self, done: UploadDone) {
        let UploadDone {
            key,
            bytes,
            kind,
            result,
            ..
        } = done;
        // A PUT can succeed while the response is lost (connection reset
        // after S3 committed the object). Trust the object, not the error, so
        // a transient network glitch does not discard a valid upload.
        let result = match result {
            Err(e) if !matches!(e, BackupError::Cancelled) => match self.store.head(&key) {
                Ok(Some(info)) if info.size == bytes => {
                    logging::warn(format!(
                        "upload {key} reported an error ({e}) but the object exists; treating as success"
                    ));
                    Ok(())
                }
                _ => Err(e),
            },
            other => other,
        };
        match result {
            Ok(()) => {
                match kind {
                    UploadKind::Pack { path, blob_ids } => {
                        logging::debug(format!("uploaded pack {key} ({bytes} bytes)"));
                        for id in &blob_ids {
                            if self.unconfirmed_blobs.remove(id) {
                                self.pending_blobs.insert(id.clone());
                            }
                        }
                        self.index.add_pack(path.clone(), bytes);
                        self.pending_packs.insert(path);
                    }
                    UploadKind::Standalone { blob_id } => {
                        if self.unconfirmed_blobs.remove(&blob_id) {
                            self.pending_blobs.insert(blob_id);
                        }
                    }
                }
                self.stats.bytes_uploaded += bytes;
                (self.progress)(ProgressEvent::Upload { key, bytes });
            }
            Err(e) => {
                match kind {
                    UploadKind::Pack { path, blob_ids } => {
                        for id in &blob_ids {
                            self.index.blobs.remove(id);
                            self.unconfirmed_blobs.remove(id);
                            self.pending_blobs.remove(id);
                        }
                        self.index.packs.remove(&path);
                        self.pending_packs.remove(&path);
                    }
                    UploadKind::Standalone { blob_id } => {
                        self.index.blobs.remove(&blob_id);
                        self.unconfirmed_blobs.remove(&blob_id);
                        self.pending_blobs.remove(&blob_id);
                    }
                }
                if !matches!(e, BackupError::Cancelled) && self.upload_error.is_none() {
                    self.upload_error = Some(e.to_string());
                }
            }
        }
    }

    fn pump_uploads(&mut self) {
        while let Some(done) = self.uploads.try_take() {
            self.handle_upload_done(done);
        }
        self.uploads.tick();
        self.emit_transfer_stats(false);
    }

    fn emit_transfer_stats(&mut self, force: bool) {
        let stats = self.uploads.stats();
        let changed = stats.target_threads != self.last_upload_target;
        if !force && !changed && self.last_transfer_emit.elapsed() < Duration::from_secs(5) {
            return;
        }
        self.last_transfer_emit = Instant::now();
        self.last_upload_target = stats.target_threads;
        (self.progress)(ProgressEvent::Transfer {
            speed_bps: stats.speed_bps,
            measured_bps: stats.measured_bps,
            per_worker_bps: stats.per_worker_bps,
            utilization: stats.utilization,
            target_threads: stats.target_threads as u32,
            max_threads: stats.max_threads as u32,
            in_flight: stats.in_flight as u32,
        });
    }

    /// Wait for every queued upload to finish and report the first real
    /// failure. Cancelled uploads are dropped from the index but do not mask a
    /// successful checkpoint of the uploads that did complete.
    fn drain_uploads(&mut self) -> Result<()> {
        while let Some(done) = self.uploads.wait_one() {
            self.handle_upload_done(done);
        }
        self.uploads.tick();
        self.emit_transfer_stats(true);
        // A worker can disappear without reporting (panic or a broken done
        // channel). Never let the base index reference such blobs: drop them
        // so the next run re-uploads them instead of restoring from missing
        // objects.
        if !self.unconfirmed_blobs.is_empty() {
            let missing: Vec<String> = self.unconfirmed_blobs.drain().collect();
            for id in &missing {
                self.index.blobs.remove(id);
                self.pending_blobs.remove(id);
            }
            let message = format!(
                "{} uploads did not report completion; they will be retried",
                missing.len()
            );
            logging::warn(&message);
            if self.upload_error.is_none() {
                self.upload_error = Some(message);
            }
        }
        match &self.upload_error {
            Some(e) => Err(BackupError::Store(e.clone())),
            None => Ok(()),
        }
    }

    /// Persist only the blobs and packs confirmed since the last checkpoint.
    /// This never waits for in-flight uploads, so it can run mid-file without
    /// stalling the walk. Anything still in flight is simply journaled by a
    /// later checkpoint.
    fn checkpoint_index(&mut self) -> Result<()> {
        if self.in_checkpoint {
            return Ok(());
        }
        self.in_checkpoint = true;
        let result = self.checkpoint_index_inner();
        self.in_checkpoint = false;
        result
    }

    /// Persist confirmed uploads that are not yet covered by a journal.
    /// Returns whether a journal segment was written.
    fn save_pending_journal(&mut self) -> Result<bool> {
        if self.pending_blobs.is_empty() && self.pending_packs.is_empty() {
            return Ok(false);
        }
        let blobs: HashMap<String, BlobLoc> = self
            .pending_blobs
            .iter()
            .filter_map(|id| {
                self.index
                    .blobs
                    .get(id)
                    .map(|loc| (id.clone(), loc.clone()))
            })
            .collect();
        let packs: HashMap<String, PackInfo> = self
            .pending_packs
            .iter()
            .filter_map(|path| {
                self.index
                    .packs
                    .get(path)
                    .map(|pack| (path.clone(), pack.clone()))
            })
            .collect();
        BlobIndex::save_journal(self.store, &self.root, &blobs, &packs)?;
        // Nothing else mutates these sets while we are on the walk thread,
        // so everything pending is either in the journal or stale.
        self.pending_blobs.clear();
        self.pending_packs.clear();
        self.index.journal_segments += 1;
        Ok(true)
    }

    /// Best-effort persistence of every upload that already succeeded. Used
    /// on abort so a network break or local error does not throw away hours of
    /// confirmed progress.
    fn preserve_progress(&mut self) {
        let _ = self.flush_pack();
        let _ = self.drain_uploads();
        let _ = self.checkpoint_index();
        // If the network is still down the journal PUT above also failed. Keep
        // the confirmed set in the local cache so the next run can merge it
        // with the remote base once connectivity returns.
        self.index.updated_at = Utc::now();
        if let Err(e) = self.index.save_local(self.config) {
            logging::warn(format!("could not cache confirmed progress locally: {e}"));
        }
    }

    fn checkpoint_index_inner(&mut self) -> Result<()> {
        self.pump_uploads();
        let upload_error = self.upload_error.clone();

        // Journal confirmed uploads even when another upload failed. The next
        // run can then reuse them instead of starting from zero.
        self.save_pending_journal()?;

        if let Some(e) = upload_error {
            self.bytes_since_checkpoint = 0;
            self.last_checkpoint = Instant::now();
            return Err(BackupError::Store(e));
        }

        self.index.updated_at = Utc::now();
        self.bytes_since_checkpoint = 0;
        self.last_checkpoint = Instant::now();

        if self.index.journal_segments >= JOURNAL_COMPACT_SEGMENTS {
            // A full base index must not reference in-flight objects, so flush
            // the open pack and wait for every upload before compacting.
            if let Err(e) = self.flush_pack() {
                let _ = self.save_pending_journal();
                return Err(e);
            }
            if let Err(e) = self.drain_uploads() {
                let _ = self.save_pending_journal();
                return Err(e);
            }
            if let Err(e) = self
                .index
                .save_and_clear(self.store, &self.root, self.config)
            {
                let _ = self.save_pending_journal();
                return Err(e);
            }
            self.pending_blobs.clear();
            self.pending_packs.clear();
            self.index.journal_segments = 0;
        }
        Ok(())
    }

    fn maybe_checkpoint(&mut self) -> Result<()> {
        if self.bytes_since_checkpoint >= CHECKPOINT_BYTES
            || self.last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL
        {
            self.checkpoint_index()?;
        }
        Ok(())
    }

    fn load_prev_tree(&self, prev: &Node) -> Option<Tree> {
        let loc = prev.tree_blob_loc.as_ref()?;
        match record::load_tree(self.store, &self.root, loc, self.keys.as_ref()) {
            Ok(tree) => Some(tree),
            Err(e) => {
                logging::warn(format!(
                    "could not read previous tree {}: {e}",
                    loc.blob_identifier
                ));
                None
            }
        }
    }

    fn is_excluded(&self, rel: &str, name: &str) -> bool {
        self.excludes.is_match(rel) || self.excludes.is_match(name)
    }

    fn node_from_metadata(&mut self, meta: &fs::Metadata, rel: &str) -> Node {
        let mode = meta.mode();
        let (mtime_sec, mtime_nsec) = system_time_parts(meta.modified().ok());
        let (ctime_sec, ctime_nsec) = (meta.ctime(), meta.ctime_nsec());
        let (create_sec, create_nsec) = system_time_parts(meta.created().ok());
        #[cfg(target_os = "macos")]
        let flags = MacMetadataExt::st_flags(meta) as i32;
        #[cfg(not(target_os = "macos"))]
        let flags = 0i32;
        let _ = rel;
        Node {
            is_tree: false,
            tree_blob_loc: None,
            computer_os_type: 1,
            data_blob_locs: Vec::new(),
            acl_blob_loc: None,
            xattrs_blob_locs: Vec::new(),
            item_size: meta.size(),
            contained_files_count: 0,
            mtime_sec,
            mtime_nsec,
            ctime_sec,
            ctime_nsec,
            create_time_sec: create_sec,
            create_time_nsec: create_nsec,
            username: Some(meta.uid().to_string()),
            group_name: Some(meta.gid().to_string()),
            deleted: false,
            mac_st_dev: meta.dev() as i32,
            mac_st_ino: meta.ino(),
            mac_st_mode: mode,
            mac_st_nlink: meta.nlink() as u32,
            mac_st_uid: meta.uid(),
            mac_st_gid: meta.gid(),
            mac_st_rdev: meta.rdev() as i32,
            mac_st_flags: flags,
            win_attrs: 0,
            win_reparse_tag: 0,
            win_reparse_point_is_directory: false,
            added_time_sec: mtime_sec,
            added_time_nsec: mtime_nsec,
            document_id: 0,
            has_document_id: false,
            is_sparse: false,
            sparse_logical_size: 0,
            holes: Vec::new(),
        }
    }

    fn unchanged(prev: &Node, meta: &fs::Metadata) -> bool {
        prev.mac_st_ino == meta.ino()
            && prev.item_size == meta.size()
            && prev.mtime_sec == meta.mtime()
            && prev.mtime_nsec == meta.mtime_nsec()
            && prev.ctime_sec == meta.ctime()
            && prev.ctime_nsec == meta.ctime_nsec()
            && prev.mac_st_mode == meta.mode()
    }

    fn collect_xattrs(
        &mut self,
        path: &Path,
        symlink_target: Option<&Path>,
    ) -> Result<Option<Vec<u8>>> {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        if let Ok(names) = xattr::list(path) {
            for name in names {
                let name = name.to_string_lossy().to_string();
                if let Ok(Some(value)) = xattr::get(path, &name) {
                    map.insert(
                        name,
                        base64::engine::general_purpose::STANDARD.encode(value),
                    );
                }
            }
        }
        if let Some(target) = symlink_target {
            map.insert(
                "__backup_symlink_target".to_string(),
                base64::engine::general_purpose::STANDARD
                    .encode(target.to_string_lossy().as_bytes()),
            );
        }
        if map.is_empty() {
            return Ok(None);
        }
        Ok(Some(serde_json::to_vec(&map)?))
    }

    fn attach_xattrs(&mut self, node: &mut Node, path: &Path, symlink: bool) -> Result<()> {
        let target = if symlink {
            std::fs::read_link(path).ok()
        } else {
            None
        };
        if let Some(json) = self.collect_xattrs(path, target.as_deref())? {
            let loc = self.store_blob(&json, BlobClass::Data)?;
            node.xattrs_blob_locs.push(loc);
        }
        Ok(())
    }

    fn backup_file(
        &mut self,
        abs: &Path,
        rel: &str,
        meta: &fs::Metadata,
        prev: Option<&Node>,
    ) -> Result<Node> {
        self.cancel.check()?;
        if let Some(prev) = prev {
            if Self::unchanged(prev, meta) {
                let node = prev.clone();
                self.stats.blobs_reused += (node.data_blob_locs.len()
                    + node.xattrs_blob_locs.len()
                    + node.acl_blob_loc.iter().count())
                    as u64;
                self.stats.bytes_scanned += meta.size();
                (self.progress)(ProgressEvent::File {
                    files: self.stats.files,
                    bytes_scanned: self.stats.bytes_scanned,
                });
                self.maybe_checkpoint()?;
                return Ok(node);
            }
        }

        if meta.nlink() > 1 {
            if let Some(locs) = self.hardlinks.get(&(meta.dev(), meta.ino())).cloned() {
                let mut node = self.node_from_metadata(meta, rel);
                node.data_blob_locs = locs.clone();
                self.stats.blobs_reused += locs.len() as u64;
                self.attach_xattrs(&mut node, abs, false)?;
                self.maybe_checkpoint()?;
                return Ok(node);
            }
        }

        let mut node = self.node_from_metadata(meta, rel);
        let mut file = fs::File::open(abs).at(abs)?;
        let cfg = ChunkerConfig::for_target(self.config.chunk_size);
        let mut chunker = Chunker::new(cfg);
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            self.cancel.check()?;
            let n = file.read(&mut buf).at(abs)?;
            if n == 0 {
                break;
            }
            self.stats.bytes_scanned += n as u64;
            self.bytes_since_checkpoint += n as u64;
            let data = &buf[..n];
            chunker.update(data, |chunk| {
                let loc = self.store_blob(chunk, BlobClass::Data)?;
                node.data_blob_locs.push(loc);
                Ok(())
            })?;
            // Checkpoint mid-file so a multi-hour single file (disk image,
            // video) still makes durable progress.
            self.maybe_checkpoint()?;
        }
        chunker.finish(|chunk| {
            let loc = self.store_blob(chunk, BlobClass::Data)?;
            node.data_blob_locs.push(loc);
            Ok(())
        })?;
        // item_size must reflect the actual stored file size.
        node.item_size = meta.size();

        self.attach_xattrs(&mut node, abs, false)?;
        if meta.nlink() > 1 {
            self.hardlinks
                .insert((meta.dev(), meta.ino()), node.data_blob_locs.clone());
        }

        (self.progress)(ProgressEvent::File {
            files: self.stats.files,
            bytes_scanned: self.stats.bytes_scanned,
        });
        self.maybe_checkpoint()?;
        Ok(node)
    }

    fn backup_symlink(
        &mut self,
        abs: &Path,
        rel: &str,
        meta: &fs::Metadata,
        prev: Option<&Node>,
    ) -> Result<Node> {
        // Symlink metadata is cheap to rebuild; content-addressed xattr blobs
        // make this effectively free on the storage side. We still reuse a
        // previous node when it is unchanged to avoid rewriting the tree.
        if let Some(prev) = prev {
            if Self::unchanged(prev, meta) {
                let node = prev.clone();
                self.maybe_checkpoint()?;
                return Ok(node);
            }
        }
        let mut node = self.node_from_metadata(meta, rel);
        node.is_tree = false;
        node.data_blob_locs.clear();
        self.attach_xattrs(&mut node, abs, true)?;
        self.maybe_checkpoint()?;
        Ok(node)
    }

    fn publish_partial_dir(
        &self,
        rel: &str,
        node: &Node,
        children: &[(String, Node)],
        complete: bool,
    ) {
        if let Some(partial) = &self.partial {
            partial.publish_dir(rel, node.clone(), children, complete);
        }
    }

    fn publish_partial_child(&self, parent: &str, name: &str, node: &Node) {
        if let Some(partial) = &self.partial {
            partial.publish_child(parent, name, node.clone());
        }
    }

    fn backup_directory(&mut self, abs: &Path, rel: &str, prev: Option<&Node>) -> Result<Node> {
        self.cancel.check()?;
        let meta = fs::symlink_metadata(abs).at(abs)?;
        if self.source_dev == 0 {
            self.source_dev = meta.dev();
        }
        let mut dir_node = self.node_from_metadata(&meta, rel);
        dir_node.is_tree = true;

        let prev_tree = prev.and_then(|p| self.load_prev_tree(p));
        let mut prev_children: HashMap<String, Node> = HashMap::new();
        if let Some(tree) = &prev_tree {
            for (name, node) in &tree.children {
                prev_children.insert(name.clone(), node.clone());
            }
        }

        let include_all = self.includes.includes_all(rel);
        let mut entries: Vec<(String, PathBuf)> = Vec::new();
        for entry in fs::read_dir(abs).at(abs)? {
            self.cancel.check()?;
            let entry = entry.at(abs)?;
            let name = entry.file_name().to_string_lossy().to_string();
            entries.push((name, entry.path()));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        // Make the directory navigable immediately. Files and child
        // directories become visible as they complete.
        self.publish_partial_dir(rel, &dir_node, &[], false);

        let mut children: Vec<(String, Node)> = Vec::new();
        let mut contained_files = 0u64;
        for (name, path) in entries {
            self.cancel.check()?;
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if self.is_excluded(&child_rel, &name) {
                continue;
            }
            let meta = match fs::symlink_metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    self.add_error(&path, &child_rel, 3, e.to_string(), 1);
                    continue;
                }
            };
            let decision = if include_all {
                IncludeDecision::All
            } else {
                self.includes.child_decision(&child_rel, meta.is_dir())
            };
            if decision == IncludeDecision::Skip
                || (!meta.is_dir() && decision == IncludeDecision::Descend)
            {
                continue;
            }
            let prev_child = prev_children.get(&name);
            if meta.is_dir() && meta.dev() != self.source_dev {
                logging::debug(format!("skipping mount point {child_rel}"));
                continue;
            }
            if meta.is_dir() {
                self.stats.directories += 1;
                match self.backup_directory(&path, &child_rel, prev_child) {
                    Ok(node) => {
                        contained_files += node.contained_files_count;
                        children.push((name, node));
                    }
                    Err(BackupError::Cancelled) => return Err(BackupError::Cancelled),
                    Err(e @ BackupError::Store(_)) => return Err(e),
                    Err(e) => self.add_error(&path, &child_rel, 4, e.to_string(), 2),
                }
            } else if meta.is_file() {
                self.stats.files += 1;
                match self.backup_file(&path, &child_rel, &meta, prev_child) {
                    Ok(node) => {
                        contained_files += 1;
                        self.publish_partial_child(rel, &name, &node);
                        children.push((name, node));
                    }
                    Err(BackupError::Cancelled) => return Err(BackupError::Cancelled),
                    Err(e @ BackupError::Store(_)) => return Err(e),
                    Err(e) => self.add_error(&path, &child_rel, 3, e.to_string(), 3),
                }
            } else if meta.file_type().is_symlink() {
                match self.backup_symlink(&path, &child_rel, &meta, prev_child) {
                    Ok(node) => {
                        self.publish_partial_child(rel, &name, &node);
                        children.push((name, node));
                    }
                    Err(BackupError::Cancelled) => return Err(BackupError::Cancelled),
                    Err(e @ BackupError::Store(_)) => return Err(e),
                    Err(e) => self.add_error(&path, &child_rel, 3, e.to_string(), 4),
                }
            } else {
                // Sockets, fifos and device nodes: record as metadata-only
                // entries so a restore can recreate the node type.
                let node = self.node_from_metadata(&meta, &child_rel);
                self.publish_partial_child(rel, &name, &node);
                children.push((name, node));
            }
        }

        let mut tree = Tree {
            version: NODE_TREE_VERSION,
            children,
        };
        tree.sort();
        let encoded = codec::encode_tree(&tree);
        let tree_loc = self.store_blob(&encoded, BlobClass::Tree)?;
        dir_node.tree_blob_loc = Some(tree_loc);
        dir_node.contained_files_count = contained_files;
        dir_node.item_size = 0;
        self.attach_xattrs(&mut dir_node, abs, false)?;
        // The final tree blob is stored, so the live child map can be dropped;
        // the explorer now loads this directory from its content-addressed tree.
        self.publish_partial_dir(rel, &dir_node, &[], true);
        (self.progress)(ProgressEvent::File {
            files: self.stats.files,
            bytes_scanned: self.stats.bytes_scanned,
        });
        self.maybe_checkpoint()?;
        Ok(dir_node)
    }

    fn finish(
        mut self,
    ) -> Result<(
        Stats,
        Vec<BackupRecordError>,
        BlobIndex,
        Option<ObjectEncryptor>,
    )> {
        if let Err(e) = self.flush_pack() {
            self.preserve_progress();
            return Err(e);
        }
        // The final base index may only reference objects that are actually in
        // S3, so wait for every upload before writing it.
        if let Err(e) = self.drain_uploads() {
            self.preserve_progress();
            return Err(e);
        }
        if let Err(e) = self
            .index
            .save_and_clear(self.store, &self.root, self.config)
        {
            self.preserve_progress();
            return Err(e);
        }
        self.pending_blobs.clear();
        self.pending_packs.clear();
        let encryptor = self.encryptor.take();
        Ok((self.stats, self.errors, self.index, encryptor))
    }
}

pub fn build_excludes(user_patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    // Metadata patterns are hardcoded; skip duplicates if a user also added
    // one of them to their custom exclusions.
    let mut seen = HashSet::new();
    for pattern in ALWAYS_IGNORED_METADATA
        .iter()
        .copied()
        .chain(user_patterns.iter().map(String::as_str))
    {
        if !seen.insert(pattern) {
            continue;
        }
        let glob = Glob::new(pattern)
            .map_err(|e| BackupError::Config(format!("exclude glob {pattern}: {e}")))?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|e| BackupError::Config(format!("exclude patterns: {e}")))
}

fn system_time_parts(value: Option<SystemTime>) -> (i64, i64) {
    let Some(value) = value else {
        return (0, 0);
    };
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos() as i64),
        Err(e) => {
            let duration = e.duration();
            (
                -(duration.as_secs() as i64),
                -(duration.subsec_nanos() as i64),
            )
        }
    }
}

fn ensure_record_key(
    store: &dyn BlobStore,
    root: &str,
    folder_uuid: &str,
) -> Result<(String, i64)> {
    let mut epoch = Utc::now().timestamp();
    for _ in 0..120 {
        let key = record::record_key(root, folder_uuid, epoch);
        if store.head(&key)?.is_none() {
            return Ok((key, epoch));
        }
        epoch += 1;
    }
    Err(BackupError::Store(
        "could not allocate a unique backup record key".into(),
    ))
}

fn load_or_create_keys(
    config: &AppConfig,
    store: &dyn BlobStore,
    root: &str,
) -> Result<Option<MasterKeys>> {
    let key = record::root_key(root, "encryptedkeyset.dat");
    if config.encryption_enabled {
        if config.encryption_password.is_empty() {
            return Err(BackupError::Config(
                "encryption is enabled but the password is empty".into(),
            ));
        }
        match store.get_opt(&key)? {
            Some(bytes) => Ok(Some(crate::core::crypto::decrypt_keyset(
                &bytes,
                &config.encryption_password,
            )?)),
            None => Ok(None),
        }
    } else if store.get_opt(&key)?.is_some() {
        Err(BackupError::Config(
            "the backup set is encrypted; enter its password or create a new backup set".into(),
        ))
    } else {
        Ok(None)
    }
}

/// Ensure every selected included folder exists as a directory under the
/// prepared source. Snapshot mounts and path sources both use this check.
fn validate_included_folders(config: &AppConfig, walk_root: &Path) -> Result<()> {
    let includes = config::normalize_included_folders(&config.included_folders)?;
    if includes.is_empty() {
        return Ok(());
    }
    let mut found = 0usize;
    let mut missing = Vec::new();
    for include in includes {
        let path = if include.is_empty() {
            walk_root.to_path_buf()
        } else {
            walk_root.join(&include)
        };
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() => found += 1,
            Ok(_) => {
                return Err(BackupError::Config(format!(
                    "included path {include:?} is not a folder"
                )));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => missing.push(include),
            Err(e) => {
                return Err(BackupError::Config(format!(
                    "included folder {include:?} does not exist in the source: {e}"
                )));
            }
        }
    }
    if found == 0 {
        return Err(BackupError::Config(format!(
            "none of the included folders exist in the source: {}",
            missing.join(", ")
        )));
    }
    for include in missing {
        logging::warn(format!(
            "included folder {include:?} does not exist in the source; skipping"
        ));
    }
    Ok(())
}

/// Count files and logical bytes under `walk_root` without reading file
/// contents. This mirrors the engine's exclusion and mount-boundary rules
/// closely enough to seed a progress estimate for the first backup.
fn estimate_source_totals(
    config: &AppConfig,
    walk_root: &Path,
    cancel: &CancelToken,
    progress: &mut dyn FnMut(ProgressEvent),
) -> Result<(u64, u64)> {
    let excludes = build_excludes(&config.exclude_patterns)?;
    let includes = IncludeFilter::new(config)?;
    let root_meta = fs::symlink_metadata(walk_root).at(walk_root)?;
    let root_dev = root_meta.dev();
    let mut stack = vec![(
        walk_root.to_path_buf(),
        String::new(),
        includes.includes_all(""),
    )];
    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut skipped = 0u64;
    let mut last_report = Instant::now();

    while let Some((dir, rel, include_all)) = stack.pop() {
        cancel.check()?;
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => {
                // Protected system paths are expected on a full-volume backup.
                // Keep the detail available at debug level, but log only one
                // summary line so the activity log stays readable.
                logging::debug(format!("could not count {}: {e}", dir.display()));
                skipped += 1;
                continue;
            }
        };
        for entry in entries {
            cancel.check()?;
            let Ok(entry) = entry else {
                continue;
            };
            let name = entry.file_name().to_string_lossy().to_string();
            let child_rel = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if excludes.is_match(&child_rel) || excludes.is_match(&name) {
                continue;
            }
            let path = entry.path();
            let meta = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(_) => continue,
            };
            let decision = if include_all {
                IncludeDecision::All
            } else {
                includes.child_decision(&child_rel, meta.is_dir())
            };
            if meta.is_dir() {
                if decision != IncludeDecision::Skip && meta.dev() == root_dev {
                    stack.push((path, child_rel, decision == IncludeDecision::All));
                }
            } else if meta.is_file() && decision == IncludeDecision::All {
                files += 1;
                bytes = bytes.saturating_add(meta.size());
            }
        }
        if last_report.elapsed() >= std::time::Duration::from_secs(1) {
            progress(ProgressEvent::Phase {
                name: "counting".into(),
                detail: format!("{files} files found so far, {bytes} bytes"),
            });
            last_report = Instant::now();
        }
    }
    if skipped > 0 {
        let noun = if skipped == 1 {
            "directory"
        } else {
            "directories"
        };
        logging::warn(format!(
            "counting skipped {skipped} unreadable {noun} (protected/system paths); \
             they are not included in the estimate"
        ));
    }
    Ok((files, bytes))
}

/// Run a full backup. `progress` is called synchronously from the backup
/// thread; it should be cheap and non-blocking.
pub fn run_backup(
    config: &mut AppConfig,
    store: &dyn BlobStore,
    cancel: CancelToken,
    progress: &mut dyn FnMut(ProgressEvent),
) -> Result<BackupSummary> {
    run_backup_with_partial(config, store, cancel, progress, None)
}

/// Run a backup while publishing a live partial tree for the file explorer.
/// The tree is cleared only after a completed record is written; after a
/// cancellation or error it remains available for browsing.
pub fn run_backup_with_partial(
    config: &mut AppConfig,
    store: &dyn BlobStore,
    cancel: CancelToken,
    progress: &mut dyn FnMut(ProgressEvent),
    partial: Option<PartialTree>,
) -> Result<BackupSummary> {
    config::validate(config)?;
    config::validate_source(config)?;
    config::validate_backup_selection(config)?;
    let included_folders = config::normalize_included_folders(&config.included_folders)?;
    if let Some(partial) = &partial {
        partial.begin();
    }
    let started_at = Utc::now();
    let started = Instant::now();
    let root = config.key_root();

    // Fail fast on credentials/keyset before prompting for admin rights.
    let keys = load_or_create_keys(config, store, &root)?;

    let mut index = match BlobIndex::load(store, &root, config) {
        Ok(index) => index,
        Err(BackupError::Store(message)) => {
            // A corrupt/missing index is recoverable when records exist.
            // Transient S3 errors are returned below instead of triggering a
            // potentially expensive rebuild.
            let record_keys = record::list_record_keys(store, &root, &config.backup_set_uuid)?;
            if record_keys.is_empty() {
                return Err(BackupError::Store(message));
            }
            logging::warn(format!(
                "blob index could not be loaded ({message}); rebuilding from {} backup records",
                record_keys.len()
            ));
            progress(ProgressEvent::Phase {
                name: "recovering".into(),
                detail: "rebuilding blob index from existing backup records".into(),
            });
            let rebuilt = {
                let mut report = |detail: String| {
                    progress(ProgressEvent::Phase {
                        name: "recovering".into(),
                        detail,
                    });
                };
                recover::rebuild_index(config, store, keys.as_ref(), &cancel, &mut report)?
            };
            rebuilt
        }
        Err(e) => return Err(e),
    };

    // A set with records but an empty index has lost its dedupe state (for
    // example after a bucket cleanup or an older app version). Rebuild rather
    // than silently treating the destination as empty.
    if index.blobs.is_empty() && index.packs.is_empty() {
        let record_keys = record::list_record_keys(store, &root, &config.backup_set_uuid)?;
        if !record_keys.is_empty() {
            logging::warn(format!(
                "blob index is empty but {} backup records exist; rebuilding it",
                record_keys.len()
            ));
            progress(ProgressEvent::Phase {
                name: "recovering".into(),
                detail: "rebuilding blob index from existing backup records".into(),
            });
            let rebuilt = {
                let mut report = |detail: String| {
                    progress(ProgressEvent::Phase {
                        name: "recovering".into(),
                        detail,
                    });
                };
                recover::rebuild_index(config, store, keys.as_ref(), &cancel, &mut report)?
            };
            index = rebuilt;
        }
    }
    // A marker left over from an earlier run means that run did not reach a
    // completed record (crash, force-quit, cancellation, or record-write
    // failure). The next success can safely collect objects that the earlier
    // run uploaded but never journaled.
    let previous_run_interrupted = match record::partial_run_exists(store, &root) {
        Ok(exists) => exists,
        Err(e) => {
            logging::warn(format!(
                "could not check for an interrupted-run marker: {e}; automatic orphan cleanup is skipped"
            ));
            false
        }
    };

    // Mark the destination while a run is active. The explorer checks this
    // marker before trusting in-memory partial data; a successful run removes
    // it, so stale local data is never presented as remote content.
    let partial_marker = record::partial_run_key(&root);
    if let Err(e) = store.put(&partial_marker, Utc::now().to_rfc3339().as_bytes()) {
        logging::warn(format!(
            "could not write run marker {partial_marker}: {e}; interrupted-run recovery and partial browsing may be unavailable"
        ));
    }

    let previous = {
        let folder_uuid = &config.backup_set_uuid;
        let key = record::latest_record_key(store, &root, folder_uuid)?;
        match key {
            Some(key) => match record::load_record(store, &key, keys.as_ref()) {
                Ok(record) => {
                    let node = record.root_node().ok();
                    // Older/imported records may not carry files_count, so fall
                    // back to the file count stored on the root node.
                    let total_files = if record.files_count > 0 {
                        record.files_count
                    } else {
                        node.as_ref().map_or(0, |n| n.contained_files_count)
                    };
                    Some((
                        key,
                        node,
                        total_files,
                        record.bytes_scanned,
                        record.included_folders.clone(),
                    ))
                }
                Err(e) => {
                    logging::warn(format!("could not read previous record {key}: {e}"));
                    None
                }
            },
            None => None,
        }
    };

    // Estimate the run's size from the previous record so the UI can show a
    // percentage instead of a purely indeterminate bar. A first backup has no
    // previous record and is counted once below instead.
    let previous_totals_match = previous
        .as_ref()
        .is_some_and(|(_, _, _, _, includes)| includes == &included_folders);
    let (previous_files, previous_bytes) = if previous_totals_match {
        previous
            .as_ref()
            .map(|(_, _, files, bytes, _)| (*files, *bytes))
            .unwrap_or((0, 0))
    } else {
        (0, 0)
    };
    if previous_files > 0 || previous_bytes > 0 {
        progress(ProgressEvent::Totals {
            files: previous_files,
            bytes: previous_bytes,
        });
    }

    progress(ProgressEvent::Phase {
        name: "snapshot".into(),
        detail: config.source_description(),
    });
    let prepared = snapshot::prepare_source(config)?;
    let PreparedSource {
        walk_root,
        local_path,
        local_mount_point,
        volume_name,
        snapshot_name,
        snapshot: snapshot_guard,
    } = prepared;
    validate_included_folders(config, &walk_root)?;

    // A first backup has no previous record to provide totals. Walk the
    // snapshot's metadata once (no file contents are read) so the progress bar
    // still has a denominator. Later runs reuse the previous record's totals,
    // which is much cheaper.
    if previous_files == 0 {
        progress(ProgressEvent::Phase {
            name: "counting".into(),
            detail: "estimating the number of files to back up".into(),
        });
        let (files, bytes) = estimate_source_totals(config, &walk_root, &cancel, &mut *progress)?;
        if files > 0 || bytes > 0 {
            progress(ProgressEvent::Totals { files, bytes });
        }
    }

    let plan = config.plan();
    let configured_threads = plan.thread_count.clamp(1, 16) as usize;
    let max_threads = MAX_UPLOAD_THREADS;
    // Start conservatively and let measured throughput ramp the pool up. The
    // configured thread count is only a starting hint; a slow link quickly
    // scales back to one worker and a fast link grows toward the hard cap.
    let initial_threads = configured_threads.clamp(1, 2);
    let pack_limit = config
        .pack_size_bytes
        .clamp(1024 * 1024, 5 * 1024 * 1024 * 1024) as usize;
    let budget = max_threads
        .saturating_mul(pack_limit)
        .min(1024 * 1024 * 1024);

    std::thread::scope(|scope| {
        let uploads = UploadPool::start_with_target(
            scope,
            store,
            cancel.clone(),
            max_threads,
            initial_threads,
            budget,
        );
        let mut engine = BackupEngine::new(
            store,
            config,
            index,
            keys,
            cancel.clone(),
            uploads,
            partial.clone(),
            progress,
        )?;
        engine.ensure_metadata()?;
        engine.phase("scanning", walk_root.to_string_lossy());
        let root_meta = fs::symlink_metadata(&walk_root).at(&walk_root)?;
        engine.source_dev = root_meta.dev();
        engine.root_metadata = Some(root_meta);

        let previous_root = previous
            .as_ref()
            .and_then(|(_, node, _, _, _)| node.as_ref());
        let result = engine.backup_directory(&walk_root, "", previous_root);
        drop(snapshot_guard);
        let root_node = match result {
            Ok(node) => {
                // Do not commit a record if cancel was requested after the last
                // walk-time check. Preserve confirmed uploads for the next run.
                if let Err(e) = cancel.check() {
                    engine.preserve_progress();
                    return Err(e);
                }
                node
            }
            Err(e) => {
                engine.preserve_progress();
                return Err(e);
            }
        };
        engine.phase("uploading", "writing final pack and backup record");
        let (stats, errors, index, mut record_encryptor) = engine.finish()?;
        let disk_identifier = snapshot::disk_identifier(Path::new(&local_path));
        let mut record = record::make_record(
            &plan,
            &config.backup_set_uuid,
            &root_node,
            &local_path,
            &local_mount_point,
            &volume_name,
            &disk_identifier,
            &config.source_kind,
            &snapshot_name,
            stats.files,
            stats.bytes_uploaded,
            stats.bytes_scanned,
            0.0,
            errors.clone(),
        )?;
        record.duration_seconds = started.elapsed().as_secs_f64();
        record.included_folders = included_folders.clone();

        let (record_key, _epoch) = ensure_record_key(store, &root, &config.backup_set_uuid)?;
        let record_bytes = record::encode_record(&record, record_encryptor.as_mut())?;
        store.put(&record_key, &record_bytes)?;
        // The completed record now supersedes the live partial tree.
        if let Some(partial) = &partial {
            partial.clear();
        }
        if let Err(e) = store.delete(&partial_marker) {
            logging::warn(format!(
                "could not remove partial-run marker {partial_marker}: {e}"
            ));
        }

        // The just-saved index is already in memory. Reuse it to scan only the
        // data prefixes for objects left behind by the interrupted run. A
        // cleanup failure must never invalidate an otherwise complete backup.
        if previous_run_interrupted {
            (progress)(ProgressEvent::Phase {
                name: "cleaning".into(),
                detail: "removing objects left by the interrupted run".into(),
            });
            match retention::prune_orphans_with_index(config, store, &index, &cancel, false) {
                Ok(cleanup) => logging::info(format!(
                    "interrupted-run cleanup: scanned {} objects, deleted {} orphaned objects",
                    cleanup.objects_scanned, cleanup.objects_deleted
                )),
                Err(e) => logging::warn(format!(
                    "interrupted-run cleanup failed; the backup itself is complete: {e}"
                )),
            }
        }

        // The complete index was already persisted by `engine.finish()` after
        // every upload drained, so uploaded blobs remain deduplicable even if
        // this record write had failed. Saving it again here would only
        // re-serialize and re-upload the whole index for no new entries.
        config.touch_backup_time();

        let finished_at = Utc::now();
        let summary = BackupSummary {
            record_key,
            started_at,
            finished_at,
            files: stats.files,
            directories: stats.directories,
            bytes_scanned: stats.bytes_scanned,
            bytes_uploaded: stats.bytes_uploaded + record_bytes.len() as u64,
            blobs_new: stats.blobs_new,
            blobs_reused: stats.blobs_reused,
            errors: stats.errors,
            duration_seconds: started.elapsed().as_secs_f64(),
        };
        logging::info(format!(
            "backup complete: {} files, {} bytes scanned, {} bytes uploaded, {} errors, {:.1}s",
            summary.files,
            summary.bytes_scanned,
            summary.bytes_uploaded,
            summary.errors,
            summary.duration_seconds
        ));
        (progress)(ProgressEvent::Finished {
            summary: summary.clone(),
        });
        Ok(summary)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::partial::PartialTree;
    use crate::core::store::LocalStore;

    #[test]
    fn full_backup_and_restore_roundtrip() {
        let src = std::env::temp_dir().join(format!("backup-src-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("backup-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("hello.txt"), b"hello qia\n").unwrap();
        std::fs::write(src.join("sub/data.bin"), vec![7u8; 900_000]).unwrap();

        let store = LocalStore::new(&dst).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "test".into();
        cfg.region = "us-east-1".into();
        cfg.access_key_id = "test".into();
        cfg.secret_access_key = "test".into();
        cfg.prefix = "prefix".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;

        let mut events = Vec::new();
        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |e| {
            events.push(e)
        })
        .unwrap();
        assert!(summary.files >= 2);
        assert!(summary.bytes_uploaded > 0);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProgressEvent::Finished { .. }))
        );
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProgressEvent::Transfer { max_threads, .. }
                    if *max_threads == MAX_UPLOAD_THREADS as u32
            )),
            "backup should report upload pool statistics"
        );
        let first_totals = events.iter().find_map(|e| match e {
            ProgressEvent::Totals { files, bytes } => Some((*files, *bytes)),
            _ => None,
        });
        assert_eq!(
            first_totals.map(|(files, _)| files),
            Some(2),
            "first backup should count the source for a progress estimate"
        );
        assert!(
            first_totals.map(|(_, bytes)| bytes).unwrap_or(0) > 0,
            "first backup should estimate the source's byte size"
        );

        let records =
            record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid).unwrap();
        assert_eq!(records.len(), 1);

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn excludes_are_valid() {
        let set = build_excludes(&["**/*.tmp".to_string()]).unwrap();
        assert!(set.is_match("a/b.tmp"));
        assert!(!set.is_match("a/b.txt"));
    }

    #[test]
    fn metadata_is_always_excluded_but_broad_paths_are_not() {
        let set = build_excludes(&[]).unwrap();
        for path in [
            ".DS_Store",
            "Users/alice/.DS_Store",
            ".Spotlight-V100",
            "Users/alice/.Spotlight-V100",
            ".fseventsd",
            ".DocumentRevisions-V100",
            ".TemporaryItems",
            ".Trashes",
            ".Trash",
            ".noindex",
            ".com.apple.timemachine.donotpresent",
        ] {
            assert!(set.is_match(path), "expected {path} to be excluded");
        }
        for path in [
            "Users/alice/Library/Caches",
            "System/Library/Caches",
            "Users/alice/.cache",
            "Users/alice/Library/Logs",
            "private/var/folders",
            "private/var/tmp",
            "private/tmp",
        ] {
            assert!(!set.is_match(path), "expected {path} to be backed up");
        }
        assert!(!set.is_match("Users/alice/Documents/notes.txt"));
    }

    #[test]
    fn duplicate_patterns_are_deduplicated() {
        // Users may also add a metadata pattern manually; `build_excludes`
        // must not fail or duplicate it.
        let metadata = crate::core::model::ALWAYS_IGNORED_METADATA;
        let mut user: Vec<String> = metadata.iter().map(|p| (*p).to_string()).collect();
        user.push("**/*.tmp".to_string());
        let set = build_excludes(&user).unwrap();
        assert!(set.is_match("a/b.tmp"));
        assert!(set.is_match(".DS_Store"));
        assert!(!set.is_match("Users/alice/Library/Caches"));
    }

    #[test]
    fn partial_tree_is_published_during_backup_and_cleared_on_success() {
        let src = std::env::temp_dir().join(format!("partial-src-{}", uuid::Uuid::new_v4()));
        let store_dir =
            std::env::temp_dir().join(format!("partial-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("a.txt"), b"partial file").unwrap();
        std::fs::write(src.join("nested/b.txt"), b"nested file").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "b".into();
        cfg.region = "r".into();
        cfg.access_key_id = "a".into();
        cfg.secret_access_key = "s".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;

        let partial = PartialTree::new();
        let mut saw_partial_files = false;
        let summary = run_backup_with_partial(
            &mut cfg,
            &store,
            CancelToken::new(),
            &mut |_| {
                if let Some(view) = partial.dir_view("") {
                    if let Some(children) = view.children {
                        if children.iter().any(|(name, _)| name == "a.txt") {
                            saw_partial_files = true;
                        }
                    }
                }
            },
            Some(partial.clone()),
        )
        .unwrap();

        assert!(summary.files >= 2);
        assert!(saw_partial_files, "live tree should expose completed files");
        assert!(
            !partial.is_available(),
            "a completed record supersedes the live tree"
        );
        assert!(
            !record::partial_run_exists(&store, &cfg.key_root()).unwrap(),
            "a successful record removes the partial-run marker"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
    }

    #[test]
    fn successful_run_cleans_orphans_left_by_an_interrupted_run() {
        let src = std::env::temp_dir().join(format!("orphan-run-src-{}", uuid::Uuid::new_v4()));
        let store_dir =
            std::env::temp_dir().join(format!("orphan-run-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("keep.txt"), b"keep this data").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "b".into();
        cfg.region = "r".into();
        cfg.access_key_id = "a".into();
        cfg.secret_access_key = "s".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;

        let root = cfg.key_root();
        store
            .put(
                &record::partial_run_key(&root),
                b"previous run was interrupted",
            )
            .unwrap();
        let orphan = format!(
            "{}/blobpacks/orphan-{}.blobpack",
            root,
            uuid::Uuid::new_v4()
        );
        store.put(&orphan, b"orphan").unwrap();

        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        assert!(
            store.head(&orphan).unwrap().is_none(),
            "a successful run after an interruption should remove untracked objects"
        );
        assert!(
            !record::partial_run_exists(&store, &root).unwrap(),
            "the completed run should remove its marker"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
    }

    #[test]
    fn cancelled_backup_keeps_partial_tree_for_browsing() {
        let src = std::env::temp_dir().join(format!("partial-cancel-src-{}", uuid::Uuid::new_v4()));
        let store_dir =
            std::env::temp_dir().join(format!("partial-cancel-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("a.txt"), b"survives cancel").unwrap();
        std::fs::write(src.join("nested/b.txt"), b"nested survives").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = AppConfig::default();
        cfg.bucket = "b".into();
        cfg.region = "r".into();
        cfg.access_key_id = "a".into();
        cfg.secret_access_key = "s".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;

        let partial = PartialTree::new();
        let cancel = CancelToken::new();
        let cancel_from_progress = cancel.clone();
        let result = run_backup_with_partial(
            &mut cfg,
            &store,
            cancel,
            &mut |_| {
                let has_file = partial
                    .dir_view("")
                    .and_then(|view| view.children)
                    .is_some_and(|children| children.iter().any(|(name, _)| name == "a.txt"));
                if has_file {
                    cancel_from_progress.cancel();
                }
            },
            Some(partial.clone()),
        );
        assert!(matches!(result, Err(BackupError::Cancelled)));
        assert!(partial.is_available(), "cancel must keep the live tree");

        assert!(
            record::partial_run_exists(&store, &cfg.key_root()).unwrap(),
            "a canceled run keeps the partial-run marker"
        );
        let listing =
            crate::core::restore::list_partial_entries(&cfg, &store, &partial, "").unwrap();
        assert!(listing.entries.iter().any(|entry| entry.name == "a.txt"));

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&store_dir).ok();
    }
}

#[cfg(test)]
mod more_tests {
    use super::*;
    use crate::core::restore::restore_latest;
    use crate::core::store::{BlobStore, LocalStore, ObjectInfo};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// Local store that rejects every blobpack upload, used to verify that a
    /// failed asynchronous upload aborts the run and never leaves the index
    /// pointing at an object that was not written.
    struct FailingPackStore {
        inner: LocalStore,
    }

    impl BlobStore for FailingPackStore {
        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            if key.ends_with(".blobpack") {
                return Err(BackupError::S3("simulated blobpack failure".into()));
            }
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get(key)
        }

        fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_opt(key)
        }

        fn head(&self, key: &str) -> Result<Option<ObjectInfo>> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
            self.inner.list(prefix)
        }

        fn describe(&self) -> String {
            self.inner.describe()
        }
    }

    /// Fault-injecting store used by the fail-tolerance tests. Modes are kept
    /// deliberately simple so each test exercises one network/disk failure.
    #[derive(Clone, Copy)]
    enum FaultMode {
        FailNthPack(usize),
        LoseFirstPackResponse,
        FailRecords,
        FailJournals,
    }

    struct FaultStore {
        inner: LocalStore,
        mode: FaultMode,
        pack_writes: AtomicUsize,
        lost_pack_response: AtomicBool,
    }

    impl FaultStore {
        fn new(inner: LocalStore, mode: FaultMode) -> Self {
            Self {
                inner,
                mode,
                pack_writes: AtomicUsize::new(0),
                lost_pack_response: AtomicBool::new(true),
            }
        }
    }

    impl BlobStore for FaultStore {
        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            match self.mode {
                FaultMode::FailNthPack(n) if key.ends_with(".blobpack") => {
                    let written = self.pack_writes.fetch_add(1, Ordering::SeqCst) + 1;
                    if written == n {
                        return Err(BackupError::S3(format!(
                            "simulated network break while uploading pack {n}"
                        )));
                    }
                }
                FaultMode::LoseFirstPackResponse if key.ends_with(".blobpack") => {
                    if self.lost_pack_response.swap(false, Ordering::SeqCst) {
                        // Simulate S3 committing the object but the response
                        // being lost on the way back.
                        self.inner.put(key, data)?;
                        return Err(BackupError::S3("simulated lost upload response".into()));
                    }
                }
                FaultMode::FailRecords if key.ends_with(".backuprecord") => {
                    return Err(BackupError::S3("simulated record write failure".into()));
                }
                FaultMode::FailJournals if key.contains("indexjournal") => {
                    return Err(BackupError::S3("simulated journal write failure".into()));
                }
                _ => {}
            }
            self.inner.put(key, data)
        }

        fn get(&self, key: &str) -> Result<Vec<u8>> {
            self.inner.get(key)
        }

        fn get_opt(&self, key: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get_opt(key)
        }

        fn head(&self, key: &str) -> Result<Option<ObjectInfo>> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
            self.inner.list(prefix)
        }

        fn describe(&self) -> String {
            self.inner.describe()
        }
    }

    fn test_config(src: &Path, encrypted: bool) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.bucket = "test-bucket".into();
        cfg.region = "us-east-1".into();
        cfg.access_key_id = "test".into();
        cfg.secret_access_key = "secret".into();
        cfg.source_kind = "path".into();
        cfg.source_path = src.to_string_lossy().to_string();
        cfg.schedule_minutes = 0;
        cfg.encryption_enabled = encrypted;
        cfg.encryption_password = if encrypted {
            "correct horse".into()
        } else {
            String::new()
        };
        cfg
    }

    #[test]
    fn failed_pack_upload_aborts_without_record() {
        let src = std::env::temp_dir().join(format!("fail-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("fail-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("small.txt"), b"packed data").unwrap();
        std::fs::write(src.join("large.bin"), vec![3u8; 400_000]).unwrap();

        let store = FailingPackStore {
            inner: LocalStore::new(&dir).unwrap(),
        };
        let mut cfg = test_config(&src, false);
        let err = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap_err();
        assert!(matches!(err, BackupError::Store(_)), "got {err:?}");

        assert!(
            record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .is_empty(),
            "a failed upload must not commit a backup record"
        );
        let index = BlobIndex::load(&store, &cfg.key_root(), &cfg).unwrap();
        index.check_consistency().unwrap();

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn network_break_preserves_confirmed_uploads_and_resumes() {
        let src = std::env::temp_dir().join(format!("break-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("break-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("break-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        let mut state = 0x1234_5678u32;
        let data: Vec<u8> = (0..8_000_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                (state & 0xff) as u8
            })
            .collect();
        std::fs::write(src.join("data.bin"), &data).unwrap();

        let store = FaultStore::new(LocalStore::new(&dir).unwrap(), FaultMode::FailNthPack(2));
        let mut cfg = test_config(&src, false);
        cfg.pack_size_bytes = 1024 * 1024;
        let err = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap_err();
        assert!(
            matches!(err, BackupError::Store(_) | BackupError::S3(_)),
            "got {err:?}"
        );
        assert!(
            record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .is_empty(),
            "a failed run must not commit a record"
        );

        let root = cfg.key_root();
        assert!(
            !store
                .list(&BlobIndex::journal_prefix(&root))
                .unwrap()
                .is_empty(),
            "confirmed uploads must be journaled before aborting"
        );
        let index = BlobIndex::load(&store, &root, &cfg).unwrap();
        index.check_consistency().unwrap();
        assert!(!index.blobs.is_empty());

        // The next run sees the same directory and must reuse the journaled
        // pack instead of starting over.
        let clean = LocalStore::new(&dir).unwrap();
        let summary = run_backup(&mut cfg, &clean, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(
            summary.blobs_reused > 0,
            "next run should reuse uploads confirmed before the network break"
        );
        restore_latest(&cfg, &clean, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(std::fs::read(dst.join("data.bin")).unwrap(), data);

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn lost_upload_response_is_treated_as_success() {
        let src = std::env::temp_dir().join(format!("lost-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("lost-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("lost-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), b"hello fail tolerance").unwrap();

        let store = FaultStore::new(
            LocalStore::new(&dir).unwrap(),
            FaultMode::LoseFirstPackResponse,
        );
        let mut cfg = test_config(&src, false);
        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(
            !store.lost_pack_response.load(Ordering::SeqCst),
            "the test store should have injected a lost response"
        );
        assert!(summary.bytes_uploaded > 0);
        assert_eq!(
            record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            1
        );
        restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            std::fs::read(dst.join("file.txt")).unwrap(),
            b"hello fail tolerance"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn record_write_failure_keeps_index_for_resume() {
        let src = std::env::temp_dir().join(format!("record-fail-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("record-fail-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("record-fail-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), b"record write can fail").unwrap();

        let failing = FaultStore::new(LocalStore::new(&dir).unwrap(), FaultMode::FailRecords);
        let mut cfg = test_config(&src, false);
        let err = run_backup(&mut cfg, &failing, CancelToken::new(), &mut |_| {}).unwrap_err();
        assert!(matches!(err, BackupError::S3(_)), "got {err:?}");
        assert!(
            record::list_record_keys(&failing, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .is_empty()
        );
        let root = cfg.key_root();
        let index = BlobIndex::load(&failing, &root, &cfg).unwrap();
        index.check_consistency().unwrap();
        assert!(!index.blobs.is_empty());

        // The index was saved before the record write, so the retry should
        // upload no blob data again and only commit the record.
        let clean = LocalStore::new(&dir).unwrap();
        let summary = run_backup(&mut cfg, &clean, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            summary.blobs_new, 0,
            "all blobs should be reused after the record write failure"
        );
        assert_eq!(
            record::list_record_keys(&clean, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            1
        );
        restore_latest(&cfg, &clean, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            std::fs::read(dst.join("file.txt")).unwrap(),
            b"record write can fail"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn partial_tree_restores_after_record_write_failure() {
        let src = std::env::temp_dir().join(format!("partial-rw-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("partial-rw-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("partial-rw-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(
            src.join("file.txt"),
            b"restore from partial after record failure",
        )
        .unwrap();

        let partial = crate::core::partial::PartialTree::new();
        let failing = FaultStore::new(LocalStore::new(&dir).unwrap(), FaultMode::FailRecords);
        let mut cfg = test_config(&src, false);
        let err = run_backup_with_partial(
            &mut cfg,
            &failing,
            CancelToken::new(),
            &mut |_| {},
            Some(partial.clone()),
        )
        .unwrap_err();
        assert!(matches!(err, BackupError::S3(_)), "got {err:?}");
        assert!(
            partial.is_available(),
            "the live tree must survive a failed record write"
        );

        let clean = LocalStore::new(&dir).unwrap();
        let listing =
            crate::core::restore::list_partial_entries(&cfg, &clean, &partial, "").unwrap();
        assert!(listing.entries.iter().any(|entry| entry.name == "file.txt"));

        crate::core::restore::restore_partial_entry(
            &cfg,
            &clean,
            &partial,
            "file.txt",
            &dst,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(
            std::fs::read(dst.join("file.txt")).unwrap(),
            b"restore from partial after record failure"
        );

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn journal_upload_failure_falls_back_to_local_progress() {
        let src = std::env::temp_dir().join(format!("journal-fail-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("journal-fail-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("journal-fail-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        let mut state = 0x0BAD_F00Du32;
        let first_data: Vec<u8> = (0..6_000_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                (state & 0xff) as u8
            })
            .collect();
        std::fs::write(src.join("file.bin"), &first_data).unwrap();

        let mut cfg = test_config(&src, false);
        cfg.pack_size_bytes = 1024 * 1024;
        let clean = LocalStore::new(&dir).unwrap();
        run_backup(&mut cfg, &clean, CancelToken::new(), &mut |_| {}).unwrap();
        let root = cfg.key_root();
        assert_eq!(
            record::list_record_keys(&clean, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            1
        );

        // A later run changes the file, uploads the new blobs successfully,
        // but cannot upload its checkpoint journal. The local cache must keep
        // those confirmed uploads so the next run can reuse them.
        let mut state = 0xC0FF_EE00u32;
        let new_data: Vec<u8> = (0..6_000_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                (state & 0xff) as u8
            })
            .collect();
        std::fs::write(src.join("file.bin"), &new_data).unwrap();
        let faulty = FaultStore::new(LocalStore::new(&dir).unwrap(), FaultMode::FailJournals);
        let err = run_backup(&mut cfg, &faulty, CancelToken::new(), &mut |_| {}).unwrap_err();
        assert!(
            matches!(err, BackupError::Store(_) | BackupError::S3(_)),
            "got {err:?}"
        );
        assert_eq!(
            record::list_record_keys(&faulty, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            1
        );

        let recovered = LocalStore::new(&dir).unwrap();
        let summary = run_backup(&mut cfg, &recovered, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(
            summary.blobs_reused > 0,
            "the local cache should preserve uploads confirmed before the journal failure"
        );
        assert_eq!(
            record::list_record_keys(&recovered, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            2
        );
        restore_latest(&cfg, &recovered, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(std::fs::read(dst.join("file.bin")).unwrap(), new_data);

        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn missing_index_is_rebuilt_from_existing_records() {
        let src = std::env::temp_dir().join(format!("no-index-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("no-index-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("no-index-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("nested/file.bin"), vec![4u8; 600_000]).unwrap();

        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        cfg.pack_size_bytes = 1024 * 1024;
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let root = cfg.key_root();
        assert_eq!(
            record::list_record_keys(&store, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            1
        );

        // Simulate a bucket with valid records and data but no index (older
        // version, cleanup, or lost index object).
        store.delete(&BlobIndex::remote_key(&root)).unwrap();
        std::fs::remove_file(BlobIndex::local_path(&cfg)).ok();
        for object in store.list(&BlobIndex::journal_prefix(&root)).unwrap() {
            store.delete(&object.key).unwrap();
        }

        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            summary.blobs_new, 0,
            "a rebuilt index should let the next run reuse every existing blob"
        );
        assert_eq!(
            record::list_record_keys(&store, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            2
        );
        let index = BlobIndex::load(&store, &root, &cfg).unwrap();
        index.check_consistency().unwrap();
        restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            std::fs::read(dst.join("nested/file.bin")).unwrap(),
            vec![4u8; 600_000]
        );

        std::fs::remove_file(BlobIndex::local_path(&cfg)).ok();
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn corrupt_index_is_rebuilt_from_existing_records() {
        let src = std::env::temp_dir().join(format!("bad-index-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("bad-index-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("bad-index-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), b"existing set recovery").unwrap();

        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let root = cfg.key_root();
        store
            .put(&BlobIndex::remote_key(&root), b"corrupt blob index")
            .unwrap();
        std::fs::remove_file(BlobIndex::local_path(&cfg)).ok();

        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(summary.blobs_new, 0);
        assert_eq!(
            record::list_record_keys(&store, &root, &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            2
        );
        let index = BlobIndex::load(&store, &root, &cfg).unwrap();
        index.check_consistency().unwrap();
        restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            std::fs::read(dst.join("file.txt")).unwrap(),
            b"existing set recovery"
        );

        std::fs::remove_file(BlobIndex::local_path(&cfg)).ok();
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn encrypted_backup_restore() {
        let src = std::env::temp_dir().join(format!("enc-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("enc-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("enc-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("d")).unwrap();
        std::fs::write(src.join("d/file.txt"), b"secret contents").unwrap();
        std::fs::write(src.join("plain.txt"), b"hello").unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, true);
        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(summary.bytes_uploaded > 0);
        assert!(
            store
                .get_opt(&format!("{}/encryptedkeyset.dat", cfg.key_root()))
                .unwrap()
                .is_some()
        );
        let rec = restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(rec.files, 2);
        assert_eq!(
            std::fs::read(dst.join("d/file.txt")).unwrap(),
            b"secret contents"
        );
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[test]
    fn incremental_reuses_blobs_and_restores() {
        let src = std::env::temp_dir().join(format!("inc-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("inc-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("inc-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("one.bin"), vec![9u8; 300_000]).unwrap();
        std::fs::write(src.join("two.bin"), b"two").unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        std::fs::write(src.join("two.bin"), b"two changed").unwrap();
        let mut second_events = Vec::new();
        let second = run_backup(&mut cfg, &store, CancelToken::new(), &mut |e| {
            second_events.push(e)
        })
        .unwrap();
        assert!(second.blobs_reused > 0, "expected dedupe on unchanged file");
        let totals = second_events.iter().find_map(|e| match e {
            ProgressEvent::Totals { files, bytes } => Some((*files, *bytes)),
            _ => None,
        });
        assert_eq!(
            totals.map(|(files, _)| files),
            Some(2),
            "second run should use the previous record's file count"
        );
        assert!(
            totals.map(|(_, bytes)| bytes).unwrap_or(0) > 0,
            "second run should use the previous record's byte count"
        );
        assert_eq!(
            crate::core::record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .len(),
            2
        );
        // Reused pack members must still be fully recorded by the index even
        // though the walk no longer keeps a per-record manifest.
        let index = BlobIndex::load(&store, &cfg.key_root(), &cfg).unwrap();
        index.check_consistency().unwrap();
        assert!(!index.packs.is_empty());
        restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(std::fs::read(dst.join("two.bin")).unwrap(), b"two changed");
        assert_eq!(
            std::fs::read(dst.join("one.bin")).unwrap(),
            vec![9u8; 300_000]
        );
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn snapshot_failure_does_not_back_up_live_files() {
        let src = std::env::temp_dir().join(format!("nosnap-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("nosnap-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("file.txt"), b"no external drive").unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        cfg.source_kind = "snapshot".into();
        cfg.source_path.clear();
        cfg.snapshot_volume = src.to_string_lossy().to_string();
        cfg.included_folders = vec!["Users".into()];
        let err = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap_err();
        assert!(matches!(err, BackupError::Snapshot(_)));
        assert!(
            record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .is_empty()
        );
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn interrupted_backup_resumes_from_checkpoint() {
        let src = std::env::temp_dir().join(format!("resume-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("resume-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("big.bin"), vec![5u8; 700_000]).unwrap();
        std::fs::write(src.join("small.txt"), b"resume me").unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        // Simulate a crash after packs/index were uploaded but before the
        // record was committed.
        for key in
            crate::core::record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
        {
            store.delete(&key).unwrap();
        }
        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(
            summary.blobs_reused > 0,
            "checkpointed index should avoid re-uploading blobs"
        );
        assert_eq!(
            summary.blobs_new, 0,
            "all data should come from the checkpoint"
        );
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn journal_only_resume_reuses_uploaded_blobs() {
        let src = std::env::temp_dir().join(format!("journal-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("journal-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("one.bin"), vec![9u8; 900_000]).unwrap();
        std::fs::write(src.join("sub/two.txt"), b"journal resume").unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        let root = cfg.key_root();

        // Simulate a crash that uploaded journal segments but never wrote the
        // base index or a record: keep only journals.
        let index = BlobIndex::load(&store, &root, &cfg).unwrap();
        BlobIndex::save_journal(&store, &root, &index.blobs, &index.packs).unwrap();
        store.delete(&BlobIndex::remote_key(&root)).unwrap();
        std::fs::remove_file(BlobIndex::local_path(&cfg)).ok();
        for key in
            crate::core::record::list_record_keys(&store, &root, &cfg.backup_set_uuid).unwrap()
        {
            store.delete(&key).unwrap();
        }

        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(
            summary.blobs_new, 0,
            "all blobs should be recovered from journal segments"
        );
        assert!(summary.blobs_reused > 0);
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn large_data_is_packed_and_journals_are_cleared() {
        let src = std::env::temp_dir().join(format!("pack-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("pack-store-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        // Incompressible-ish data: each 1 MiB chunk must exceed the old
        // 256 KB standalone threshold, so this exercises the new packing.
        let data: Vec<u8> = (0..8_000_000u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
            .collect();
        std::fs::write(src.join("big.bin"), &data).unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert!(summary.bytes_uploaded > 0);
        let root = cfg.key_root();
        let standalone = store
            .list(&format!("{root}/standardobjects/"))
            .unwrap()
            .len();
        let packs = store.list(&format!("{root}/blobpacks/")).unwrap().len();
        assert_eq!(
            standalone, 0,
            "large data should not become standalone objects"
        );
        assert!(
            store
                .list(&format!("{root}/treepacks/"))
                .unwrap()
                .is_empty(),
            "trees should be packed instead of uploaded one per directory"
        );
        assert!(packs >= 1, "large data should be in a blobpack");

        let record_key = record::latest_record_key(&store, &root, &cfg.backup_set_uuid)
            .unwrap()
            .expect("backup record");
        let record = record::load_record(&store, &record_key, None).unwrap();
        let root_node = record.root_node().unwrap();
        let tree_loc = root_node.tree_blob_loc.expect("root tree location");
        assert!(
            tree_loc.is_packed,
            "root tree should be stored in a blobpack"
        );
        let tree = record::load_tree(&store, &root, &tree_loc, None).unwrap();
        assert!(!tree.children.is_empty());
        let journals = store.list(&BlobIndex::journal_prefix(&root)).unwrap().len();
        assert_eq!(
            journals, 0,
            "successful backup must compact journal segments"
        );
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn symlink_roundtrip() {
        let src = std::env::temp_dir().join(format!("link-src-{}", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("link-store-{}", uuid::Uuid::new_v4()));
        let dst = std::env::temp_dir().join(format!("link-dst-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("target.txt"), b"target").unwrap();
        std::os::unix::fs::symlink("target.txt", src.join("link")).unwrap();
        let store = LocalStore::new(&dir).unwrap();
        let mut cfg = test_config(&src, false);
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        restore_latest(&cfg, &store, &dst, CancelToken::new(), &mut |_| {}).unwrap();
        let link = std::fs::read_link(dst.join("link")).unwrap();
        assert_eq!(link, Path::new("target.txt"));
        std::fs::remove_dir_all(&src).ok();
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dst).ok();
    }
}

#[cfg(test)]
mod include_tests {
    use super::*;
    use crate::core::store::LocalStore;

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

    fn cleanup(paths: &[&std::path::PathBuf]) {
        for path in paths {
            std::fs::remove_dir_all(path).ok();
        }
    }

    #[test]
    fn included_folders_limit_scan_and_restore() {
        let src = temp("include-src");
        let store_dir = temp("include-store");
        let dst = temp("include-dst");
        std::fs::create_dir_all(src.join("keep/sub")).unwrap();
        std::fs::create_dir_all(src.join("other")).unwrap();
        std::fs::write(src.join("root.txt"), b"root").unwrap();
        std::fs::write(src.join("keep/a.txt"), b"a").unwrap();
        std::fs::write(src.join("keep/sub/b.txt"), b"b").unwrap();
        std::fs::write(src.join("other/c.txt"), b"c").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = config(&src);
        cfg.included_folders = vec!["keep".into()];
        let summary = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();
        assert_eq!(summary.files, 2);

        let root = crate::core::restore::list_entries(&cfg, &store, None, "").unwrap();
        let names: Vec<&str> = root.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["keep"]);
        let keep = crate::core::restore::list_entries(&cfg, &store, None, "keep").unwrap();
        let keep_names: Vec<&str> = keep.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(keep_names, vec!["sub", "a.txt"]);

        crate::core::restore::restore_entry(
            &cfg,
            &store,
            None,
            "",
            &dst,
            CancelToken::new(),
            &mut |_| {},
        )
        .unwrap();
        assert!(dst.join("keep/a.txt").exists());
        assert!(dst.join("keep/sub/b.txt").exists());
        assert!(!dst.join("other").exists());
        assert!(!dst.join("root.txt").exists());

        cleanup(&[&src, &store_dir, &dst]);
    }

    #[test]
    fn nested_include_keeps_ancestors_but_not_siblings() {
        let src = temp("nested-include-src");
        let store_dir = temp("nested-include-store");
        std::fs::create_dir_all(src.join("a/sub")).unwrap();
        std::fs::write(src.join("a/sibling.txt"), b"sibling").unwrap();
        std::fs::write(src.join("a/sub/want.txt"), b"want").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = config(&src);
        cfg.included_folders = vec!["a/sub".into()];
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        let a = crate::core::restore::list_entries(&cfg, &store, None, "a").unwrap();
        let names: Vec<&str> = a.entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["sub"]);
        let sub = crate::core::restore::list_entries(&cfg, &store, None, "a/sub").unwrap();
        assert!(sub.entries.iter().any(|e| e.name == "want.txt"));

        cleanup(&[&src, &store_dir]);
    }

    #[test]
    fn changing_includes_resets_progress_totals() {
        let src = temp("include-progress-src");
        let store_dir = temp("include-progress-store");
        std::fs::create_dir_all(src.join("one")).unwrap();
        std::fs::create_dir_all(src.join("two")).unwrap();
        std::fs::write(src.join("one/a.txt"), b"a").unwrap();
        std::fs::write(src.join("one/b.txt"), b"b").unwrap();
        std::fs::write(src.join("two/c.txt"), b"c").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = config(&src);
        cfg.included_folders = vec!["one".into()];
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        cfg.included_folders = vec!["two".into()];
        let mut events = Vec::new();
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |event| {
            events.push(event)
        })
        .unwrap();
        let totals = events.iter().find_map(|event| match event {
            ProgressEvent::Totals { files, .. } => Some(*files),
            _ => None,
        });
        assert_eq!(
            totals,
            Some(1),
            "changed includes must recount instead of reusing old totals"
        );

        cleanup(&[&src, &store_dir]);
    }

    #[test]
    fn missing_included_folder_fails_before_writing_a_record() {
        let src = temp("include-missing-src");
        let store_dir = temp("include-missing-store");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("a.txt"), b"a").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = config(&src);
        cfg.included_folders = vec!["does-not-exist".into()];
        let err = run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap_err();
        assert!(err.to_string().contains("included folder"));
        assert!(
            record::list_record_keys(&store, &cfg.key_root(), &cfg.backup_set_uuid)
                .unwrap()
                .is_empty()
        );

        cleanup(&[&src, &store_dir]);
    }

    #[test]
    fn missing_optional_included_folder_is_skipped() {
        let src = temp("include-optional-src");
        std::fs::create_dir_all(src.join("keep")).unwrap();
        std::fs::write(src.join("keep/file.txt"), b"keep").unwrap();

        let mut cfg = config(&src);
        cfg.included_folders = vec!["keep".into(), "does-not-exist".into()];
        validate_included_folders(&cfg, &src).unwrap();

        cleanup(&[&src]);
    }

    #[test]
    fn metadata_is_ignored_inside_included_folders_but_caches_are_kept() {
        let src = temp("include-meta-src");
        let store_dir = temp("include-meta-store");
        std::fs::create_dir_all(src.join("keep/Library/Caches")).unwrap();
        std::fs::write(src.join("keep/.DS_Store"), b"meta").unwrap();
        std::fs::write(src.join("keep/Library/Caches/cache.bin"), b"cache").unwrap();
        std::fs::write(src.join("keep/file.txt"), b"file").unwrap();

        let store = LocalStore::new(&store_dir).unwrap();
        let mut cfg = config(&src);
        cfg.included_folders = vec!["keep".into()];
        run_backup(&mut cfg, &store, CancelToken::new(), &mut |_| {}).unwrap();

        let keep = crate::core::restore::list_entries(&cfg, &store, None, "keep").unwrap();
        let names: Vec<&str> = keep.entries.iter().map(|e| e.name.as_str()).collect();
        assert!(!names.contains(&".DS_Store"));
        assert!(names.contains(&"file.txt"));
        assert!(
            names.contains(&"Library"),
            "caches are no longer excluded by default"
        );

        cleanup(&[&src, &store_dir]);
    }

    #[test]
    fn snapshot_backup_requires_an_included_folder() {
        let mut cfg = AppConfig::default();
        cfg.source_kind = "snapshot".into();
        let err = crate::core::config::validate_backup_selection(&cfg).unwrap_err();
        assert!(
            err.to_string()
                .contains("select at least one included folder")
        );
        cfg.included_folders = vec!["Users".into()];
        assert!(crate::core::config::validate_backup_selection(&cfg).is_ok());
    }
}
