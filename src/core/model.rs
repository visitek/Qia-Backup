//! Qia Backup data model: backup-set metadata, nodes, trees and records.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const BACKUP_FORMAT_VERSION: u32 = 101;
pub const NODE_TREE_VERSION: u32 = 4;
pub const BLOB_IDENTIFIER_SHA1: u32 = 1;
pub const BLOB_IDENTIFIER_SHA256: u32 = 2;
pub const COMPRESSION_NONE: u32 = 0;
pub const COMPRESSION_GZIP: u32 = 1;
pub const COMPRESSION_LZ4: u32 = 2;

/// A location of a blob (or a tree) in the backup set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobLoc {
    /// Lowercase hexadecimal blob identifier (SHA-256 for new data).
    pub blob_identifier: String,
    pub is_packed: bool,
    /// Path relative to the backup-set root, e.g. `blobpacks/abc.blobpack`.
    pub relative_path: String,
    pub offset: u64,
    pub length: u64,
    pub stretch_encryption_key: bool,
    pub compression_type: u32,
}

impl BlobLoc {
    pub fn unpacked(blob_identifier: String, relative_path: String, length: u64) -> Self {
        Self {
            blob_identifier,
            is_packed: false,
            relative_path,
            offset: 0,
            length,
            stretch_encryption_key: false,
            compression_type: COMPRESSION_LZ4,
        }
    }

    pub fn is_encrypted(&self) -> bool {
        // Encrypted objects are marked with the stretched
        // (object) key. We keep the flag explicit so records remain portable.
        self.stretch_encryption_key
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub is_tree: bool,
    pub tree_blob_loc: Option<BlobLoc>,
    pub computer_os_type: u32,
    pub data_blob_locs: Vec<BlobLoc>,
    pub acl_blob_loc: Option<BlobLoc>,
    pub xattrs_blob_locs: Vec<BlobLoc>,
    pub item_size: u64,
    pub contained_files_count: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub ctime_sec: i64,
    pub ctime_nsec: i64,
    pub create_time_sec: i64,
    pub create_time_nsec: i64,
    pub username: Option<String>,
    pub group_name: Option<String>,
    pub deleted: bool,
    pub mac_st_dev: i32,
    pub mac_st_ino: u64,
    pub mac_st_mode: u32,
    pub mac_st_nlink: u32,
    pub mac_st_uid: u32,
    pub mac_st_gid: u32,
    pub mac_st_rdev: i32,
    pub mac_st_flags: i32,
    pub win_attrs: u32,
    pub win_reparse_tag: u32,
    pub win_reparse_point_is_directory: bool,
    pub added_time_sec: i64,
    pub added_time_nsec: i64,
    pub document_id: u32,
    pub has_document_id: bool,
    pub is_sparse: bool,
    pub sparse_logical_size: u64,
    pub holes: Vec<(u64, u64)>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            is_tree: false,
            tree_blob_loc: None,
            computer_os_type: 1,
            data_blob_locs: Vec::new(),
            acl_blob_loc: None,
            xattrs_blob_locs: Vec::new(),
            item_size: 0,
            contained_files_count: 0,
            mtime_sec: 0,
            mtime_nsec: 0,
            ctime_sec: 0,
            ctime_nsec: 0,
            create_time_sec: 0,
            create_time_nsec: 0,
            username: None,
            group_name: None,
            deleted: false,
            mac_st_dev: 0,
            mac_st_ino: 0,
            mac_st_mode: 0,
            mac_st_nlink: 1,
            mac_st_uid: 0,
            mac_st_gid: 0,
            mac_st_rdev: 0,
            mac_st_flags: 0,
            win_attrs: 0,
            win_reparse_tag: 0,
            win_reparse_point_is_directory: false,
            added_time_sec: 0,
            added_time_nsec: 0,
            document_id: 0,
            has_document_id: false,
            is_sparse: false,
            sparse_logical_size: 0,
            holes: Vec::new(),
        }
    }
}

impl Node {
    pub fn is_directory(&self) -> bool {
        self.is_tree
    }

    pub fn mode_is_dir(mode: u32) -> bool {
        (mode & libc::S_IFMT as u32) == libc::S_IFDIR as u32
    }

    pub fn mode_is_symlink(mode: u32) -> bool {
        (mode & libc::S_IFMT as u32) == libc::S_IFLNK as u32
    }
}

/// A directory tree. Stored LZ4-compressed as a content-addressed blob.
#[derive(Debug, Clone, Default)]
pub struct Tree {
    pub version: u32,
    pub children: Vec<(String, Node)>,
}

impl Tree {
    pub fn new() -> Self {
        Self {
            version: NODE_TREE_VERSION,
            children: Vec::new(),
        }
    }

    pub fn insert(&mut self, name: String, node: Node) {
        self.children.push((name, node));
    }

    pub fn sort(&mut self) {
        self.children.sort_by(|a, b| a.0.cmp(&b.0));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfig {
    pub blob_identifier_type: u32,
    pub max_packed_item_length: u64,
    pub backup_name: String,
    #[serde(default)]
    pub is_worm: bool,
    #[serde(default)]
    pub contains_glacier_archives: bool,
    #[serde(default)]
    pub additional_unpacked_blob_dirs: Vec<String>,
    pub chunker_version: u32,
    pub computer_name: String,
    pub computer_serial: String,
    pub blob_storage_class: String,
    pub is_encrypted: bool,
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slfs_type: Option<String>,
}

impl BackupConfig {
    pub fn new(backup_name: String, computer_name: String, computer_serial: String) -> Self {
        Self {
            blob_identifier_type: BLOB_IDENTIFIER_SHA256,
            max_packed_item_length: 256_000,
            backup_name,
            is_worm: false,
            contains_glacier_archives: false,
            additional_unpacked_blob_dirs: Vec::new(),
            chunker_version: 3,
            computer_name,
            computer_serial,
            blob_storage_class: "STANDARD".to_string(),
            is_encrypted: false,
            version: BACKUP_FORMAT_VERSION,
            slfs_type: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupPlan {
    pub plan_uuid: String,
    pub name: String,
    pub storage_location_id: String,
    pub plan_version: u32,
    pub use_apfs_snapshots: bool,
    pub use_buzhash: bool,
    pub retain_all: bool,
    pub retain_days: u32,
    pub retain_hours: u32,
    pub retain_months: u32,
    pub retain_weeks: u32,
    pub schedule_json: String,
    pub thread_count: u32,
    pub cpu_usage: u32,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub source_path: String,
    #[serde(default)]
    pub source_kind: String,
}

fn default_true() -> bool {
    true
}

impl Default for BackupPlan {
    fn default() -> Self {
        Self {
            plan_uuid: uuid::Uuid::new_v4().to_string(),
            name: "Time Machine to S3".to_string(),
            storage_location_id: "s3".to_string(),
            plan_version: 1,
            use_apfs_snapshots: true,
            use_buzhash: true,
            retain_all: false,
            retain_days: 30,
            retain_hours: 24,
            retain_months: 12,
            retain_weeks: 8,
            schedule_json: "{\"type\":\"Daily\",\"hour\":2,\"minute\":0}".to_string(),
            thread_count: DEFAULT_THREAD_COUNT,
            cpu_usage: 100,
            enabled: true,
            source_path: String::new(),
            source_kind: "snapshot".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupFolders {
    #[serde(default)]
    pub standard_object_dirs: Vec<String>,
    #[serde(default)]
    pub standard_ia_object_dirs: Vec<String>,
    #[serde(default)]
    pub onezone_ia_object_dirs: Vec<String>,
    #[serde(default)]
    pub s3_glacier_object_dirs: Vec<String>,
    #[serde(default)]
    pub s3_deep_archive_object_dirs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imported_from: Option<Value>,
}

impl BackupFolders {
    pub fn new(root: &str) -> Self {
        Self {
            standard_object_dirs: vec![format!("/{root}/standardobjects")],
            standard_ia_object_dirs: vec![format!("/{root}/standardiaobjects")],
            onezone_ia_object_dirs: vec![format!("/{root}/onezoneiaobjects")],
            s3_glacier_object_dirs: vec![format!("/{root}/s3glacierobjects")],
            s3_deep_archive_object_dirs: vec![format!("/{root}/s3deeparchiveobjects")],
            imported_from: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupFolder {
    pub local_path: String,
    pub storage_class: String,
    pub disk_identifier: String,
    pub uuid: String,
    #[serde(default)]
    pub local_mount_point: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub migrated_from_s3_bucket: bool,
    #[serde(default)]
    pub migrated_from_s3_prefix: String,
    #[serde(default)]
    pub source_path: String,
    #[serde(default)]
    pub source_kind: String,
}

impl BackupFolder {
    pub fn new(uuid: String, local_path: String) -> Self {
        Self {
            local_path: local_path.clone(),
            storage_class: "STANDARD".to_string(),
            disk_identifier: String::new(),
            uuid,
            local_mount_point: local_path.clone(),
            name: "Time Machine".to_string(),
            migrated_from_s3_bucket: false,
            migrated_from_s3_prefix: String::new(),
            source_path: local_path,
            source_kind: "snapshot".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupRecordError {
    #[serde(default)]
    pub error_message: String,
    #[serde(default)]
    pub local_path: String,
    #[serde(default)]
    pub relative_path: String,
    #[serde(default)]
    pub volume_name: String,
    #[serde(default)]
    pub disk_identifier: String,
    #[serde(default)]
    pub path_is_directory: bool,
    /// 0 unclassified, 1 filtered, 2 retried, 3 file-level (yellow), 4 session (red)
    #[serde(default)]
    pub severity: u32,
    #[serde(default)]
    pub error_domain: String,
    #[serde(default)]
    pub error_code: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupRecord {
    pub backup_plan_json: Value,
    pub backup_folder_uuid: String,
    pub backup_plan_uuid: String,
    pub computer_os_type: u32,
    pub creation_date: DateTime<Utc>,
    #[serde(default)]
    pub disk_identifier: String,
    #[serde(default)]
    pub error_count: u64,
    #[serde(default)]
    pub backup_record_errors: Vec<BackupRecordError>,
    pub is_complete: bool,
    #[serde(default)]
    pub local_mount_point: String,
    #[serde(default)]
    pub local_path: String,
    /// Base64-encoded binary root Node.
    pub node: String,
    pub node_tree_version: u32,
    #[serde(default)]
    pub relative_path: String,
    pub storage_class: String,
    pub version: u32,
    #[serde(default)]
    pub volume_name: String,
    // ---- extensions used by this implementation ----
    #[serde(default)]
    pub source_kind: String,
    #[serde(default)]
    pub snapshot_name: String,
    #[serde(default)]
    pub files_count: u64,
    #[serde(default)]
    pub bytes_uploaded: u64,
    #[serde(default)]
    pub bytes_scanned: u64,
    #[serde(default)]
    pub duration_seconds: f64,
    /// Normalized included folders used for this record. Empty means the whole
    /// selected path (legacy/path source). Used to invalidate progress totals
    /// when the selection changes.
    #[serde(default)]
    pub included_folders: Vec<String>,
}

impl BackupRecord {
    pub fn root_node(&self) -> crate::Result<Node> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(self.node.as_bytes())
            .map_err(|e| crate::BackupError::Codec(format!("record node base64: {e}")))?;
        crate::core::codec::decode_node(&bytes, self.node_tree_version)
    }
}

/// Persistent configuration of Qia Backup (not remote backup-set metadata).
/// backup-set metadata; this lives in the user's Library).
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    pub version: u32,
    pub backup_set_uuid: String,
    pub backup_set_name: String,
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub access_key_id: String,
    #[cfg_attr(target_os = "macos", serde(skip_serializing))]
    #[serde(default)]
    pub secret_access_key: String,
    #[serde(default)]
    pub source_path: String,
    #[serde(default = "default_source_kind")]
    pub source_kind: String,
    #[serde(default = "default_snapshot_volume")]
    pub snapshot_volume: String,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    /// Folders relative to the source root that are backed up. Empty means the
    /// whole selected path for a path source; an APFS snapshot backup requires
    /// at least one entry.
    #[serde(default)]
    pub included_folders: Vec<String>,
    #[serde(default = "default_interval")]
    pub schedule_minutes: u64,
    #[serde(default = "default_retain_hours")]
    pub retain_hours: u32,
    #[serde(default = "default_retain_days")]
    pub retain_days: u32,
    #[serde(default = "default_retain_weeks")]
    pub retain_weeks: u32,
    #[serde(default = "default_retain_months")]
    pub retain_months: u32,
    #[serde(default)]
    pub retain_all: bool,
    #[serde(default)]
    pub encryption_enabled: bool,
    #[cfg_attr(target_os = "macos", serde(skip_serializing))]
    #[serde(default)]
    pub encryption_password: String,
    #[serde(default = "default_pack_size")]
    pub pack_size_bytes: u64,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,
    /// Whether the upload rate limit is active.
    #[serde(default = "default_upload_limit_enabled")]
    pub upload_limit_enabled: bool,
    /// Maximum upload throughput in bytes per second. Defaults to 3 MiB/s.
    #[serde(default = "default_upload_limit_bps")]
    pub upload_limit_bytes_per_sec: u64,
    #[serde(default)]
    pub last_backup_at: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppConfig")
            .field("version", &self.version)
            .field("backup_set_uuid", &self.backup_set_uuid)
            .field("backup_set_name", &self.backup_set_name)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &redacted(&self.secret_access_key))
            .field("source_path", &self.source_path)
            .field("source_kind", &self.source_kind)
            .field("snapshot_volume", &self.snapshot_volume)
            .field("exclude_patterns", &self.exclude_patterns)
            .field("included_folders", &self.included_folders)
            .field("schedule_minutes", &self.schedule_minutes)
            .field("retain_hours", &self.retain_hours)
            .field("retain_days", &self.retain_days)
            .field("retain_weeks", &self.retain_weeks)
            .field("retain_months", &self.retain_months)
            .field("retain_all", &self.retain_all)
            .field("encryption_enabled", &self.encryption_enabled)
            .field("encryption_password", &redacted(&self.encryption_password))
            .field("pack_size_bytes", &self.pack_size_bytes)
            .field("chunk_size", &self.chunk_size)
            .field("upload_limit_enabled", &self.upload_limit_enabled)
            .field(
                "upload_limit_bytes_per_sec",
                &self.upload_limit_bytes_per_sec,
            )
            .field("last_backup_at", &self.last_backup_at)
            .finish()
    }
}

fn redacted(value: &str) -> &'static str {
    if value.is_empty() { "" } else { "<redacted>" }
}

fn default_source_kind() -> String {
    "snapshot".to_string()
}
fn default_snapshot_volume() -> String {
    "/System/Volumes/Data".to_string()
}
fn default_interval() -> u64 {
    60
}
fn default_retain_hours() -> u32 {
    24
}
fn default_retain_days() -> u32 {
    30
}
fn default_retain_weeks() -> u32 {
    8
}
fn default_retain_months() -> u32 {
    12
}
fn default_pack_size() -> u64 {
    64 * 1024 * 1024
}
fn default_chunk_size() -> u64 {
    1024 * 1024
}
fn default_upload_limit_enabled() -> bool {
    true
}
fn default_upload_limit_bps() -> u64 {
    crate::core::throttle::DEFAULT_UPLOAD_LIMIT_BPS
}

/// Recommended folders for a practical Mac data backup. These are the initial
/// selection for a new configuration; users can edit or reset them.
pub const DEFAULT_INCLUDED_FOLDERS: &[&str] = &[
    "Applications",
    "Users",
    "Library",
    "private/etc",
    "opt",
    "usr/local",
];

/// Recommended cache/log exclusions. These keep a first backup from spending
/// days uploading rebuildable data while preserving applications, user data,
/// and machine-wide settings.
pub const DEFAULT_EXCLUDE_PATTERNS: &[&str] = &[
    "**/Library/Caches",
    "**/Library/Logs",
    "**/Library/Saved Application State",
    "**/Library/Application Support/CrashReporter",
    "**/.cache",
    "**/.npm/_cacache",
    "**/.yarn/cache",
    "**/.cargo/registry/cache",
    "**/Library/Developer/Xcode/DerivedData",
    "**/Library/Developer/Xcode/iOS DeviceSupport",
    "**/Library/Developer/Xcode/watchOS DeviceSupport",
    "**/Library/Developer/Xcode/tvOS DeviceSupport",
    "**/Library/Developer/CoreSimulator",
    "**/private/var/folders",
    "**/private/var/tmp",
    "**/private/tmp",
];

/// macOS filesystem metadata that is never useful in a backup. These are
/// hardcoded and applied on top of user patterns, so they cannot be removed by
/// editing the config.
pub const ALWAYS_IGNORED_METADATA: &[&str] = &[
    "**/.DS_Store",
    "**/.Spotlight-V100",
    "**/.fseventsd",
    "**/.DocumentRevisions-V100",
    "**/.TemporaryItems",
    "**/.Trashes",
    "**/.Trash",
    "**/.noindex",
    "**/.com.apple.timemachine.donotpresent",
];

/// The old broad exclusion list. It is kept only so existing configuration
/// files can be migrated by removing these entries.
pub const LEGACY_DEFAULT_EXCLUDE_PATTERNS: &[&str] = &[
    "**/.Spotlight-V100",
    "**/.fseventsd",
    "**/.DocumentRevisions-V100",
    "**/.TemporaryItems",
    "**/.Trashes",
    "**/.Trash",
    "**/.noindex",
    "**/.DS_Store",
    "**/Library/Caches",
    "**/System/Library/Caches",
    "**/Caches",
    "**/.cache",
    "**/Library/Saved Application State",
    "**/Library/Developer/Xcode/DerivedData",
    "**/Library/Developer/Xcode/iOS DeviceSupport",
    "**/Library/Developer/Xcode/watchOS DeviceSupport",
    "**/Library/Developer/Xcode/tvOS DeviceSupport",
    "**/Library/Developer/CoreSimulator",
    "**/.npm/_cacache",
    "**/.yarn/cache",
    "**/.cargo/registry/cache",
    "**/.lmstudio/models",
    "**/Library/Logs",
    "**/Library/Application Support/CrashReporter",
    "**/private/var/folders",
    "**/private/var/tmp",
    "**/private/var/vm",
    "**/private/tmp",
    "**/System/Volumes/VM",
    "**/dev",
    "**/Network",
    "**/cores",
    "**/.vol",
    "**/.MobileBackups",
    "**/.PKInstallSandboxManager",
    "**/.PKInstallSandboxManager-SystemSoftware",
    "**/.com.apple.templatemigration.*",
];

/// Default exclusion patterns for new configurations. The hardcoded metadata
/// set above is applied in addition to these.
pub fn default_exclude_patterns() -> Vec<String> {
    DEFAULT_EXCLUDE_PATTERNS
        .iter()
        .map(|pattern| (*pattern).to_string())
        .collect()
}

/// Default included folders for new configurations.
pub fn default_included_folders() -> Vec<String> {
    DEFAULT_INCLUDED_FOLDERS
        .iter()
        .map(|path| (*path).to_string())
        .collect()
}

impl AppConfig {
    /// Recommended starting configuration for a new backup set.
    ///
    /// A plain `AppConfig::default()` stays empty so path-source/CLI behavior
    /// and tests are explicit; the desktop app and a fresh config file use this
    /// recommended selection.
    pub fn recommended() -> Self {
        let mut config = Self::default();
        config.included_folders = default_included_folders();
        config.exclude_patterns = default_exclude_patterns();
        config
    }
}

/// Default AWS region used when the configuration does not specify one.
pub const DEFAULT_REGION: &str = "eu-west-1";

/// Default number of concurrent uploaders.
pub const DEFAULT_THREAD_COUNT: u32 = 4;

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            version: 1,
            backup_set_uuid: uuid::Uuid::new_v4().to_string(),
            backup_set_name: "Time Machine".to_string(),
            bucket: String::new(),
            region: DEFAULT_REGION.to_string(),
            prefix: "qia-backups".to_string(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            source_path: String::new(),
            source_kind: default_source_kind(),
            snapshot_volume: default_snapshot_volume(),
            exclude_patterns: Vec::new(),
            included_folders: Vec::new(),
            schedule_minutes: default_interval(),
            retain_hours: default_retain_hours(),
            retain_days: default_retain_days(),
            retain_weeks: default_retain_weeks(),
            retain_months: default_retain_months(),
            retain_all: false,
            encryption_enabled: false,
            encryption_password: String::new(),
            pack_size_bytes: default_pack_size(),
            chunk_size: default_chunk_size(),
            upload_limit_enabled: default_upload_limit_enabled(),
            upload_limit_bytes_per_sec: default_upload_limit_bps(),
            last_backup_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upload_limit_defaults_to_three_mb_per_second() {
        let cfg = AppConfig::default();
        assert!(cfg.upload_limit_enabled);
        assert_eq!(cfg.upload_limit_bytes_per_sec, 3 * 1024 * 1024);
    }

    #[test]
    fn older_configs_get_upload_limit_defaults() {
        let mut value = serde_json::to_value(AppConfig::default()).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("uploadLimitEnabled");
        object.remove("uploadLimitBytesPerSec");
        let cfg: AppConfig = serde_json::from_value(value).unwrap();
        assert!(cfg.upload_limit_enabled);
        assert_eq!(
            cfg.upload_limit_bytes_per_sec,
            crate::core::throttle::DEFAULT_UPLOAD_LIMIT_BPS
        );
    }

    #[test]
    fn debug_redacts_secrets() {
        let cfg = AppConfig {
            secret_access_key: "AKIA-LEAK-ME".into(),
            encryption_password: "PASSWORD-LEAK-ME".into(),
            ..AppConfig::default()
        };
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("AKIA-LEAK-ME"));
        assert!(!rendered.contains("PASSWORD-LEAK-ME"));
        assert!(rendered.contains("<redacted>"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn serialized_config_omits_secrets() {
        let cfg = AppConfig {
            secret_access_key: "AKIA-LEAK-ME".into(),
            encryption_password: "PASSWORD-LEAK-ME".into(),
            ..AppConfig::default()
        };
        let value = serde_json::to_value(&cfg).unwrap();
        let object = value.as_object().unwrap();
        assert!(!object.contains_key("secretAccessKey"));
        assert!(!object.contains_key("encryptionPassword"));
    }
}
