//! Application configuration and platform paths.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::core::error::{BackupError, Result};
use crate::core::model::{AppConfig, LEGACY_DEFAULT_EXCLUDE_PATTERNS};
use crate::core::settings::{self, SettingsDocument};

pub use crate::core::settings::{AppSettings, new_profile};

pub fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

#[cfg(target_os = "macos")]
pub fn app_support_dir() -> PathBuf {
    home_dir().join("Library/Application Support/Qia Backup")
}

#[cfg(not(target_os = "macos"))]
pub fn app_support_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(xdg).join("qia-backup")
    } else {
        home_dir().join(".config/qia-backup")
    }
}

#[cfg(target_os = "macos")]
pub fn log_dir() -> PathBuf {
    home_dir().join("Library/Logs/Qia Backup")
}

#[cfg(not(target_os = "macos"))]
pub fn log_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        PathBuf::from(xdg).join("qia-backup/logs")
    } else {
        home_dir().join(".local/state/qia-backup/logs")
    }
}

#[cfg(target_os = "macos")]
pub fn cache_dir() -> PathBuf {
    home_dir().join("Library/Caches/com.quantaia.backup")
}

#[cfg(not(target_os = "macos"))]
pub fn cache_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        PathBuf::from(xdg).join("qia-backup")
    } else {
        home_dir().join(".cache/qia-backup")
    }
}

pub fn config_path() -> PathBuf {
    app_support_dir().join("config.json")
}

pub fn default_snapshot_volume() -> String {
    "/System/Volumes/Data".to_string()
}

/// Remove the old broad defaults from a stored exclusion list while keeping
/// custom user patterns. Returns the number of removed entries.
pub fn strip_legacy_default_excludes(patterns: &mut Vec<String>) -> usize {
    let before = patterns.len();
    let defaults = crate::core::model::default_exclude_patterns();
    patterns.retain(|pattern| {
        !LEGACY_DEFAULT_EXCLUDE_PATTERNS
            .iter()
            .any(|legacy| legacy == pattern)
            || defaults.iter().any(|default| default == pattern)
    });
    before - patterns.len()
}

/// Load the selected destination for existing CLI callers. An unconfigured
/// installation gets a new independent draft and is not written to disk.
pub fn load() -> Result<AppConfig> {
    let settings = load_settings()?;
    Ok(settings
        .selected_profile()
        .cloned()
        .unwrap_or_else(|| new_profile(settings.selected_provider)))
}

/// Load both destination profiles from the single canonical config file.
/// A legacy single profile is migrated only into its actual provider's slot;
/// its UUID and complete non-secret configuration are preserved verbatim.
/// On macOS its legacy encryption password is copied into that destination's
/// scoped Keychain account before the atomic document replacement.
pub fn load_settings() -> Result<AppSettings> {
    let path = config_path();
    match settings::read_document(&path)? {
        None => Ok(AppSettings::default()),
        Some(SettingsDocument::Legacy(profile)) => {
            let stored = crate::core::secrets::load_legacy_for(&profile)?;
            let profile = profile_with_secrets(&profile, stored);
            crate::core::secrets::store(&profile)?;
            let settings = AppSettings::default().with_profile(&profile);
            settings::write_document(&path, &settings)?;
            Ok(settings)
        }
        Some(SettingsDocument::Current(stored_settings)) => {
            let settings = AppSettings {
                aws_s3: stored_settings
                    .aws_s3
                    .as_ref()
                    .map(load_profile_secrets)
                    .transpose()?,
                backblaze_b2: stored_settings
                    .backblaze_b2
                    .as_ref()
                    .map(load_profile_secrets)
                    .transpose()?,
                ..stored_settings.clone()
            };
            #[cfg(target_os = "macos")]
            if [&stored_settings.aws_s3, &stored_settings.backblaze_b2]
                .into_iter()
                .flatten()
                .any(|profile| {
                    !profile.secret_access_key.is_empty() || !profile.encryption_password.is_empty()
                })
            {
                // Hydration first stored any legacy plaintext values in their
                // owned Keychain accounts; remove their on-disk copies now.
                settings::write_document(&path, &settings)?;
            }
            Ok(settings)
        }
    }
}

fn profile_with_secrets(
    profile: &AppConfig,
    stored: crate::core::secrets::StoredSecrets,
) -> AppConfig {
    AppConfig {
        secret_access_key: stored
            .secret_access_key
            .unwrap_or_else(|| profile.secret_access_key.clone()),
        encryption_password: stored
            .encryption_password
            .unwrap_or_else(|| profile.encryption_password.clone()),
        ..profile.clone()
    }
}

fn load_profile_secrets(profile: &AppConfig) -> Result<AppConfig> {
    let stored = crate::core::secrets::load_for(profile)?;
    let loaded = profile_with_secrets(profile, stored);
    #[cfg(target_os = "macos")]
    if !profile.secret_access_key.is_empty() || !profile.encryption_password.is_empty() {
        crate::core::secrets::store(&loaded)?;
    }
    Ok(loaded)
}

/// Save the active CLI plan without dropping the other saved destination.
/// Callers persist changed secrets through `secrets` before saving settings.
pub fn save(config: &AppConfig) -> Result<()> {
    validate(config)?;
    let settings = load_settings()?.with_profile(config);
    save_settings(&settings)
}

/// Atomically save both profiles into config.json. AppConfig's macOS
/// serialization omits secret keys and encryption passwords; they remain in
/// their destination-owned Keychain accounts.
pub fn save_settings(settings: &AppSettings) -> Result<()> {
    settings::write_document(&config_path(), settings)
}

/// Validate just enough to reach S3. Used by preflight checks so an existing
/// encrypted set can be inspected even when the local password is missing.
pub fn validate_connection(config: &AppConfig) -> Result<()> {
    crate::core::store::S3Config::from_app(config).validate()
}

pub fn validate(config: &AppConfig) -> Result<()> {
    validate_connection(config)?;
    if config.encryption_enabled && config.encryption_password.is_empty() {
        return Err(BackupError::Config(
            "encryption is enabled but the encryption password is empty".into(),
        ));
    }
    if config.backup_set_uuid.trim().is_empty() {
        return Err(BackupError::Config("backup set UUID is required".into()));
    }
    if config.prefix.starts_with('/') || config.prefix.contains("//") {
        return Err(BackupError::Config(
            "S3 prefix must not start with '/' or contain '//'".into(),
        ));
    }
    if config.schedule_minutes > 0 && config.schedule_minutes < 5 {
        return Err(BackupError::Config(
            "schedule must be at least 5 minutes (or 0 to disable)".into(),
        ));
    }
    if config.chunk_size < 16 * 1024 {
        return Err(BackupError::Config("chunk size is too small".into()));
    }
    if config.pack_size_bytes < 1024 * 1024 {
        return Err(BackupError::Config("pack size is too small".into()));
    }
    // Validate path syntax now so a bad include cannot be saved and only
    // discovered hours later when a backup starts.
    normalize_included_folders(&config.included_folders)?;
    Ok(())
}

/// Validate the selected source. Kept separate from destination validation so
/// list/restore/prune can run on a machine where the source volume is absent.
/// Normalize a user-entered included folder path. Returns an empty string for
/// the source root (`.` or `/`). Paths are always relative to the source.
pub fn normalize_included_folder(path: &str) -> Result<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "/" {
        return Ok(String::new());
    }
    let trimmed = trimmed.trim_matches('/');
    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(BackupError::Config(format!(
                "invalid included folder path: {path:?}"
            )));
        }
        components.push(component);
    }
    Ok(components.join("/"))
}

/// Convert a path selected in a native picker into a backup-relative path.
///
/// The selected path must live inside `root` (the APFS data volume or a path
/// source). Symlinks are resolved before comparison so a picker selection made
/// through a firmlink/sidebar alias still maps to the right subtree.
pub fn relative_to_source(root: &Path, selected: &Path) -> Result<String> {
    let root_canonical = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let selected_canonical = selected
        .canonicalize()
        .unwrap_or_else(|_| selected.to_path_buf());
    let invalid = || {
        let message = if root_canonical == Path::new("/System/Volumes/Data") {
            "the selected path is not on the APFS data volume; choose a folder on this Mac's startup disk"
        } else {
            "select a file or folder inside the source"
        };
        BackupError::Config(message.into())
    };

    if selected_canonical == root_canonical {
        return Err(BackupError::Config(
            "select a file or folder inside the source, not the source root".into(),
        ));
    }

    // Prefer the lexical path so a symlink entry inside the source is excluded
    // as itself. Fall back to canonical paths for firmlinks/sidebar aliases.
    let relative = if let Ok(relative) = selected.strip_prefix(root) {
        relative.to_path_buf()
    } else if let Ok(relative) = selected_canonical.strip_prefix(&root_canonical) {
        relative.to_path_buf()
    } else if root_canonical == Path::new("/System/Volumes/Data")
        && same_device(&root_canonical, &selected_canonical)
    {
        // Finder exposes the Data volume through firmlinks such as /Users and
        // /Applications. A same-device path outside the mount prefix is still
        // a Data-volume path, so store it relative to the volume root.
        selected_canonical
            .strip_prefix(Path::new("/"))
            .map_err(|_| invalid())?
            .to_path_buf()
    } else {
        return Err(invalid());
    };
    let text = relative.to_string_lossy();
    let normalized = normalize_included_folder(&text)?;
    if normalized.is_empty() {
        return Err(BackupError::Config(
            "select a file or folder inside the source, not the source root".into(),
        ));
    }
    Ok(normalized)
}

#[cfg(target_os = "macos")]
fn same_device(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev(),
        _ => false,
    }
}

#[cfg(not(target_os = "macos"))]
fn same_device(_: &Path, _: &Path) -> bool {
    false
}

/// Normalize, deduplicate and collapse nested included folders. When a parent
/// folder is selected, selecting a child is redundant and would duplicate
/// work, so the child is dropped.
pub fn normalize_included_folders(paths: &[String]) -> Result<Vec<String>> {
    let mut normalized = Vec::new();
    for path in paths {
        let path = normalize_included_folder(path)?;
        if path.is_empty() {
            return Ok(vec![String::new()]);
        }
        if !normalized.contains(&path) {
            normalized.push(path);
        }
    }
    normalized.sort_by(|a, b| a.matches('/').count().cmp(&b.matches('/').count()));
    let mut result: Vec<String> = Vec::new();
    for path in normalized {
        if result
            .iter()
            .any(|parent| path == *parent || path.starts_with(&format!("{parent}/")))
        {
            continue;
        }
        result.push(path);
    }
    result.sort();
    Ok(result)
}

/// Validate that the configuration can start a backup. A path source may use
/// its whole selected path when no includes are given; an APFS snapshot must
/// explicitly list at least one included folder so the app never silently
/// scans the entire SSD.
pub fn validate_backup_selection(config: &AppConfig) -> Result<()> {
    let includes = normalize_included_folders(&config.included_folders)?;
    if config.source_kind != "path" && includes.is_empty() {
        return Err(BackupError::Config(
            "select at least one included folder for an APFS snapshot backup".into(),
        ));
    }
    Ok(())
}

pub fn validate_source(config: &AppConfig) -> Result<()> {
    if config.source_kind == "path" {
        if config.source_path.trim().is_empty() {
            return Err(BackupError::Config("a source path is required".into()));
        }
        if !Path::new(&config.source_path).exists() {
            return Err(BackupError::Config(format!(
                "source path does not exist: {}",
                config.source_path
            )));
        }
    }
    Ok(())
}

impl AppConfig {
    /// Key prefix inside the bucket for this backup set (no leading/trailing slash).
    pub fn key_root(&self) -> String {
        let prefix = self.prefix.trim_matches('/');
        if prefix.is_empty() {
            self.backup_set_uuid.clone()
        } else {
            format!("{}/{}", prefix, self.backup_set_uuid)
        }
    }

    pub fn display_name(&self) -> String {
        if self.backup_set_name.trim().is_empty() {
            "Time Machine".to_string()
        } else {
            self.backup_set_name.clone()
        }
    }

    pub fn effective_source(&self) -> String {
        if self.source_kind == "path" {
            self.source_path.clone()
        } else {
            self.snapshot_volume.clone()
        }
    }

    pub fn destination(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key_root())
    }

    pub fn source_description(&self) -> String {
        if self.source_kind == "path" {
            format!("Path: {}", self.source_path)
        } else {
            format!("APFS local snapshot of {}", self.snapshot_volume)
        }
    }

    pub fn touch_backup_time(&mut self) {
        self.last_backup_at = Some(Utc::now());
    }

    pub fn apply_plan(&mut self, plan: &crate::core::model::BackupPlan) {
        self.retain_hours = plan.retain_hours;
        self.retain_days = plan.retain_days;
        self.retain_weeks = plan.retain_weeks;
        self.retain_months = plan.retain_months;
        self.retain_all = plan.retain_all;
        if !plan.source_path.is_empty() {
            self.source_path = plan.source_path.clone();
            self.source_kind = plan.source_kind.clone();
        }
    }

    pub fn plan(&self) -> crate::core::model::BackupPlan {
        crate::core::model::BackupPlan {
            plan_uuid: format!("{}-plan", self.backup_set_uuid),
            name: self.display_name(),
            storage_location_id: "s3".to_string(),
            plan_version: 1,
            use_apfs_snapshots: self.source_kind == "snapshot",
            use_buzhash: true,
            retain_all: self.retain_all,
            retain_days: self.retain_days,
            retain_hours: self.retain_hours,
            retain_months: self.retain_months,
            retain_weeks: self.retain_weeks,
            schedule_json: format!(
                "{{\"type\":\"Interval\",\"minutes\":{}}}",
                self.schedule_minutes
            ),
            thread_count: crate::core::model::DEFAULT_THREAD_COUNT,
            cpu_usage: 100,
            enabled: self.schedule_minutes > 0,
            source_path: self.source_path.clone(),
            source_kind: self.source_kind.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BackupListEntry {
    pub timestamp: DateTime<Utc>,
    pub key: String,
    pub size: u64,
    pub is_complete: Option<bool>,
    pub files_count: Option<u64>,
    pub bytes_uploaded: Option<u64>,
    pub record_name: String,
}

pub fn computer_name() -> String {
    if let Ok(name) = std::env::var("COMPUTER_NAME") {
        if !name.trim().is_empty() {
            return name;
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("/usr/sbin/scutil")
            .args(["--get", "ComputerName"])
            .output()
        {
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }
    if let Ok(name) = std::env::var("HOSTNAME") {
        if !name.trim().is_empty() {
            return name;
        }
    }
    "Mac".to_string()
}

pub fn computer_serial() -> String {
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("/usr/sbin/ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                if line.contains("IOPlatformSerialNumber") {
                    if let Some(value) = line.split('=').nth(1) {
                        let v = value.trim().trim_matches('"').to_string();
                        if !v.is_empty() {
                            return v;
                        }
                    }
                }
            }
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_root_normalizes() {
        let mut cfg = AppConfig::default();
        cfg.prefix = "/backups/".into();
        cfg.backup_set_uuid = "abc".into();
        assert_eq!(cfg.key_root(), "backups/abc");
        cfg.prefix = String::new();
        assert_eq!(cfg.key_root(), "abc");
    }

    #[test]
    fn validation_rejects_missing_bucket() {
        let cfg = AppConfig::default();
        assert!(validate(&cfg).is_err());
    }

    #[test]
    fn default_region_is_eu_west_1() {
        assert_eq!(
            AppConfig::default().region,
            crate::core::model::DEFAULT_REGION
        );
        assert_eq!(AppConfig::default().region, "eu-west-1");
    }

    #[test]
    fn recommended_config_has_includes_and_cache_defaults() {
        let cfg = AppConfig::recommended();
        assert_eq!(
            cfg.included_folders,
            crate::core::model::default_included_folders()
        );
        assert!(
            cfg.exclude_patterns
                .iter()
                .any(|p| p == "**/Library/Caches")
        );
        assert!(!cfg.exclude_patterns.iter().any(|p| p == "**/dev"));
        assert!(AppConfig::default().included_folders.is_empty());
        assert!(AppConfig::default().exclude_patterns.is_empty());
    }

    #[test]
    fn include_paths_are_normalized_and_nested_paths_collapse() {
        let paths = vec![
            "/Users/me/Documents/".to_string(),
            "Users/me/Documents/Projects".to_string(),
            "Users/me/Pictures".to_string(),
        ];
        assert_eq!(
            normalize_included_folders(&paths).unwrap(),
            vec!["Users/me/Documents", "Users/me/Pictures"]
        );
        assert_eq!(
            normalize_included_folders(&[".".to_string()]).unwrap(),
            vec![String::new()]
        );
        assert!(normalize_included_folders(&["../escape".to_string()]).is_err());
    }

    #[test]
    fn relative_picker_paths_are_normalized_and_bounded() {
        let root = std::env::temp_dir().join(format!("qia-picker-{}", uuid::Uuid::new_v4()));
        let nested = root.join("Users/me/Documents");
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(
            relative_to_source(&root, &nested).unwrap(),
            "Users/me/Documents"
        );
        assert!(relative_to_source(&root, &root).is_err());
        assert!(relative_to_source(&root, &std::env::temp_dir()).is_err());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn legacy_default_excludes_are_removed_but_current_defaults_remain() {
        let mut patterns = vec![
            "**/Caches".to_string(),
            "**/dev".to_string(),
            "**/Library/Caches".to_string(),
            "**/*.tmp".to_string(),
        ];
        assert_eq!(strip_legacy_default_excludes(&mut patterns), 2);
        assert_eq!(patterns, vec!["**/Library/Caches", "**/*.tmp"]);
    }
}
