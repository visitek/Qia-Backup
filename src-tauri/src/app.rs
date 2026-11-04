//! Tauri desktop shell (macOS only).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;
#[cfg(target_os = "macos")]
use tauri::RunEvent;
use tauri::image::Image;
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::{TrayIcon, TrayIconBuilder};
use tauri::{AppHandle, Emitter, Manager, State, WindowEvent, Wry};
use tauri_plugin_dialog::DialogExt;

use qia_backup::core::config;
use qia_backup::core::content;
use qia_backup::core::cost;
use qia_backup::core::diff;
use qia_backup::core::edit;
use qia_backup::core::engine::{CancelToken, ProgressEvent, run_backup_with_partial};
use qia_backup::core::error::BackupError;
use qia_backup::core::logging;
use qia_backup::core::model::AppConfig;
use qia_backup::core::partial::PartialTree;
use qia_backup::core::preflight;
use qia_backup::core::record;
use qia_backup::core::restore::{self, RestoreEvent};
use qia_backup::core::retention;
use qia_backup::core::scheduler;
use qia_backup::core::secrets;
use qia_backup::core::snapshot;
use qia_backup::core::status::AppStatus;
use qia_backup::core::store::{new_store, new_store_with_upload_throttle};
use qia_backup::core::throttle::{MIN_UPLOAD_LIMIT_BPS, UploadLimiter};
use qia_backup::core::versions;

pub type SharedState = Arc<AppInner>;

const AUTH_GRANT_TTL: Duration = Duration::from_secs(300);

pub struct AppInner {
    config: Mutex<AppConfig>,
    status: Arc<Mutex<AppStatus>>,
    cancel: Mutex<CancelToken>,
    partial_tree: PartialTree,
    /// A short-lived, action-scoped grant produced by the macOS authentication
    /// prompt. Destructive commands consume it exactly once.
    sensitive_auth: Mutex<Option<SensitiveGrant>>,
    /// Shared, live-updatable limit applied to S3 PUT bodies.
    upload_limiter: Arc<UploadLimiter>,
    /// Sleep/battery policy and the macOS observers that feed it.
    power: crate::power::PowerMonitor,
    /// Set before cancelling a run because power conditions changed. The
    /// backup error handler turns this cancellation into a resumable pause.
    power_pause_requested: AtomicBool,
}

struct SensitiveGrant {
    action: String,
    granted_at: Instant,
}

fn consume_sensitive_grant(inner: &AppInner, action: &str) -> Result<(), String> {
    let mut guard = inner.sensitive_auth.lock();
    match guard.take() {
        Some(grant) if grant.action == action && grant.granted_at.elapsed() <= AUTH_GRANT_TTL => {
            Ok(())
        }
        _ => Err("authentication is required for this action".into()),
    }
}

struct TrayState {
    tray: TrayIcon<Wry>,
    progress: MenuItem<Wry>,
    cancel: MenuItem<Wry>,
    auto: MenuItem<Wry>,
    upload_limit: MenuItem<Wry>,
    _menu: Menu<Wry>,
}

fn show_main_window(app: &AppHandle) {
    #[cfg(target_os = "macos")]
    {
        let _ = app.show();
    }
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn short_object_name(key: &str) -> &str {
    key.rsplit('/').next().unwrap_or(key)
}

fn tray_status(status: &AppStatus) -> (Option<String>, String, String) {
    if status.restoring {
        let progress = if status.total_files > 0 {
            format!("Restore · {} / {} files", status.files, status.total_files)
        } else {
            format!("Restore · {} files", status.files)
        };
        (
            Some("R".to_string()),
            format!("Qia Backup — {progress}"),
            progress,
        )
    } else if status.editing {
        (
            Some("…".to_string()),
            "Qia Backup — updating the backup".to_string(),
            "Updating backup…".to_string(),
        )
    } else if status.running {
        let total = status.total_files;
        let percent = if total > 0 {
            Some((status.files.min(total) * 100 / total) as u32)
        } else {
            None
        };
        let title = percent
            .map(|p| format!("{p}%"))
            .unwrap_or_else(|| "…".to_string());
        let progress = match percent {
            Some(p) => format!("Backup {p}% · {} / {} files", status.files, total),
            None => format!("Backup · {} files · {}", status.files, status.phase),
        };
        let speed = if status.upload_speed_bps > 0 {
            format!(" · {}/s", human_bytes(status.upload_speed_bps))
        } else {
            String::new()
        };
        let tooltip = format!(
            "Qia Backup — {progress} · {} read · {} uploaded{}",
            human_bytes(status.bytes_scanned),
            human_bytes(status.bytes_uploaded),
            speed
        );
        (Some(title), tooltip, progress)
    } else if let Some(error) = &status.last_error {
        (
            Some("!".to_string()),
            format!("Qia Backup — last operation failed: {error}"),
            "Last operation failed".to_string(),
        )
    } else {
        (None, "Qia Backup — idle".to_string(), "Idle".to_string())
    }
}

fn spawn_tray_updater(app: AppHandle, status: Arc<Mutex<AppStatus>>) {
    std::thread::spawn(move || {
        let Some(state) = app.try_state::<TrayState>() else {
            return;
        };
        let tray = state.tray.clone();
        let progress_item = state.progress.clone();
        let cancel_item = state.cancel.clone();
        let auto_item = state.auto.clone();
        let upload_limit_item = state.upload_limit.clone();
        drop(state);

        let mut last_title: Option<String> = None;
        let mut last_tooltip = String::new();
        let mut last_progress = String::new();
        let mut last_cancel_label = String::new();
        let mut last_cancel_enabled: Option<bool> = None;
        let mut last_auto_label = String::new();
        let mut last_upload_limit_label = String::new();
        loop {
            let status = status.lock().clone();
            let (title, tooltip, progress) = tray_status(&status);
            let cancel_label = if status.cancel_requested {
                "Cancelling…".to_string()
            } else if status.restoring {
                "Cancel restore".to_string()
            } else {
                "Cancel backup".to_string()
            };
            let cancel_enabled = (status.running || status.restoring) && !status.cancel_requested;
            let auto_label = if status.scheduler_paused {
                "Resume automatic backups".to_string()
            } else {
                "Pause automatic backups".to_string()
            };
            let upload_limit_label = format!(
                "{} upload limit ({}/s)",
                if status.upload_limit_enabled {
                    "Disable"
                } else {
                    "Enable"
                },
                human_bytes(status.upload_limit_bytes_per_sec)
            );

            if title != last_title {
                let _ = tray.set_title(title.as_deref());
                last_title = title;
            }
            if tooltip != last_tooltip {
                let _ = tray.set_tooltip(Some(tooltip.as_str()));
                last_tooltip = tooltip;
            }
            if progress != last_progress {
                let _ = progress_item.set_text(progress.as_str());
                last_progress = progress;
            }
            if cancel_label != last_cancel_label {
                let _ = cancel_item.set_text(cancel_label.as_str());
                last_cancel_label = cancel_label;
            }
            if Some(cancel_enabled) != last_cancel_enabled {
                let _ = cancel_item.set_enabled(cancel_enabled);
                last_cancel_enabled = Some(cancel_enabled);
            }
            if auto_label != last_auto_label {
                let _ = auto_item.set_text(auto_label.as_str());
                last_auto_label = auto_label;
            }
            if upload_limit_label != last_upload_limit_label {
                let _ = upload_limit_item.set_text(upload_limit_label.as_str());
                last_upload_limit_label = upload_limit_label;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    });
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Bootstrap {
    pub config: AppConfig,
    pub has_secret_access_key: bool,
    pub has_encryption_password: bool,
    pub status: AppStatus,
    pub logs: Vec<String>,
    pub version: String,
    pub config_path: String,
    pub log_path: String,
    pub destination: String,
    pub source: String,
    pub snapshots: Vec<String>,
    pub tm_destinations: Vec<String>,
    pub restore_destination: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PickedBackupPath {
    pub absolute: String,
    pub relative: String,
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    pub status: AppStatus,
    pub logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResponse {
    pub volume: String,
    pub snapshots: Vec<String>,
    pub destinations: Vec<String>,
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

fn snapshot_names(volume: &str) -> Vec<String> {
    snapshot::list_local_snapshots(volume)
        .map(|v| v.into_iter().map(|s| s.name).collect())
        .unwrap_or_default()
}

#[tauri::command]
fn get_state(state: State<'_, SharedState>) -> Bootstrap {
    let config = state.config.lock().clone();
    let status = state.status.lock().clone();
    let volume = if config.snapshot_volume.is_empty() {
        config::default_snapshot_volume()
    } else {
        config.snapshot_volume.clone()
    };
    let has_secret_access_key = !config.secret_access_key.is_empty();
    let has_encryption_password = !config.encryption_password.is_empty();
    Bootstrap {
        destination: config.destination(),
        source: config.source_description(),
        snapshots: snapshot_names(&volume),
        tm_destinations: snapshot::time_machine_destinations(),
        restore_destination: restore::default_restore_destination()
            .to_string_lossy()
            .to_string(),
        config,
        has_secret_access_key,
        has_encryption_password,
        status,
        logs: logging::tail(400),
        version: env!("CARGO_PKG_VERSION").to_string(),
        config_path: config::config_path().to_string_lossy().to_string(),
        log_path: config::log_dir()
            .join("qia-backup.log")
            .to_string_lossy()
            .to_string(),
    }
}

#[tauri::command]
fn get_status(state: State<'_, SharedState>) -> StatusResponse {
    let (limit_enabled, limit_bps) = state.upload_limiter.settings();
    let mut status = state.status.lock().clone();
    if !status.running || status.restoring {
        status.upload_speed_bps = 0;
    }
    status.upload_limit_enabled = limit_enabled;
    status.upload_limit_bytes_per_sec = limit_bps;
    StatusResponse {
        status,
        logs: logging::tail(200),
    }
}

#[tauri::command]
fn save_config(state: State<'_, SharedState>, mut config: AppConfig) -> Result<AppConfig, String> {
    let previous = state.config.lock().clone();
    // Secrets are not sent to the webview. An empty field means "keep the
    // value that is already in the Keychain". The encryption password belongs
    // to a backup set and must not be carried across to a different set.
    if config.secret_access_key.is_empty() {
        config.secret_access_key = previous.secret_access_key.clone();
    }
    if config.backup_set_uuid == previous.backup_set_uuid && config.encryption_password.is_empty() {
        config.encryption_password = previous.encryption_password.clone();
    }
    // The desktop app is APFS-snapshot-only.
    config.source_kind = "snapshot".into();
    config.source_path.clear();
    config::validate(&config).map_err(err)?;
    config::validate_source(&config).map_err(err)?;
    secrets::store_changed(&previous, &config).map_err(err)?;
    config::save(&config).map_err(err)?;
    *state.config.lock() = config.clone();
    state.upload_limiter.set(
        config.upload_limit_enabled,
        config.upload_limit_bytes_per_sec,
    );
    {
        let mut status = state.status.lock();
        status.upload_limit_enabled = config.upload_limit_enabled;
        status.upload_limit_bytes_per_sec = config.upload_limit_bytes_per_sec;
        // A previous error may have been about the settings that just changed.
        status.last_error = None;
    }
    logging::info("configuration saved");
    Ok(config)
}

#[tauri::command]
async fn test_connection(state: State<'_, SharedState>) -> Result<String, String> {
    let cfg = state.config.lock().clone();
    tauri::async_runtime::spawn_blocking(move || {
        config::validate_connection(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        // Deliberately do not enumerate the whole set: a full OS backup can
        // contain millions of objects, and listing them just to say "OK" is
        // slow and unnecessary.
        store.check_access().map_err(err)?;
        let check = preflight::check(&cfg, store.as_ref()).map_err(err)?;
        if !check.can_backup {
            return Err(check.message);
        }
        Ok(format!(
            "Connected to {} — bucket is readable. {}",
            cfg.destination(),
            check.message
        ))
    })
    .await
    .map_err(err)?
}

/// Inspect the existing destination without changing it. The UI uses this to
/// surface encryption/password problems before a backup is started.
#[tauri::command]
async fn check_destination(
    state: State<'_, SharedState>,
) -> Result<preflight::DestinationCheck, String> {
    let cfg = state.config.lock().clone();
    tauri::async_runtime::spawn_blocking(move || {
        config::validate_connection(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        preflight::check(&cfg, store.as_ref()).map_err(err)
    })
    .await
    .map_err(err)?
}

/// List one directory from a backup record for the file explorer. An empty or
/// `latest` record key resolves the current backup.
/// Add a literal exclusion for a file or folder selected in the explorer.
/// This only affects future backups; existing records are left untouched.
#[tauri::command]
fn ignore_backup_entry(state: State<'_, SharedState>, path: String) -> Result<Vec<String>, String> {
    let mut guard = state.config.lock();
    let mut config = guard.clone();
    let added = edit::add_ignore_path(&mut config, &path).map_err(err)?;
    if added.is_empty() {
        return Ok(added);
    }
    config::save(&config).map_err(err)?;
    *guard = config;
    logging::info(format!("ignored backup path {path:?} in future backups"));
    Ok(added)
}

/// Remove one file or folder from a completed record and reclaim objects that
/// no other record references. Partial data has no record to edit.
#[tauri::command]
async fn remove_backup_entry(
    state: State<'_, SharedState>,
    record_key: Option<String>,
    path: String,
) -> Result<edit::RemoveSummary, String> {
    consume_sensitive_grant(state.inner(), "remove")?;
    {
        let mut status = state.status.lock();
        if status.running || status.restoring || status.editing {
            return Err("stop the active backup or restore before removing files".into());
        }
        status.editing = true;
        status.phase = "editing".into();
        status.detail = format!("Removing {path} from backup…");
        status.last_error = None;
    }
    let cfg = state.config.lock().clone();
    if let Err(e) = config::validate(&cfg) {
        state.status.lock().fail(err(&e));
        return Err(err(e));
    }
    let status_arc = state.status.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let store = match new_store(&cfg) {
            Ok(store) => store,
            Err(e) => {
                status_arc.lock().fail(err(&e));
                return Err(err(e));
            }
        };
        let cancel = CancelToken::new();
        let summary =
            match edit::remove_entry(&cfg, store.as_ref(), record_key.as_deref(), &path, &cancel) {
                Ok(summary) => summary,
                Err(e) => {
                    status_arc.lock().fail(err(&e));
                    return Err(err(e));
                }
            };
        if summary.index_saved {
            match retention::prune_unreferenced(&cfg, store.as_ref(), &cancel) {
                Ok(gc) => logging::info(format!(
                    "removed {} from backup; reclaimed {} objects",
                    summary.path, gc.objects_deleted
                )),
                Err(e) => logging::warn(format!(
                    "removed {} from backup, but garbage collection failed: {e}",
                    summary.path
                )),
            }
        }
        {
            let mut status = status_arc.lock();
            status.editing = false;
            status.phase = "idle".into();
            status.detail.clear();
            status.last_finished_at = Some(chrono::Utc::now());
        }
        Ok(summary)
    })
    .await
    .map_err(err)?
}

/// Remove every entry that the current ignore rules match from a completed
/// record. This is the bulk counterpart of `remove_backup_entry` and requires
/// the same one-shot authentication grant.
#[tauri::command]
async fn remove_ignored_entries(
    state: State<'_, SharedState>,
    record_key: Option<String>,
) -> Result<edit::RemoveIgnoredSummary, String> {
    consume_sensitive_grant(state.inner(), "remove")?;
    {
        let mut status = state.status.lock();
        if status.running || status.restoring || status.editing {
            return Err("stop the active backup or restore before removing files".into());
        }
        status.editing = true;
        status.phase = "editing".into();
        status.detail = "Removing ignored paths from backup…".into();
        status.last_error = None;
    }
    let cfg = state.config.lock().clone();
    if let Err(e) = config::validate(&cfg) {
        state.status.lock().fail(err(&e));
        return Err(err(e));
    }
    let status_arc = state.status.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let store = match new_store(&cfg) {
            Ok(store) => store,
            Err(e) => {
                status_arc.lock().fail(err(&e));
                return Err(err(e));
            }
        };
        let cancel = CancelToken::new();
        let summary = match edit::remove_ignored_entries(
            &cfg,
            store.as_ref(),
            record_key.as_deref(),
            &cancel,
        ) {
            Ok(summary) => summary,
            Err(e) => {
                status_arc.lock().fail(err(&e));
                return Err(err(e));
            }
        };
        if summary.index_saved && summary.removed_entries > 0 {
            match retention::prune_unreferenced(&cfg, store.as_ref(), &cancel) {
                Ok(gc) => logging::info(format!(
                    "removed {} ignored entries from backup; reclaimed {} objects",
                    summary.removed_entries, gc.objects_deleted
                )),
                Err(e) => logging::warn(format!(
                    "removed {} ignored entries from backup, but garbage collection failed: {e}",
                    summary.removed_entries
                )),
            }
        }
        {
            let mut status = status_arc.lock();
            status.editing = false;
            status.phase = "idle".into();
            status.detail.clear();
            status.last_finished_at = Some(chrono::Utc::now());
        }
        Ok(summary)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
async fn list_backup_entries(
    state: State<'_, SharedState>,
    record_key: Option<String>,
    path: String,
) -> Result<restore::BackupListing, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        restore::list_entries(&cfg, store.as_ref(), record_key.as_deref(), &path).map_err(err)
    })
    .await
    .map_err(err)?
}

/// Version history for one path across all completed records. `at_record`
/// marks the version that was present in the record the user was browsing.
#[tauri::command]
async fn backup_file_versions(
    state: State<'_, SharedState>,
    path: String,
    at_record: Option<String>,
) -> Result<versions::FileVersionHistory, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        versions::file_versions(&cfg, store.as_ref(), &path, at_record.as_deref()).map_err(err)
    })
    .await
    .map_err(err)?
}

/// Number of distinct versions for every child of a directory.
#[tauri::command]
async fn backup_version_counts(
    state: State<'_, SharedState>,
    path: String,
) -> Result<versions::VersionCounts, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        versions::version_counts(&cfg, store.as_ref(), &path).map_err(err)
    })
    .await
    .map_err(err)?
}

/// Read one file's text content from a completed record.
#[tauri::command]
async fn read_backup_file_content(
    state: State<'_, SharedState>,
    record_key: Option<String>,
    path: String,
) -> Result<content::FileContent, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        restore::read_file_content(&cfg, store.as_ref(), record_key.as_deref(), &path).map_err(err)
    })
    .await
    .map_err(err)?
}

/// Read one file's text content from the live partial backup.
#[tauri::command]
async fn read_partial_file_content(
    state: State<'_, SharedState>,
    path: String,
) -> Result<content::FileContent, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    let partial = state.partial_tree.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        restore::read_partial_file_content(&cfg, store.as_ref(), &partial, &path).map_err(err)
    })
    .await
    .map_err(err)?
}

/// List a directory from the live in-progress backup tree.
#[tauri::command]
async fn list_partial_entries(
    state: State<'_, SharedState>,
    path: String,
) -> Result<restore::BackupListing, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    let partial = state.partial_tree.clone();
    let status_arc = state.status.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        let root = cfg.key_root();
        if !record::partial_run_exists(store.as_ref(), &root).map_err(err)? {
            partial.clear();
            status_arc.lock().partial_available = false;
            return Err(
                "the destination no longer contains this partial backup; local partial data was discarded"
                    .to_string(),
            );
        }
        restore::list_partial_entries(&cfg, store.as_ref(), &partial, &path).map_err(err)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
fn start_backup(
    state: State<'_, SharedState>,
    app: AppHandle,
    cleanup_authorized: Option<bool>,
) -> Result<(), String> {
    start_backup_inner(
        state.inner().clone(),
        app,
        "manual",
        cleanup_authorized.unwrap_or(false),
    )
}

fn start_backup_inner(
    inner: SharedState,
    app: AppHandle,
    reason: &str,
    cleanup_authorized: bool,
) -> Result<(), String> {
    {
        let status = inner.status.lock();
        if status.running || status.restoring || status.editing {
            return Err("another backup or restore is already running".into());
        }
    }
    if !inner.power.can_run() {
        return Err(inner
            .power
            .message()
            .unwrap_or_else(|| "backups are paused until power conditions recover".into()));
    }
    if cleanup_authorized {
        consume_sensitive_grant(inner.as_ref(), "cleanup")?;
    }
    let config = inner.config.lock().clone();
    config::validate_connection(&config).map_err(err)?;
    config::validate_source(&config).map_err(err)?;
    config::validate_backup_selection(&config).map_err(err)?;
    let cancel = CancelToken::new();
    *inner.cancel.lock() = cancel.clone();
    let partial_tree = inner.partial_tree.clone();
    {
        let mut status = inner.status.lock();
        status.begin(
            "starting",
            format!("{reason} backup to {}", config.destination()),
        );
        status.partial_available = false;
        status.power_paused = false;
        status.power_message = inner.power.message().unwrap_or_default();
    }
    inner.power_pause_requested.store(false, Ordering::SeqCst);
    logging::info(format!("starting backup ({reason})"));
    let scheduled = reason == "scheduled";
    std::thread::spawn(move || {
        let mut cfg = config;
        let status_arc = inner.status.clone();
        let app_progress = app.clone();
        let mut last_emit = Instant::now();
        let result = match new_store_with_upload_throttle(
            &cfg,
            inner.upload_limiter.clone(),
            cancel.clone(),
        ) {
            Ok(store) => match preflight::check(&cfg, store.as_ref()) {
                Ok(check) if !check.can_backup => Err(BackupError::Config(check.message)),
                Ok(_) => run_backup_with_partial(
                    &mut cfg,
                    store.as_ref(),
                    cancel.clone(),
                    &mut |event| {
                        // Always keep the polled status current, but throttle the
                        // event stream: a large backup emits one File event per
                        // file, and forwarding millions of them to the webview is
                        // far more expensive than polling twice a second.
                        apply_progress(&status_arc, &event);
                        if partial_tree.is_available() {
                            let mut status = status_arc.lock();
                            if !status.partial_available {
                                status.partial_available = true;
                            }
                        }
                        let important = matches!(
                            &event,
                            ProgressEvent::Phase { .. }
                                | ProgressEvent::Totals { .. }
                                | ProgressEvent::Warning { .. }
                                | ProgressEvent::Finished { .. }
                        );
                        if important || last_emit.elapsed() >= Duration::from_millis(500) {
                            let _ = app_progress.emit("backup-progress", &event);
                            last_emit = Instant::now();
                        }
                    },
                    Some(partial_tree.clone()),
                    cleanup_authorized,
                ),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        match result {
            Ok(summary) => {
                let summary_for_status = summary.clone();
                {
                    let mut status = inner.status.lock();
                    status.partial_available = false;
                }
                {
                    let mut shared = inner.config.lock();
                    shared.last_backup_at = Some(summary.finished_at);
                    if let Err(e) = config::save(&shared) {
                        logging::warn(format!("could not save config after backup: {e}"));
                    }
                }
                {
                    let mut status = inner.status.lock();
                    status.phase = "pruning".into();
                    status.detail = "applying retention".into();
                }
                if !cfg.retain_all {
                    let prune_cfg = inner.config.lock().clone();
                    match new_store(&prune_cfg) {
                        Ok(store) => {
                            if let Err(e) = qia_backup::core::retention::prune(
                                &prune_cfg,
                                store.as_ref(),
                                &cancel,
                                false,
                            ) {
                                logging::warn(format!("prune failed: {e}"));
                            }
                        }
                        Err(e) => logging::warn(format!("prune store: {e}")),
                    }
                }
                {
                    let mut status = inner.status.lock();
                    status.running = false;
                    status.phase = "idle".into();
                    status.detail = String::new();
                    status.recent_activity.clear();
                    status.last_summary = Some(summary_for_status.clone());
                    status.last_finished_at = Some(summary.finished_at);
                    status.cancel_requested = false;
                    status.scheduler_failures = 0;
                    status.scheduler_retry_at = None;
                    status.upload_speed_bps = 0;
                    status.upload_target_threads = 0;
                    status.upload_max_threads = 0;
                    status.upload_in_flight = 0;
                    status.power_paused = false;
                    status.power_message = inner.power.message().unwrap_or_default();
                }
                inner.power_pause_requested.store(false, Ordering::SeqCst);
                let _ = app.emit("backup-finished", &summary);
            }
            Err(BackupError::Cancelled) => {
                let power_pause = inner.power_pause_requested.swap(false, Ordering::SeqCst);
                if power_pause {
                    let message = inner.power.message().unwrap_or_else(|| {
                        "backup paused until power conditions recover".to_string()
                    });
                    {
                        let mut status = inner.status.lock();
                        status.running = false;
                        status.phase = "paused".into();
                        status.detail = message.clone();
                        status.power_paused = true;
                        status.power_message = message.clone();
                        status.last_finished_at = Some(chrono::Utc::now());
                        status.partial_available = partial_tree.is_available();
                        status.scheduler_failures = 0;
                        status.scheduler_retry_at = None;
                        status.upload_speed_bps = 0;
                        status.upload_target_threads = 0;
                        status.upload_max_threads = 0;
                        status.upload_in_flight = 0;
                    }
                    logging::warn("backup paused for power conditions");
                    let _ = app.emit("backup-paused", message);
                } else {
                    {
                        let mut status = inner.status.lock();
                        status.running = false;
                        status.phase = "cancelled".into();
                        status.detail = "backup cancelled".into();
                        status.last_finished_at = Some(chrono::Utc::now());
                        status.partial_available = partial_tree.is_available();
                        status.scheduler_failures = 0;
                        status.scheduler_retry_at = None;
                        status.power_paused = false;
                        status.power_message = inner.power.message().unwrap_or_default();
                        if scheduled {
                            // Do not immediately retry a scheduled run the user
                            // just cancelled.
                            status.scheduler_paused = true;
                        }
                    }
                    logging::warn("backup cancelled");
                    let _ = app.emit("backup-cancelled", ());
                }
            }
            Err(e) => {
                inner.power_pause_requested.store(false, Ordering::SeqCst);
                let message = e.to_string();
                let transient = scheduler::is_transient(&e);
                {
                    let mut status = inner.status.lock();
                    status.partial_available = partial_tree.is_available();
                    status.fail(message.clone());
                    if transient {
                        // A short network outage should recover automatically.
                        // Keep the scheduler enabled and back off instead of
                        // pausing it until the user notices.
                        status.scheduler_failures = status.scheduler_failures.saturating_add(1);
                        let delay = scheduler::retry_delay(status.scheduler_failures);
                        status.scheduler_retry_at = Some(chrono::Utc::now() + delay);
                        logging::warn(format!(
                            "backup failed transiently; automatic retry in {}s",
                            delay.num_seconds()
                        ));
                    } else if scheduled {
                        // A scheduled failure that needs user action (wrong
                        // password, mismatch, config) should not retry every
                        // 30 seconds.
                        status.scheduler_paused = true;
                    }
                }
                logging::error(format!("backup failed: {message}"));
                let _ = app.emit("backup-failed", message);
            }
        }
    });
    Ok(())
}

fn apply_progress(status: &Arc<Mutex<AppStatus>>, event: &ProgressEvent) {
    let mut status = status.lock();
    match event {
        ProgressEvent::Phase { name, detail } => {
            // Keep showing the cancellation state while the current upload or
            // file operation unwinds.
            if !status.cancel_requested {
                status.phase = name.clone();
                status.detail = detail.clone();
            }
            let text = if detail.trim().is_empty() {
                name.clone()
            } else {
                format!("{name}: {detail}")
            };
            status.record_activity(text);
        }
        ProgressEvent::Activity { text } => {
            status.record_activity(text.clone());
        }
        ProgressEvent::File {
            files,
            bytes_scanned,
            ..
        } => {
            status.files = *files;
            status.bytes_scanned = *bytes_scanned;
        }
        ProgressEvent::Upload { bytes, key, .. } => {
            status.bytes_uploaded += *bytes;
            status.record_activity(format!(
                "Uploaded {} ({})",
                short_object_name(key),
                human_bytes(*bytes)
            ));
        }
        ProgressEvent::Transfer {
            speed_bps,
            per_worker_bps,
            utilization,
            target_threads,
            max_threads,
            in_flight,
            ..
        } => {
            status.upload_speed_bps = *speed_bps;
            status.upload_per_worker_bps = *per_worker_bps;
            status.upload_utilization = *utilization;
            status.upload_target_threads = *target_threads;
            status.upload_max_threads = *max_threads;
            status.upload_in_flight = *in_flight;
            if *in_flight > 0 {
                let mut text = format!(
                    "Uploading {} object{}",
                    in_flight,
                    if *in_flight == 1 { "" } else { "s" }
                );
                if *speed_bps > 0 {
                    text.push_str(&format!(" at {}", human_bytes(*speed_bps)));
                }
                status.record_activity(text);
            } else if *speed_bps > 0 {
                status.record_activity(format!("Finishing uploads at {}", human_bytes(*speed_bps)));
            }
        }
        ProgressEvent::Totals { files, bytes } => {
            status.total_files = *files;
            status.total_bytes = *bytes;
        }
        ProgressEvent::Warning { .. } => {}
        ProgressEvent::Finished { summary } => {
            status.upload_speed_bps = 0;
            status.upload_in_flight = 0;
            status.last_summary = Some(summary.clone());
        }
    }
}

/// Prompt for Touch ID / Face ID / account password before a destructive
/// action. The returned grant is scoped to one action and consumed by the
/// matching command.
#[tauri::command]
async fn authorize_sensitive_action(
    state: State<'_, SharedState>,
    action: String,
) -> Result<(), String> {
    let reason = match action.as_str() {
        "remove" => "remove a file from a backup revision",
        "delete_set" => "delete the entire backup set",
        "prune" => "delete old backup revisions",
        "cleanup" => "clean up uploads left by an interrupted backup",
        _ => return Err("unknown sensitive action".into()),
    }
    .to_string();
    let inner = state.inner().clone();
    tauri::async_runtime::spawn_blocking(move || crate::auth::authenticate(&reason))
        .await
        .map_err(err)??;
    *inner.sensitive_auth.lock() = Some(SensitiveGrant {
        action,
        granted_at: Instant::now(),
    });
    Ok(())
}

#[tauri::command]
fn clear_sensitive_authorization(state: State<'_, SharedState>) {
    *state.sensitive_auth.lock() = None;
}

/// Report whether an interrupted run left uploads that require an authorized
/// cleanup. The UI checks this before a manual backup so it can show
/// Confirm -> Auth instead of cleaning automatically.
#[tauri::command]
async fn cleanup_pending(state: State<'_, SharedState>) -> Result<bool, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        let root = cfg.key_root();
        Ok(record::partial_run_exists(store.as_ref(), &root).map_err(err)?
            || record::cleanup_pending_exists(store.as_ref(), &root).map_err(err)?)
    })
    .await
    .map_err(err)?
}

fn spawn_power_watcher(inner: SharedState) {
    std::thread::spawn(move || {
        loop {
            let message = inner.power.message();
            let can_run = inner.power.can_run();
            let (running, restoring, cancel_requested) = {
                let status = inner.status.lock();
                (status.running, status.restoring, status.cancel_requested)
            };
            inner.status.lock().power_message = message.clone().unwrap_or_default();

            if running && !restoring && !can_run && !cancel_requested {
                {
                    let mut status = inner.status.lock();
                    status.phase = "pausing".into();
                    status.detail = message
                        .clone()
                        .unwrap_or_else(|| "paused for power conditions".into());
                }
                inner.power_pause_requested.store(true, Ordering::SeqCst);
                inner.cancel.lock().cancel();
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    });
}

fn set_scheduler_paused_inner(inner: &AppInner, paused: bool) -> bool {
    inner.status.lock().scheduler_paused = paused;
    logging::info(if paused {
        "automatic backups paused"
    } else {
        "automatic backups enabled"
    });
    paused
}

#[tauri::command]
fn set_scheduler_paused(state: State<'_, SharedState>, paused: bool) -> bool {
    set_scheduler_paused_inner(state.inner().as_ref(), paused)
}

fn set_upload_limit_enabled_inner(inner: &AppInner, enabled: bool) -> Result<bool, String> {
    let mut config = inner.config.lock().clone();
    config.upload_limit_enabled = enabled;
    if config.upload_limit_bytes_per_sec < MIN_UPLOAD_LIMIT_BPS {
        config.upload_limit_bytes_per_sec = MIN_UPLOAD_LIMIT_BPS;
    }
    config::save(&config).map_err(err)?;
    *inner.config.lock() = config.clone();
    inner.upload_limiter.set(
        config.upload_limit_enabled,
        config.upload_limit_bytes_per_sec,
    );
    {
        let mut status = inner.status.lock();
        status.upload_limit_enabled = config.upload_limit_enabled;
        status.upload_limit_bytes_per_sec = config.upload_limit_bytes_per_sec;
    }
    logging::info(if enabled {
        "upload limit enabled"
    } else {
        "upload limit disabled"
    });
    Ok(enabled)
}

#[tauri::command]
fn set_upload_limit_enabled(state: State<'_, SharedState>, enabled: bool) -> Result<bool, String> {
    set_upload_limit_enabled_inner(state.inner().as_ref(), enabled)
}

#[tauri::command]
fn apply_upload_limit(state: State<'_, SharedState>, bytes_per_sec: u64) -> Result<(), String> {
    if bytes_per_sec < MIN_UPLOAD_LIMIT_BPS {
        return Err("upload limit must be at least 0.25 MB/s".into());
    }
    // Keep the current on/off state. The Dashboard toggle owns that choice, so
    // a live rate edit cannot race with an enable/disable click.
    let (enabled, _) = state.upload_limiter.settings();
    state.upload_limiter.set(enabled, bytes_per_sec);
    let mut status = state.status.lock();
    status.upload_limit_enabled = enabled;
    status.upload_limit_bytes_per_sec = bytes_per_sec;
    Ok(())
}

fn request_cancel(inner: &AppInner) -> Result<(), String> {
    let mut status = inner.status.lock();
    if !status.running && !status.restoring {
        return Err("nothing is running".into());
    }
    status.cancel_requested = true;
    status.phase = "cancelling".into();
    status.detail = "cancellation requested — finishing the current operation".into();
    drop(status);
    inner.cancel.lock().cancel();
    Ok(())
}

#[tauri::command]
fn cancel_backup(state: State<'_, SharedState>) -> Result<(), String> {
    request_cancel(state.inner().as_ref())
}

#[tauri::command]
fn cancel_power_pause(state: State<'_, SharedState>) -> Result<(), String> {
    let mut status = state.status.lock();
    if !status.power_paused {
        return Err("no paused backup to cancel".into());
    }
    status.power_paused = false;
    status.power_message = state.power.message().unwrap_or_default();
    status.phase = "idle".into();
    status.detail = String::new();
    // The user explicitly chose not to resume. Keep automatic backups paused
    // so the due interval cannot immediately start the same run again.
    status.scheduler_paused = true;
    logging::info("paused backup cancelled; automatic backups remain paused");
    Ok(())
}

#[tauri::command]
async fn list_backups(
    state: State<'_, SharedState>,
) -> Result<Vec<config::BackupListEntry>, String> {
    let inner = state.inner().clone();
    tauri::async_runtime::spawn_blocking(move || {
        let cfg = inner.config.lock().clone();
        config::validate(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        let keys = restore::load_keys(&cfg, store.as_ref(), &cfg.key_root()).map_err(err)?;
        restore::list_backups(
            store.as_ref(),
            &cfg.key_root(),
            &cfg.backup_set_uuid,
            keys.as_ref(),
        )
        .map_err(err)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
async fn storage_estimate(
    state: State<'_, SharedState>,
    source_bytes: Option<u64>,
) -> Result<qia_backup::core::cost::StorageEstimate, String> {
    let inner = state.inner().clone();
    tauri::async_runtime::spawn_blocking(move || {
        let cfg = inner.config.lock().clone();
        config::validate(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        cost::estimate_storage(&cfg, store.as_ref(), source_bytes).map_err(err)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
async fn diff_backup(
    state: State<'_, SharedState>,
    record: String,
    previous: Option<String>,
    limit: Option<usize>,
) -> Result<qia_backup::core::diff::DiffSummary, String> {
    let inner = state.inner().clone();
    tauri::async_runtime::spawn_blocking(move || {
        let cfg = inner.config.lock().clone();
        config::validate(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        diff::diff_backup(
            &cfg,
            store.as_ref(),
            &record,
            previous.as_deref(),
            limit.unwrap_or(diff::DEFAULT_DIFF_LIMIT),
        )
        .map_err(err)
    })
    .await
    .map_err(err)?
}

/// Side-by-side text diff for one file between two records.
#[tauri::command]
async fn diff_backup_file(
    state: State<'_, SharedState>,
    record: String,
    previous: Option<String>,
    path: String,
) -> Result<qia_backup::core::diff::FileDiff, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        diff::diff_file_text(&cfg, store.as_ref(), &record, previous.as_deref(), &path).map_err(err)
    })
    .await
    .map_err(err)?
}

/// Side-by-side text diff of a partial file against the latest completed
/// record. When no completed record exists, every line is shown as added.
#[tauri::command]
async fn diff_partial_file(
    state: State<'_, SharedState>,
    path: String,
) -> Result<qia_backup::core::diff::FileDiff, String> {
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    let partial = state.partial_tree.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let store = new_store(&cfg).map_err(err)?;
        diff::diff_partial_file(&cfg, store.as_ref(), &partial, &path).map_err(err)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
async fn prune_backups(
    state: State<'_, SharedState>,
    dry_run: bool,
) -> Result<qia_backup::core::retention::PruneSummary, String> {
    if !dry_run {
        consume_sensitive_grant(state.inner(), "prune")?;
    }
    let inner = state.inner().clone();
    tauri::async_runtime::spawn_blocking(move || {
        let cfg = inner.config.lock().clone();
        config::validate(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        qia_backup::core::retention::prune(&cfg, store.as_ref(), &CancelToken::new(), dry_run)
            .map_err(err)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
async fn delete_backup_set(state: State<'_, SharedState>) -> Result<usize, String> {
    consume_sensitive_grant(state.inner(), "delete_set")?;
    let inner = state.inner().clone();
    tauri::async_runtime::spawn_blocking(move || {
        let cfg = inner.config.lock().clone();
        config::validate(&cfg).map_err(err)?;
        let store = new_store(&cfg).map_err(err)?;
        qia_backup::core::retention::delete_backup_set(&cfg, store.as_ref()).map_err(err)
    })
    .await
    .map_err(err)?
}

#[tauri::command]
fn restore_backup(
    state: State<'_, SharedState>,
    app: AppHandle,
    record_key: String,
    destination: String,
) -> Result<(), String> {
    {
        let status = state.status.lock();
        if status.running || status.restoring || status.editing {
            return Err("another backup or restore is already running".into());
        }
    }
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    let cancel = CancelToken::new();
    *state.cancel.lock() = cancel.clone();
    {
        let mut status = state.status.lock();
        status.begin("restore", format!("record {record_key}"));
        status.restoring = true;
    }
    let inner = state.inner().clone();
    std::thread::spawn(move || {
        let status_arc = inner.status.clone();
        let app_progress = app.clone();
        let destination = PathBuf::from(destination);
        let result = match new_store(&cfg) {
            Ok(store) => {
                let record = if record_key == "latest" {
                    let key = record::latest_record_key(
                        store.as_ref(),
                        &cfg.key_root(),
                        &cfg.backup_set_uuid,
                    )
                    .map_err(|e| e.to_string())
                    .and_then(|k| k.ok_or_else(|| "no backup records found".to_string()));
                    match key {
                        Ok(key) => restore::restore_backup(
                            &cfg,
                            store.as_ref(),
                            &key,
                            &destination,
                            cancel.clone(),
                            &mut |event| {
                                apply_restore_progress(&status_arc, &event);
                                let _ = app_progress.emit("restore-progress", &event);
                            },
                        ),
                        Err(e) => Err(BackupError::Other(e)),
                    }
                } else {
                    restore::restore_backup(
                        &cfg,
                        store.as_ref(),
                        &record_key,
                        &destination,
                        cancel.clone(),
                        &mut |event| {
                            apply_restore_progress(&status_arc, &event);
                            let _ = app_progress.emit("restore-progress", &event);
                        },
                    )
                };
                record.map_err(|e| e.to_string())
            }
            Err(e) => Err(e.to_string()),
        };
        match result {
            Ok(summary) => {
                {
                    let mut status = inner.status.lock();
                    status.restoring = false;
                    status.running = false;
                    status.phase = "idle".into();
                    status.detail = String::new();
                    status.last_finished_at = Some(chrono::Utc::now());
                }
                let _ = app_progress.emit("restore-finished", &summary);
            }
            Err(message) => {
                inner.status.lock().fail(message.clone());
                let _ = app_progress.emit("restore-failed", message);
            }
        }
    });
    Ok(())
}

/// Restore one file or folder from a specific backup record (or the current
/// backup when `record_key` is empty) into `destination`.
#[tauri::command]
fn restore_backup_entry(
    state: State<'_, SharedState>,
    app: AppHandle,
    record_key: Option<String>,
    path: String,
    destination: String,
) -> Result<(), String> {
    {
        let status = state.status.lock();
        if status.running || status.restoring || status.editing {
            return Err("another backup or restore is already running".into());
        }
    }
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    let cancel = CancelToken::new();
    *state.cancel.lock() = cancel.clone();
    {
        let mut status = state.status.lock();
        status.begin("restore", format!("entry {path}"));
        status.restoring = true;
    }
    let inner = state.inner().clone();
    std::thread::spawn(move || {
        let status_arc = inner.status.clone();
        let app_progress = app.clone();
        let destination = PathBuf::from(destination);
        let result = match new_store(&cfg) {
            Ok(store) => restore::restore_entry(
                &cfg,
                store.as_ref(),
                record_key.as_deref(),
                &path,
                &destination,
                cancel.clone(),
                &mut |event| {
                    apply_restore_progress(&status_arc, &event);
                    let _ = app_progress.emit("restore-progress", &event);
                },
            )
            .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match result {
            Ok(summary) => {
                {
                    let mut status = inner.status.lock();
                    status.restoring = false;
                    status.running = false;
                    status.phase = "idle".into();
                    status.detail = String::new();
                    status.last_finished_at = Some(chrono::Utc::now());
                }
                let _ = app_progress.emit("restore-finished", &summary);
            }
            Err(message) => {
                inner.status.lock().fail(message.clone());
                let _ = app_progress.emit("restore-failed", message);
            }
        }
    });
    Ok(())
}

/// Restore one file or folder from the live partial tree. This is only allowed
/// while no backup is running; after a cancel/error the partial tree remains so
/// the user can still recover already-uploaded content.
#[tauri::command]
fn restore_partial_entry(
    state: State<'_, SharedState>,
    app: AppHandle,
    path: String,
    destination: String,
) -> Result<(), String> {
    {
        let status = state.status.lock();
        if status.running || status.restoring || status.editing {
            return Err("stop or finish the active backup before restoring from it".into());
        }
    }
    let cfg = state.config.lock().clone();
    config::validate(&cfg).map_err(err)?;
    let partial = state.partial_tree.clone();
    if !partial.is_available() {
        return Err("no partial backup data is available".into());
    }
    let cancel = CancelToken::new();
    *state.cancel.lock() = cancel.clone();
    {
        let mut status = state.status.lock();
        status.begin("restore", format!("partial {path}"));
        status.restoring = true;
    }
    let inner = state.inner().clone();
    std::thread::spawn(move || {
        let status_arc = inner.status.clone();
        let app_progress = app.clone();
        let destination = PathBuf::from(destination);
        let result = match new_store(&cfg) {
            Ok(store) => {
                let root = cfg.key_root();
                match record::partial_run_exists(store.as_ref(), &root) {
                    Ok(true) => restore::restore_partial_entry(
                        &cfg,
                        store.as_ref(),
                        &partial,
                        &path,
                        &destination,
                        cancel.clone(),
                        &mut |event| {
                            apply_restore_progress(&status_arc, &event);
                            let _ = app_progress.emit("restore-progress", &event);
                        },
                    )
                    .map_err(|e| e.to_string()),
                    Ok(false) => {
                        partial.clear();
                        status_arc.lock().partial_available = false;
                        Err(
                            "the destination no longer contains this partial backup; local partial data was discarded"
                                .to_string(),
                        )
                    }
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(e.to_string()),
        };
        match result {
            Ok(summary) => {
                {
                    let mut status = inner.status.lock();
                    status.restoring = false;
                    status.running = false;
                    status.phase = "idle".into();
                    status.detail = String::new();
                    status.last_finished_at = Some(chrono::Utc::now());
                }
                let _ = app_progress.emit("restore-finished", &summary);
            }
            Err(message) => {
                inner.status.lock().fail(message.clone());
                let _ = app_progress.emit("restore-failed", message);
            }
        }
    });
    Ok(())
}

fn apply_restore_progress(status: &Arc<Mutex<AppStatus>>, event: &RestoreEvent) {
    let mut status = status.lock();
    match event {
        RestoreEvent::Phase { name, detail } => {
            status.phase = name.clone();
            status.detail = detail.clone();
        }
        RestoreEvent::File { size, .. } => {
            status.files += 1;
            status.bytes_scanned += size;
        }
        RestoreEvent::Warning { .. } => {}
        RestoreEvent::Finished { summary } => {
            status.detail = format!(
                "restored {} files to {}",
                summary.files, summary.destination
            );
        }
    }
}

/// Choose a folder for restores. This keeps the destination editable while
/// avoiding a typed path in the common case.
#[tauri::command]
async fn pick_restore_destination(
    app: AppHandle,
    current: Option<String>,
) -> Result<Option<String>, String> {
    let mut dialog = app
        .dialog()
        .file()
        .set_title("Choose a restore destination");
    if let Some(current) = current
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        dialog = dialog.set_directory(current);
    }
    let Some(picked) = dialog.blocking_pick_folder() else {
        return Ok(None);
    };
    let path = picked.into_path().map_err(err)?;
    Ok(Some(path.to_string_lossy().to_string()))
}

/// Open a native picker and return the selected path relative to the source.
///
/// Included folders only accept directories. Excludes accept files or folders
/// and also return a literal glob-escaped pattern for the exclusion list.
#[tauri::command]
async fn pick_backup_path(
    app: AppHandle,
    state: State<'_, SharedState>,
    kind: String,
    purpose: String,
) -> Result<Option<PickedBackupPath>, String> {
    if purpose != "include" && purpose != "exclude" {
        return Err("unknown picker purpose".into());
    }
    if kind != "file" && kind != "folder" {
        return Err("unknown picker kind".into());
    }
    if purpose == "include" && kind != "folder" {
        return Err("included paths must be folders".into());
    }

    let root = {
        let cfg = state.config.lock();
        if cfg.source_kind == "path" && !cfg.source_path.trim().is_empty() {
            PathBuf::from(cfg.source_path.trim())
        } else {
            PathBuf::from(if cfg.snapshot_volume.trim().is_empty() {
                config::default_snapshot_volume()
            } else {
                cfg.snapshot_volume.clone()
            })
        }
    };

    // Finder presents the Data volume through its normal firmlink paths
    // (/Users, /Applications, ...). Start in the user's home instead of the
    // hidden /System/Volumes/Data mount point.
    let start = if root == PathBuf::from("/System/Volumes/Data") {
        config::home_dir()
    } else {
        root.clone()
    };
    let title = if purpose == "include" {
        "Choose a folder to include"
    } else if kind == "folder" {
        "Choose a folder to exclude"
    } else {
        "Choose a file to exclude"
    };
    let dialog = app.dialog().file().set_title(title).set_directory(start);
    let picked = if kind == "folder" {
        dialog.blocking_pick_folder()
    } else {
        dialog.blocking_pick_file()
    };
    let Some(picked) = picked else {
        return Ok(None);
    };
    let absolute = picked.into_path().map_err(err)?;
    let relative = config::relative_to_source(&root, &absolute).map_err(err)?;
    let pattern = if purpose == "exclude" {
        Some(edit::ignore_pattern_for_path(&relative).map_err(err)?)
    } else {
        None
    };
    Ok(Some(PickedBackupPath {
        absolute: absolute.to_string_lossy().into_owned(),
        relative,
        pattern,
    }))
}

#[tauri::command]
fn list_snapshots(state: State<'_, SharedState>) -> SnapshotResponse {
    let volume = {
        let cfg = state.config.lock();
        if cfg.snapshot_volume.is_empty() {
            config::default_snapshot_volume()
        } else {
            cfg.snapshot_volume.clone()
        }
    };
    SnapshotResponse {
        snapshots: snapshot_names(&volume),
        destinations: snapshot::time_machine_destinations(),
        volume,
    }
}

#[tauri::command]
fn get_logs(lines: Option<usize>) -> Vec<String> {
    logging::tail(lines.unwrap_or(300))
}

#[tauri::command]
fn clear_logs() {
    logging::clear();
}

#[tauri::command]
fn open_config_folder() -> Result<(), String> {
    std::process::Command::new("/usr/bin/open")
        .arg(config::app_support_dir())
        .spawn()
        .map(|_| ())
        .map_err(err)
}

#[tauri::command]
fn open_log() -> Result<(), String> {
    std::process::Command::new("/usr/bin/open")
        .arg(config::log_dir().join("qia-backup.log"))
        .spawn()
        .map(|_| ())
        .map_err(err)
}

#[tauri::command]
fn copy_to_clipboard(text: String) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        use std::io::Write;
        use std::process::{Command, Stdio};
        let mut child = Command::new("/usr/bin/pbcopy")
            .stdin(Stdio::piped())
            .spawn()
            .map_err(err)?;
        if let Some(stdin) = child.stdin.as_mut() {
            stdin.write_all(text.as_bytes()).map_err(err)?;
        }
        child.wait().map_err(err)?;
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = text;
        Err("clipboard is only supported on macOS".into())
    }
}

pub fn run() {
    let config_exists = config::config_path().exists();
    let mut config = config::load().unwrap_or_else(|e| {
        logging::error(format!("could not load configuration: {e}"));
        AppConfig::default()
    });
    // The desktop app is APFS-snapshot-only, including configs written by older
    // versions or the CLI folder mode.
    config.source_kind = "snapshot".into();
    config.source_path.clear();
    // Prefill a new or completely empty config with the recommended selection.
    if !config_exists || (config.included_folders.is_empty() && config.exclude_patterns.is_empty())
    {
        config.included_folders = qia_backup::core::model::default_included_folders();
        config.exclude_patterns = qia_backup::core::model::default_exclude_patterns();
    }
    let upload_limiter = Arc::new(UploadLimiter::new(
        config.upload_limit_enabled,
        config.upload_limit_bytes_per_sec,
    ));
    let mut initial_status = AppStatus::paused();
    initial_status.upload_limit_enabled = config.upload_limit_enabled;
    initial_status.upload_limit_bytes_per_sec = config.upload_limit_bytes_per_sec;
    let shared: SharedState = Arc::new(AppInner {
        config: Mutex::new(config),
        status: Arc::new(Mutex::new(initial_status)),
        cancel: Mutex::new(CancelToken::new()),
        partial_tree: PartialTree::new(),
        sensitive_auth: Mutex::new(None),
        upload_limiter,
        power: crate::power::PowerMonitor::start(),
        power_pause_requested: AtomicBool::new(false),
    });
    let scheduler_inner = shared.clone();
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(shared)
        .invoke_handler(tauri::generate_handler![
            get_state,
            get_status,
            save_config,
            test_connection,
            check_destination,
            set_scheduler_paused,
            set_upload_limit_enabled,
            apply_upload_limit,
            authorize_sensitive_action,
            clear_sensitive_authorization,
            cleanup_pending,
            start_backup,
            cancel_backup,
            cancel_power_pause,
            list_backups,
            list_backup_entries,
            backup_file_versions,
            backup_version_counts,
            list_partial_entries,
            read_backup_file_content,
            read_partial_file_content,
            pick_backup_path,
            pick_restore_destination,
            ignore_backup_entry,
            remove_backup_entry,
            remove_ignored_entries,
            storage_estimate,
            diff_backup,
            diff_backup_file,
            diff_partial_file,
            prune_backups,
            delete_backup_set,
            restore_backup,
            restore_backup_entry,
            restore_partial_entry,
            list_snapshots,
            get_logs,
            clear_logs,
            open_config_folder,
            open_log,
            copy_to_clipboard
        ])
        .setup(move |app| {
            // Tray: closing the window hides it, while Open restores it and
            // Quit terminates the process. The same tray reports progress.
            let open_item = MenuItem::with_id(
                app.handle(),
                "tray_open",
                "Open Qia Backup",
                true,
                None::<&str>,
            )?;
            let progress_item =
                MenuItem::with_id(app.handle(), "tray_progress", "Idle", false, None::<&str>)?;
            let cancel_item = MenuItem::with_id(
                app.handle(),
                "tray_cancel",
                "Cancel backup",
                false,
                None::<&str>,
            )?;
            let auto_item = MenuItem::with_id(
                app.handle(),
                "tray_toggle_auto",
                "Pause automatic backups",
                true,
                None::<&str>,
            )?;
            let upload_limit_item = MenuItem::with_id(
                app.handle(),
                "tray_toggle_upload_limit",
                "Upload limit",
                true,
                None::<&str>,
            )?;
            let separator_top = PredefinedMenuItem::separator(app.handle())?;
            let separator_bottom = PredefinedMenuItem::separator(app.handle())?;
            let quit_item = MenuItem::with_id(
                app.handle(),
                "tray_quit",
                "Quit Qia Backup",
                true,
                None::<&str>,
            )?;
            let tray_menu = Menu::with_items(
                app.handle(),
                &[
                    &open_item,
                    &progress_item,
                    &separator_top,
                    &cancel_item,
                    &auto_item,
                    &upload_limit_item,
                    &separator_bottom,
                    &quit_item,
                ],
            )?;
            let tray_icon = Image::from_bytes(include_bytes!("../icons/32x32.png"))?;
            let tray = TrayIconBuilder::with_id("main")
                .icon(tray_icon)
                .icon_as_template(false)
                .tooltip("Qia Backup")
                .menu(&tray_menu)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "tray_open" => show_main_window(app),
                    "tray_cancel" => {
                        if let Some(inner) = app.try_state::<SharedState>() {
                            if let Err(e) = request_cancel(inner.inner().as_ref()) {
                                logging::debug(format!(
                                    "tray could not cancel the running operation: {e}"
                                ));
                            }
                        }
                    }
                    "tray_toggle_auto" => {
                        if let Some(inner) = app.try_state::<SharedState>() {
                            let paused = inner.status.lock().scheduler_paused;
                            set_scheduler_paused_inner(inner.inner().as_ref(), !paused);
                        }
                    }
                    "tray_toggle_upload_limit" => {
                        if let Some(inner) = app.try_state::<SharedState>() {
                            let enabled = inner.status.lock().upload_limit_enabled;
                            if let Err(e) =
                                set_upload_limit_enabled_inner(inner.inner().as_ref(), !enabled)
                            {
                                logging::debug(format!(
                                    "tray could not toggle the upload limit: {e}"
                                ));
                            }
                        }
                    }
                    "tray_quit" => app.exit(0),
                    _ => {}
                })
                .build(app.handle())?;
            app.manage(TrayState {
                tray,
                progress: progress_item,
                cancel: cancel_item,
                auto: auto_item,
                upload_limit: upload_limit_item,
                _menu: tray_menu,
            });
            let status = app.state::<SharedState>().status.clone();
            spawn_tray_updater(app.handle().clone(), status);
            spawn_power_watcher(app.state::<SharedState>().inner().clone());

            if let Some(window) = app.get_webview_window("main") {
                let app_for_event = app.handle().clone();
                window.on_window_event(move |event| {
                    if let WindowEvent::CloseRequested { api, .. } = event {
                        api.prevent_close();
                        if let Some(window) = app_for_event.get_webview_window("main") {
                            let _ = window.hide();
                        }
                    }
                });
            }

            let handle = app.handle().clone();
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(30));
                    let (paused, retry_at, power_paused) = {
                        let status = scheduler_inner.status.lock();
                        (
                            status.scheduler_paused,
                            status.scheduler_retry_at,
                            status.power_paused,
                        )
                    };
                    if !scheduler_inner.power.can_run() {
                        continue;
                    }
                    if power_paused {
                        if let Err(e) =
                            start_backup_inner(
                                scheduler_inner.clone(),
                                handle.clone(),
                                "resumed",
                                false,
                            )
                        {
                            logging::debug(format!("could not resume powered-off backup: {e}"));
                        }
                        continue;
                    }
                    if paused {
                        continue;
                    }
                    if retry_at.is_some_and(|at| chrono::Utc::now() < at) {
                        continue;
                    }
                    let due = {
                        let cfg = scheduler_inner.config.lock();
                        scheduler::is_due(cfg.last_backup_at, cfg.schedule_minutes)
                    };
                    if due {
                        if let Err(e) = start_backup_inner(
                            scheduler_inner.clone(),
                            handle.clone(),
                            "scheduled",
                            false,
                        ) {
                            logging::debug(format!("scheduler did not start a backup: {e}"));
                        }
                    }
                }
            });
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building Qia Backup")
        .run(|app_handle, event| {
            #[cfg(target_os = "macos")]
            if let RunEvent::Reopen {
                has_visible_windows,
                ..
            } = event
            {
                if !has_visible_windows {
                    show_main_window(app_handle);
                }
            }
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (app_handle, event);
            }
        });
}
