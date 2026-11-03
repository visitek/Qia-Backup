//! Command-line interface (`qia-backupctl`) and the non-macOS entry point.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use serde_json::json;

use crate::core::config;
use crate::core::diff;
use crate::core::engine::{CancelToken, ProgressEvent, run_backup};
use crate::core::error::{BackupError, Result};
use crate::core::logging;
use crate::core::model::{AppConfig, DEFAULT_REGION};
use crate::core::preflight;
use crate::core::restore::{self, RestoreEvent};
use crate::core::scheduler;
use crate::core::secrets;
use crate::core::snapshot;
use crate::core::store::{BlobStore, new_store_with_upload_throttle};
use crate::core::throttle::UploadLimiter;

#[derive(Parser, Debug)]
#[command(
    name = "qia-backupctl",
    version,
    about = "Qia Backup: Time Machine / APFS snapshot backups to AWS S3"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Configure the S3 destination and source, and test connectivity.
    Init(InitArgs),
    /// Inspect the stored configuration.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Test S3 credentials and bucket access.
    Test,
    /// Run a backup now.
    Backup(BackupArgs),
    /// List backup records.
    List {
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// List APFS local snapshots and Time Machine destinations.
    Snapshots,
    /// Show files that changed between a backup record and its predecessor.
    Diff(DiffArgs),
    /// Restore a backup record.
    Restore(RestoreArgs),
    /// Apply retention and garbage-collect unreferenced objects.
    Prune {
        #[arg(long)]
        dry_run: bool,
    },
    /// Delete all objects in this backup set (does not delete the bucket).
    DeleteSet {
        #[arg(long)]
        yes: bool,
    },
    /// Run the scheduler in the foreground.
    Daemon {
        #[arg(long)]
        interval: Option<u64>,
    },
    /// Print version and configuration paths.
    Info,
}

#[derive(Subcommand, Debug)]
enum ConfigCommand {
    Show,
    Path,
}

#[derive(Args, Debug, Clone, Default)]
struct S3Args {
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long)]
    region: Option<String>,
    #[arg(long)]
    prefix: Option<String>,
    #[arg(long)]
    access_key: Option<String>,
    #[arg(long)]
    secret_key: Option<String>,
}

#[derive(Args, Debug, Clone)]
struct InitArgs {
    #[command(flatten)]
    s3: S3Args,
    #[arg(long)]
    name: Option<String>,
    /// Source directory (path mode). Omit to back up an APFS local snapshot.
    #[arg(long)]
    source: Option<String>,
    /// APFS volume to snapshot (default /System/Volumes/Data).
    #[arg(long)]
    snapshot_volume: Option<String>,
    /// Force snapshot mode even if a source path was previously configured.
    #[arg(long)]
    snapshot: bool,
    /// Back up only this folder (relative to the source). Repeat for multiple folders.
    #[arg(long = "include", value_name = "RELATIVE_PATH")]
    include: Vec<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    no_encryption: bool,
    /// Backup interval in minutes (0 = manual only).
    #[arg(long)]
    schedule: Option<u64>,
    /// Generate a new backup-set UUID (do not attach to an existing set).
    #[arg(long)]
    new_set: bool,
    /// Attach to an existing backup-set UUID on another machine.
    #[arg(long, value_name = "UUID", conflicts_with = "new_set")]
    set_uuid: Option<String>,
    #[arg(long)]
    no_test: bool,
}

#[derive(Args, Debug)]
struct BackupArgs {
    #[command(flatten)]
    s3: S3Args,
    #[arg(long)]
    source: Option<String>,
    /// Use an APFS local snapshot instead of a source path.
    #[arg(long)]
    snapshot: bool,
    #[arg(long)]
    snapshot_volume: Option<String>,
    /// Back up only this folder (relative to the source). Repeat for multiple folders.
    #[arg(long = "include", value_name = "RELATIVE_PATH")]
    include: Vec<String>,
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    no_prune: bool,
    #[arg(long)]
    verbose: bool,
}

#[derive(Args, Debug)]
struct DiffArgs {
    /// Record key, record name, epoch, or "latest".
    #[arg(long, default_value = "latest")]
    record: String,
    /// Record to compare against. Defaults to the previous record; use "none" to list everything as added.
    #[arg(long)]
    previous: Option<String>,
    /// Print the full result as JSON.
    #[arg(long)]
    json: bool,
    /// Maximum number of entries to return (0 = unlimited).
    #[arg(long, default_value_t = diff::DEFAULT_DIFF_LIMIT)]
    limit: usize,
}

#[derive(Args, Debug)]
struct RestoreArgs {
    /// Record key or "latest".
    #[arg(long, default_value = "latest")]
    record: String,
    #[arg(long)]
    dest: Option<PathBuf>,
}

pub fn run() {
    let cli = Cli::parse();
    if let Err(e) = run_inner(cli) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn env_fill(config: &mut AppConfig) {
    if config.access_key_id.is_empty() {
        if let Ok(v) = std::env::var("AWS_ACCESS_KEY_ID") {
            config.access_key_id = v;
        }
    }
    if config.secret_access_key.is_empty() {
        if let Ok(v) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            config.secret_access_key = v;
        }
    }
    if config.region == DEFAULT_REGION {
        if let Ok(v) = std::env::var("AWS_REGION").or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        {
            config.region = v;
        }
    }
    if config.bucket.is_empty() {
        if let Ok(v) = std::env::var("BACKUP_BUCKET") {
            config.bucket = v;
        }
    }
    if config.prefix == "qia-backups" {
        if let Ok(v) = std::env::var("BACKUP_PREFIX") {
            config.prefix = v;
        }
    }
}

fn apply_s3(config: &mut AppConfig, args: &S3Args) {
    if let Some(v) = &args.bucket {
        config.bucket = v.clone();
    }
    if let Some(v) = &args.region {
        config.region = v.clone();
    }
    if let Some(v) = &args.prefix {
        config.prefix = v.clone();
    }
    if let Some(v) = &args.access_key {
        config.access_key_id = v.clone();
    }
    if let Some(v) = &args.secret_key {
        config.secret_access_key = v.clone();
    }
}

fn load_ready() -> Result<AppConfig> {
    let mut config = config::load()?;
    env_fill(&mut config);
    Ok(config)
}

fn store_for(config: &AppConfig) -> Result<Box<dyn BlobStore>> {
    let limiter = Arc::new(UploadLimiter::new(
        config.upload_limit_enabled,
        config.upload_limit_bytes_per_sec,
    ));
    new_store_with_upload_throttle(config, limiter, CancelToken::new())
}

fn run_inner(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Init(args) => cmd_init(args),
        Commands::Config { command } => match command {
            ConfigCommand::Show => cmd_config_show(),
            ConfigCommand::Path => {
                println!("{}", config::config_path().display());
                Ok(())
            }
        },
        Commands::Test => cmd_test(),
        Commands::Backup(args) => cmd_backup(args),
        Commands::List { json, limit } => cmd_list(json, limit),
        Commands::Snapshots => cmd_snapshots(),
        Commands::Diff(args) => cmd_diff(args),
        Commands::Restore(args) => cmd_restore(args),
        Commands::Prune { dry_run } => cmd_prune(dry_run),
        Commands::DeleteSet { yes } => cmd_delete_set(yes),
        Commands::Daemon { interval } => cmd_daemon(interval),
        Commands::Info => cmd_info(),
    }
}

fn cmd_init(args: InitArgs) -> Result<()> {
    let mut config = config::load()?;
    env_fill(&mut config);
    apply_s3(&mut config, &args.s3);
    let original_uuid = config.backup_set_uuid.clone();
    let password_supplied = args.password.is_some();
    if let Some(name) = args.name {
        config.backup_set_name = name;
    }
    if args.new_set {
        config.backup_set_uuid = uuid::Uuid::new_v4().to_string();
    }
    if let Some(set_uuid) = args.set_uuid {
        config.backup_set_uuid = set_uuid;
    }
    if let Some(source) = args.source {
        config.source_path = source;
        config.source_kind = "path".to_string();
    } else if args.snapshot || config.source_path.is_empty() {
        config.source_kind = "snapshot".to_string();
    }
    if let Some(volume) = args.snapshot_volume {
        config.snapshot_volume = volume;
    }
    if !args.include.is_empty() {
        config.included_folders = args.include;
    }
    if let Some(password) = args.password {
        config.encryption_enabled = true;
        config.encryption_password = password;
    }
    if args.no_encryption {
        config.encryption_enabled = false;
        config.encryption_password.clear();
    }
    if config.backup_set_uuid != original_uuid && !password_supplied && !args.no_encryption {
        // Never reuse a password from a different backup set.
        config.encryption_password.clear();
    }
    if let Some(schedule) = args.schedule {
        config.schedule_minutes = schedule;
    }
    config::validate(&config)?;
    config::validate_source(&config)?;
    config::validate_backup_selection(&config)?;

    // Check the destination before persisting secrets, so a wrong password or
    // incompatible encryption setting is never written to the Keychain.
    let connection_message = if args.no_test {
        None
    } else {
        let store = store_for(&config)?;
        store.check_access()?;
        let check = preflight::check(&config, store.as_ref())?;
        if !check.can_backup {
            return Err(BackupError::Config(check.message));
        }
        Some(check.message)
    };

    if args.no_encryption {
        secrets::delete_encryption_password(&config.backup_set_uuid)?;
    }
    // On macOS these go to the login Keychain; `config.json` only keeps
    // non-secret settings.
    secrets::store(&config)?;
    config::save(&config)?;
    println!("configuration saved to {}", config::config_path().display());
    println!("backup set: {}", config.key_root());
    println!("destination: {}", config.destination());
    println!("source: {}", config.source_description());
    if let Some(message) = connection_message {
        println!("S3 connection OK: {message}");
    }
    println!("\nNext: run `qia-backupctl backup`");
    Ok(())
}

fn cmd_config_show() -> Result<()> {
    let config = config::load()?;
    let mut value = serde_json::to_value(&config)?;
    if let Some(obj) = value.as_object_mut() {
        if obj
            .get("secretAccessKey")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
        {
            obj.insert("secretAccessKey".into(), json!("***redacted***"));
        }
        if obj
            .get("encryptionPassword")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
        {
            obj.insert("encryptionPassword".into(), json!("***redacted***"));
        }
    }
    println!("{}", serde_json::to_string_pretty(&value)?);
    println!("\n# config file: {}", config::config_path().display());
    Ok(())
}

fn cmd_test() -> Result<()> {
    let config = load_ready()?;
    config::validate_connection(&config)?;
    let store = store_for(&config)?;
    let check = preflight::check(&config, store.as_ref())?;
    if !check.can_backup {
        return Err(BackupError::Config(check.message));
    }
    store.check_access()?;
    println!("S3 OK: {} ({})", check.message, config.key_root());
    Ok(())
}

fn cmd_backup(args: BackupArgs) -> Result<()> {
    let mut config = load_ready()?;
    apply_s3(&mut config, &args.s3);
    let secrets_supplied = args.s3.secret_key.is_some() || args.password.is_some();
    if let Some(source) = args.source {
        config.source_path = source;
        config.source_kind = "path".to_string();
    }
    if args.snapshot {
        config.source_kind = "snapshot".to_string();
    }
    if let Some(volume) = args.snapshot_volume {
        config.snapshot_volume = volume;
    }
    if !args.include.is_empty() {
        config.included_folders = args.include;
    }
    if let Some(password) = args.password {
        config.encryption_password = password;
        config.encryption_enabled = true;
    }
    config::validate_connection(&config)?;
    config::validate_source(&config)?;
    config::validate_backup_selection(&config)?;
    let store = store_for(&config)?;
    let check = preflight::check(&config, store.as_ref())?;
    if !check.can_backup {
        return Err(BackupError::Config(check.message));
    }
    if secrets_supplied {
        // Persist to the macOS Keychain only after the password/keyset check,
        // so a typo cannot overwrite a working password.
        secrets::store(&config)?;
    }
    if check.resumable {
        println!("resuming an interrupted backup from its last checkpoint");
    }

    let mut last_print = std::time::Instant::now();
    let verbose = args.verbose;
    let mut total_files = 0u64;
    let summary = run_backup(
        &mut config,
        store.as_ref(),
        CancelToken::new(),
        &mut |event| match event {
            ProgressEvent::Phase { name, detail } => println!("[{name}] {detail}"),
            ProgressEvent::Totals { files, bytes } => {
                total_files = files;
                if verbose && (files > 0 || bytes > 0) {
                    println!("  estimated {files} files, {bytes} bytes from the previous backup");
                }
            }
            ProgressEvent::File {
                files,
                bytes_scanned,
                ..
            } => {
                if verbose || last_print.elapsed() >= Duration::from_secs(5) {
                    if total_files > 0 {
                        println!("  {files}/{total_files} files, {bytes_scanned} bytes read");
                    } else {
                        println!("  {files} files, {bytes_scanned} bytes read");
                    }
                    last_print = std::time::Instant::now();
                }
            }
            ProgressEvent::Upload { key, bytes } => {
                if verbose {
                    println!("  uploaded {key} ({bytes} bytes)");
                }
            }
            ProgressEvent::Transfer {
                speed_bps,
                target_threads,
                max_threads,
                in_flight,
            } => {
                if verbose {
                    println!(
                        "  transfer {:.1} MB/s, {in_flight}/{target_threads} active (max {max_threads})",
                        speed_bps as f64 / (1024.0 * 1024.0)
                    );
                }
            }
            ProgressEvent::Warning { message } => eprintln!("warning: {message}"),
            ProgressEvent::Finished { .. } => {}
        },
    )?;
    config::save(&config)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    if !args.no_prune && !config.retain_all {
        match crate::core::retention::prune(&config, store.as_ref(), &CancelToken::new(), false) {
            Ok(prune) => println!(
                "pruned: {} records kept, {} deleted, {} objects deleted",
                prune.records_kept, prune.records_deleted, prune.objects_deleted
            ),
            Err(e) => eprintln!("warning: prune failed: {e}"),
        }
    }
    if summary.errors > 0 {
        eprintln!("backup finished with {} file error(s)", summary.errors);
        std::process::exit(2);
    }
    Ok(())
}

fn cmd_list(json_output: bool, limit: usize) -> Result<()> {
    let config = load_ready()?;
    config::validate(&config)?;
    let store = store_for(&config)?;
    let keys = restore::load_keys(&config, store.as_ref(), &config.key_root())?;
    let mut backups = restore::list_backups(
        store.as_ref(),
        &config.key_root(),
        &config.backup_set_uuid,
        keys.as_ref(),
    )?;
    backups.truncate(limit);
    if json_output {
        println!("{}", serde_json::to_string_pretty(&backups)?);
    } else if backups.is_empty() {
        println!("no backups found");
    } else {
        for entry in &backups {
            println!(
                "{}  {:>6} files  {:>12} bytes  {:<7}  {}",
                entry.timestamp.format("%Y-%m-%d %H:%M:%S"),
                entry.files_count.unwrap_or(0),
                entry.bytes_uploaded.unwrap_or(0),
                match entry.is_complete {
                    Some(true) => "complete",
                    Some(false) => "errors",
                    None => "unknown",
                },
                entry.key
            );
        }
    }
    Ok(())
}

fn cmd_snapshots() -> Result<()> {
    let config = config::load()?;
    let volume = if config.snapshot_volume.is_empty() {
        config::default_snapshot_volume()
    } else {
        config.snapshot_volume
    };
    let snapshots = snapshot::list_local_snapshots(&volume)?;
    if snapshots.is_empty() {
        println!("no local snapshots on {volume}");
    } else {
        println!("local snapshots on {volume}:");
        for snapshot in snapshots {
            println!("  {}", snapshot.name);
        }
    }
    let destinations = snapshot::time_machine_destinations();
    if destinations.is_empty() {
        println!("no mounted Time Machine destinations");
    } else {
        println!("Time Machine destinations:");
        for dest in destinations {
            println!("  {dest}");
        }
    }
    Ok(())
}

fn cmd_diff(args: DiffArgs) -> Result<()> {
    let config = load_ready()?;
    config::validate(&config)?;
    let store = store_for(&config)?;
    let summary = diff::diff_backup(
        &config,
        store.as_ref(),
        &args.record,
        args.previous.as_deref(),
        args.limit,
    )?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }

    match (summary.record_timestamp, summary.previous_timestamp) {
        (Some(current), Some(previous)) => println!(
            "Comparing {} with {}",
            current.format("%Y-%m-%d %H:%M:%S"),
            previous.format("%Y-%m-%d %H:%M:%S")
        ),
        (Some(current), None) => println!(
            "No previous record; showing everything in {} as added",
            current.format("%Y-%m-%d %H:%M:%S")
        ),
        _ => println!("Comparing {}", summary.record_key),
    }
    println!(
        "{} added, {} modified, {} deleted",
        summary.added, summary.modified, summary.deleted
    );
    for entry in &summary.entries {
        let size = match entry.change {
            diff::ChangeKind::Modified => entry
                .previous_size
                .map(|previous| format!("{} (was {previous})", entry.size))
                .unwrap_or_else(|| entry.size.to_string()),
            _ => entry.size.to_string(),
        };
        println!(
            "{} {:<9} {:>18}  {}",
            entry.change.marker(),
            entry.kind.as_str(),
            size,
            entry.path
        );
    }
    if summary.truncated {
        println!(
            "(truncated at {} entries; increase --limit or use --json)",
            args.limit
        );
    } else if summary.entries.is_empty() {
        println!("no changes");
    }
    Ok(())
}

fn cmd_restore(args: RestoreArgs) -> Result<()> {
    let config = load_ready()?;
    config::validate(&config)?;
    let store = store_for(&config)?;
    let destination = args
        .dest
        .unwrap_or_else(restore::default_restore_destination);
    let cancel = CancelToken::new();
    let mut last_print = std::time::Instant::now();
    let summary = if args.record == "latest" {
        restore::restore_latest(
            &config,
            store.as_ref(),
            &destination,
            cancel,
            &mut |event| match event {
                RestoreEvent::Phase { name, detail } => println!("[{name}] {detail}"),
                RestoreEvent::File { path, size } => {
                    if last_print.elapsed() >= Duration::from_secs(2) {
                        println!("  {path} ({size} bytes)");
                        last_print = std::time::Instant::now();
                    }
                }
                RestoreEvent::Warning { message } => eprintln!("warning: {message}"),
                RestoreEvent::Finished { .. } => {}
            },
        )?
    } else {
        restore::restore_backup(
            &config,
            store.as_ref(),
            &args.record,
            &destination,
            cancel,
            &mut |event| match event {
                RestoreEvent::Phase { name, detail } => println!("[{name}] {detail}"),
                RestoreEvent::Warning { message } => eprintln!("warning: {message}"),
                _ => {}
            },
        )?
    };
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn cmd_prune(dry_run: bool) -> Result<()> {
    let config = load_ready()?;
    config::validate(&config)?;
    let store = store_for(&config)?;
    let summary =
        crate::core::retention::prune(&config, store.as_ref(), &CancelToken::new(), dry_run)?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn cmd_delete_set(yes: bool) -> Result<()> {
    let config = load_ready()?;
    config::validate(&config)?;
    let store = store_for(&config)?;
    println!("This will delete every object under {}.", config.key_root());
    if !yes {
        print!("Type DELETE to continue: ");
        std::io::stdout().flush().ok();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).ok();
        if input.trim() != "DELETE" {
            println!("cancelled");
            return Ok(());
        }
    }
    let deleted = crate::core::retention::delete_backup_set(&config, store.as_ref())?;
    println!("deleted {deleted} objects");
    Ok(())
}

fn cmd_daemon(interval: Option<u64>) -> Result<()> {
    let mut config = load_ready()?;
    if let Some(interval) = interval {
        config.schedule_minutes = interval;
    }
    if config.schedule_minutes == 0 {
        return Err(BackupError::Config(
            "schedule is 0 (manual); pass --interval to run the daemon".into(),
        ));
    }
    logging::info(format!(
        "daemon started ({})",
        scheduler::human_interval(config.schedule_minutes)
    ));
    loop {
        let mut current = match config::load() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("could not reload config: {e}");
                config.clone()
            }
        };
        env_fill(&mut current);
        if scheduler::is_due(current.last_backup_at, current.schedule_minutes) {
            let store = match store_for(&current) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("store error: {e}");
                    std::thread::sleep(Duration::from_secs(30));
                    continue;
                }
            };
            let mut next = current.clone();
            match run_backup(
                &mut next,
                store.as_ref(),
                CancelToken::new(),
                &mut |event| {
                    if let ProgressEvent::Phase { name, detail } = event {
                        logging::info(format!("[{name}] {detail}"));
                    }
                },
            ) {
                Ok(summary) => {
                    if let Err(e) = config::save(&next) {
                        eprintln!("could not save config: {e}");
                    }
                    logging::info(format!(
                        "backup finished: {} files, {} bytes",
                        summary.files, summary.bytes_uploaded
                    ));
                    if let Err(e) = crate::core::retention::prune(
                        &next,
                        store.as_ref(),
                        &CancelToken::new(),
                        false,
                    ) {
                        logging::warn(format!("prune failed: {e}"));
                    }
                }
                Err(e) => logging::error(format!("backup failed: {e}")),
            }
        }
        std::thread::sleep(Duration::from_secs(30));
    }
}

fn cmd_info() -> Result<()> {
    let config = config::load()?;
    println!("Qia Backup {}", env!("CARGO_PKG_VERSION"));
    println!("config: {}", config::config_path().display());
    println!(
        "logs:   {}",
        config::log_dir().join("qia-backup.log").display()
    );
    println!("cache:  {}", config::cache_dir().display());
    println!("destination: {}", config.destination());
    println!("source: {}", config.source_description());
    println!(
        "schedule: {}",
        scheduler::human_interval(config.schedule_minutes)
    );
    println!(
        "last backup: {}",
        config
            .last_backup_at
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "never".to_string())
    );
    Ok(())
}
