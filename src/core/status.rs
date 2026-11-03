//! Shared runtime status for the UI and scheduler.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::engine::BackupSummary;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppStatus {
    pub running: bool,
    pub phase: String,
    pub detail: String,
    pub files: u64,
    pub bytes_scanned: u64,
    pub bytes_uploaded: u64,
    /// Totals from the previous backup record; 0 means not known yet.
    #[serde(default)]
    pub total_files: u64,
    #[serde(default)]
    pub total_bytes: u64,
    pub started_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_summary: Option<BackupSummary>,
    pub last_finished_at: Option<DateTime<Utc>>,
    pub restoring: bool,
    /// An exclusive metadata edit (for example removing an entry from a
    /// completed record) is in progress. Backups and restores must wait.
    #[serde(default)]
    pub editing: bool,
    pub cancel_requested: bool,
    /// Automatic backups always start paused when the app launches. The user
    /// can enable the scheduler for the current session.
    #[serde(default)]
    pub scheduler_paused: bool,
    /// Consecutive transient scheduled-backup failures, used for backoff.
    #[serde(default)]
    pub scheduler_failures: u32,
    /// Automatic retries must not start before this time.
    #[serde(default)]
    pub scheduler_retry_at: Option<DateTime<Utc>>,
    /// A live partial tree is available for browsing while a run is active or
    /// after it was interrupted before writing a record.
    #[serde(default)]
    pub partial_available: bool,
    /// Rolling upload throughput measured over the last few seconds.
    #[serde(default)]
    pub upload_speed_bps: u64,
    /// Current and maximum upload worker concurrency chosen by the pool.
    #[serde(default)]
    pub upload_target_threads: u32,
    #[serde(default)]
    pub upload_max_threads: u32,
    /// Uploads currently handed to the worker pool and not yet completed.
    #[serde(default)]
    pub upload_in_flight: u32,
    /// Whether the configured upload rate limit is active.
    #[serde(default)]
    pub upload_limit_enabled: bool,
    /// Configured maximum upload throughput in bytes per second.
    #[serde(default)]
    pub upload_limit_bytes_per_sec: u64,
    /// A backup was paused because the Mac slept or switched to battery. It
    /// will resume automatically after stable power returns.
    #[serde(default)]
    pub power_paused: bool,
    /// Explanation shown while backups are gated by power conditions.
    #[serde(default)]
    pub power_message: String,
}

impl AppStatus {
    /// Status used at app startup: automatic backups are paused.
    pub fn paused() -> Self {
        Self {
            scheduler_paused: true,
            ..Self::default()
        }
    }

    pub fn begin(&mut self, phase: &str, detail: impl Into<String>) {
        self.running = true;
        self.phase = phase.to_string();
        self.detail = detail.into();
        self.files = 0;
        self.bytes_scanned = 0;
        self.bytes_uploaded = 0;
        self.total_files = 0;
        self.total_bytes = 0;
        self.started_at = Some(Utc::now());
        self.last_error = None;
        self.cancel_requested = false;
        self.upload_speed_bps = 0;
        self.upload_target_threads = 0;
        self.upload_max_threads = 0;
        self.upload_in_flight = 0;
        self.power_paused = false;
    }

    pub fn fail(&mut self, message: impl Into<String>) {
        self.running = false;
        self.restoring = false;
        self.editing = false;
        self.last_error = Some(message.into());
        self.last_finished_at = Some(Utc::now());
        self.upload_speed_bps = 0;
        self.upload_in_flight = 0;
    }
}

/// Rolling upload throughput over a fixed window. Upload completions are
/// bursty (a 64 MiB pack can take many seconds), so a 15 second window gives a
/// stable speed without hiding changes for too long.
pub struct TransferMeter {
    samples: std::collections::VecDeque<(std::time::Instant, u64)>,
    speed_bps: u64,
}

impl TransferMeter {
    pub const WINDOW: std::time::Duration = std::time::Duration::from_secs(15);

    pub fn new() -> Self {
        Self {
            samples: std::collections::VecDeque::new(),
            speed_bps: 0,
        }
    }

    pub fn reset(&mut self) {
        self.samples.clear();
        self.speed_bps = 0;
    }

    pub fn record(&mut self, bytes: u64) {
        self.record_at(bytes, std::time::Instant::now());
    }

    pub fn snapshot(&mut self) -> u64 {
        self.snapshot_at(std::time::Instant::now())
    }

    fn record_at(&mut self, bytes: u64, now: std::time::Instant) {
        self.samples.push_back((now, bytes));
        self.recompute(now);
    }

    fn snapshot_at(&mut self, now: std::time::Instant) -> u64 {
        self.recompute(now);
        self.speed_bps
    }

    fn recompute(&mut self, now: std::time::Instant) {
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > Self::WINDOW)
        {
            self.samples.pop_front();
        }
        let bytes: u64 = self.samples.iter().map(|(_, bytes)| *bytes).sum();
        self.speed_bps = (bytes as f64 / Self::WINDOW.as_secs_f64()) as u64;
    }
}

impl Default for TransferMeter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod transfer_tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn rolling_speed_uses_the_window_and_expires() {
        let mut meter = TransferMeter::new();
        let start = Instant::now();
        meter.record_at(15 * 1024 * 1024, start);
        assert_eq!(meter.snapshot_at(start), 1024 * 1024);
        meter.record_at(15 * 1024 * 1024, start + Duration::from_secs(5));
        assert_eq!(
            meter.snapshot_at(start + Duration::from_secs(5)),
            2 * 1024 * 1024
        );
        assert_eq!(meter.snapshot_at(start + Duration::from_secs(21)), 0);
    }

    #[test]
    fn reset_clears_samples_and_speed() {
        let mut meter = TransferMeter::new();
        let start = Instant::now();
        meter.record_at(10 * 1024 * 1024, start);
        meter.reset();
        assert_eq!(meter.snapshot_at(start), 0);
    }
}
