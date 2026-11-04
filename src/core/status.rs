//! Shared runtime status for the UI and scheduler.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::engine::BackupSummary;

/// Number of activities shown before the user expands the feed.
pub const ACTIVITY_VISIBLE_LIMIT: usize = 5;
/// Total distinct activities retained for the expanded feed (5 + 20 more).
pub const ACTIVITY_HISTORY_LIMIT: usize = 25;

/// A timestamped, human-readable description of a fine-grained engine action.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityEntry {
    pub at: DateTime<Utc>,
    pub text: String,
}

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
    /// Measured throughput of a single upload worker.
    #[serde(default)]
    pub upload_per_worker_bps: u64,
    /// Fraction of the worker target that was actually transferring.
    #[serde(default)]
    pub upload_utilization: f64,
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
    /// Last few distinct engine actions, oldest first. Used by the dashboard
    /// activity box so a run that is quiet (for example while reading one huge
    /// file) still has a visible heartbeat.
    #[serde(default)]
    pub recent_activity: Vec<ActivityEntry>,
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
        self.upload_per_worker_bps = 0;
        self.upload_utilization = 0.0;
        self.power_paused = false;
        self.recent_activity.clear();
    }

    pub fn fail(&mut self, message: impl Into<String>) {
        self.running = false;
        self.restoring = false;
        self.editing = false;
        self.last_error = Some(message.into());
        self.last_finished_at = Some(Utc::now());
        self.upload_speed_bps = 0;
        self.upload_in_flight = 0;
        self.upload_per_worker_bps = 0;
        self.upload_utilization = 0.0;
        self.recent_activity.clear();
    }

    /// Record an engine action for the live activity box. Repeated identical
    /// actions refresh the existing entry instead of filling the feed with
    /// duplicates, so a long-running heartbeat stays visible without crowding
    /// out other distinct work.
    pub fn record_activity(&mut self, text: impl Into<String>) {
        self.record_activity_at(Utc::now(), text);
    }

    pub fn record_activity_at(&mut self, at: DateTime<Utc>, text: impl Into<String>) {
        let text = text.into().trim().to_string();
        if text.is_empty() {
            return;
        }
        self.recent_activity.retain(|entry| entry.text != text);
        self.recent_activity.push(ActivityEntry { at, text });
        if self.recent_activity.len() > ACTIVITY_HISTORY_LIMIT {
            let excess = self.recent_activity.len() - ACTIVITY_HISTORY_LIMIT;
            self.recent_activity.drain(0..excess);
        }
    }
}

/// Rolling upload throughput derived from real transfer durations.
///
/// A completion is attributed to the seconds during which the transfer was
/// actually in flight, not to the instant it finished. Counting completions
/// inside a fixed window made a 64 MiB pack appear as a spike followed by
/// nothing, and could not distinguish "slow" from "one large object in
/// flight". Spreading bytes over the transfer duration also lets the pool
/// accumulate active-worker-seconds (concurrency) over the same window.
pub struct TransferMeter {
    buckets: Vec<Bucket>,
    base: std::time::Instant,
    advanced: u64,
}

#[derive(Clone, Copy, Default)]
struct Bucket {
    second: u64,
    bytes: u64,
    worker_micros: u64,
}

/// Bytes moved and worker-time spent in the measurement window.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ThroughputSample {
    pub bytes: u64,
    pub worker_seconds: f64,
}

impl TransferMeter {
    /// Measurement window used for the reported rate.
    pub const WINDOW: std::time::Duration = std::time::Duration::from_secs(60);
    /// Buckets retained so transfers longer than the window are still fully
    /// attributed.
    const BUCKETS: usize = 120;

    pub fn new() -> Self {
        Self {
            buckets: vec![Bucket::default(); Self::BUCKETS],
            base: std::time::Instant::now(),
            advanced: 0,
        }
    }

    pub fn reset(&mut self) {
        self.base = std::time::Instant::now();
        self.advanced = 0;
        for bucket in &mut self.buckets {
            *bucket = Bucket::default();
        }
    }

    fn sec(&self, at: std::time::Instant) -> u64 {
        at.saturating_duration_since(self.base).as_secs()
    }

    fn clear(&mut self, second: u64) {
        let index = (second as usize) % Self::BUCKETS;
        self.buckets[index] = Bucket {
            second,
            bytes: 0,
            worker_micros: 0,
        };
    }

    fn advance(&mut self, at: std::time::Instant) {
        let current = self.sec(at);
        if current <= self.advanced {
            return;
        }
        let first = self.advanced.saturating_add(1);
        if current.saturating_sub(first) >= Self::BUCKETS as u64 {
            let oldest = current.saturating_sub(Self::BUCKETS as u64 - 1);
            for second in oldest..=current {
                self.clear(second);
            }
        } else {
            for second in first..=current {
                self.clear(second);
            }
        }
        self.advanced = current;
    }

    /// Record a transfer that moved `bytes` over `duration`, finishing at `at`.
    /// The bytes are spread evenly over the transfer's actual lifetime.
    pub fn record(&mut self, at: std::time::Instant, bytes: u64, duration: std::time::Duration) {
        if bytes == 0 {
            return;
        }
        self.advance(at);
        let duration_micros = duration.as_micros().max(1) as u64;
        let started = at.checked_sub(duration).unwrap_or(at);
        let window_start = self.advanced.saturating_sub(Self::BUCKETS as u64 - 1);
        let first = self.sec(started).max(window_start);
        let last = self.sec(at);
        if last < first {
            return;
        }
        let mut assigned = 0u64;
        for second in first..=last {
            let index = (second as usize) % Self::BUCKETS;
            if self.buckets[index].second != second {
                continue;
            }
            let bucket_start = self
                .base
                .checked_add(std::time::Duration::from_secs(second))
                .unwrap_or(self.base);
            let bucket_end = bucket_start
                .checked_add(std::time::Duration::from_secs(1))
                .unwrap_or(bucket_start);
            let overlap_start = started.max(bucket_start);
            let overlap_end = at.min(bucket_end);
            if overlap_end <= overlap_start {
                continue;
            }
            let overlap_micros = (overlap_end - overlap_start).as_micros() as u64;
            let remaining = bytes.saturating_sub(assigned);
            let share = if second == last {
                remaining
            } else {
                ((bytes as u128 * overlap_micros as u128) / duration_micros as u128) as u64
            };
            let share = share.min(remaining);
            assigned += share;
            self.buckets[index].bytes += share;
            self.buckets[index].worker_micros += overlap_micros;
        }
    }

    /// Bytes and active-worker-seconds in `window`, ending at `at`.
    pub fn sample_window(
        &mut self,
        at: std::time::Instant,
        window: std::time::Duration,
    ) -> ThroughputSample {
        self.advance(at);
        let current = self.advanced;
        let first = current.saturating_sub(window.as_secs().saturating_sub(1));
        let mut bytes = 0u64;
        let mut worker_micros = 0u64;
        for bucket in &self.buckets {
            if bucket.second >= first && bucket.second <= current {
                bytes += bucket.bytes;
                worker_micros += bucket.worker_micros;
            }
        }
        ThroughputSample {
            bytes,
            worker_seconds: worker_micros as f64 / 1_000_000.0,
        }
    }

    /// Bytes and active-worker-seconds in the default measurement window.
    pub fn sample(&mut self, at: std::time::Instant) -> ThroughputSample {
        self.sample_window(at, Self::WINDOW)
    }

    pub fn speed_bps(&mut self, at: std::time::Instant) -> u64 {
        let sample = self.sample(at);
        (sample.bytes as f64 / Self::WINDOW.as_secs_f64()) as u64
    }

    pub fn snapshot(&mut self) -> u64 {
        self.speed_bps(std::time::Instant::now())
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

    fn at(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    #[test]
    fn duration_is_attributed_across_the_transfer_lifetime() {
        let mut meter = TransferMeter::new();
        let start = Instant::now();
        // 60 MiB uploaded over 30 s = 2 MiB/s per worker. It must not be
        // reported as a 30 MiB/s spike at the completion instant.
        meter.record(at(start, 30), 60 * 1024 * 1024, Duration::from_secs(30));
        let sample = meter.sample(at(start, 30));
        assert_eq!(sample.bytes, 60 * 1024 * 1024);
        assert!((sample.worker_seconds - 30.0).abs() < 0.01);
        let per_worker = (sample.bytes as f64 / sample.worker_seconds) as u64;
        assert_eq!(per_worker, 2 * 1024 * 1024);
        // Partway through the transfer the per-worker rate is already the
        // correct value, even though the completion has not happened yet.
        let partial = meter.sample(at(start, 15));
        assert_eq!(
            (partial.bytes as f64 / partial.worker_seconds) as u64,
            2 * 1024 * 1024
        );
    }

    #[test]
    fn multiple_overlapping_transfers_accumulate_worker_seconds() {
        let mut meter = TransferMeter::new();
        let start = Instant::now();
        meter.record(at(start, 10), 10 * 1024 * 1024, Duration::from_secs(10));
        meter.record(at(start, 10), 10 * 1024 * 1024, Duration::from_secs(10));
        let sample = meter.sample(at(start, 10));
        assert_eq!(sample.bytes, 20 * 1024 * 1024);
        // Two workers ran for 10 s each.
        assert!((sample.worker_seconds - 20.0).abs() < 0.01);
        // Aggregate 2 MiB/s, per worker 1 MiB/s.
        let per_worker = sample.bytes as f64 / sample.worker_seconds;
        assert!((per_worker - 1024.0 * 1024.0).abs() < 1.0);
    }

    #[test]
    fn stale_worker_time_expires_from_the_window() {
        let mut meter = TransferMeter::new();
        let start = Instant::now();
        meter.record(at(start, 10), 10 * 1024 * 1024, Duration::from_secs(10));
        assert!(meter.sample(at(start, 10)).worker_seconds > 0.0);
        // More than the window later there is no measured activity.
        let sample = meter.sample(at(start, 90));
        assert_eq!(sample.bytes, 0);
        assert_eq!(sample.worker_seconds, 0.0);
    }

    #[test]
    fn reset_clears_measurements() {
        let mut meter = TransferMeter::new();
        let start = Instant::now();
        meter.record(at(start, 10), 10 * 1024 * 1024, Duration::from_secs(10));
        meter.reset();
        let sample = meter.sample(Instant::now());
        assert_eq!(sample.bytes, 0);
        assert_eq!(sample.worker_seconds, 0.0);
    }
}

#[cfg(test)]
mod activity_tests {
    use super::*;
    use chrono::Duration;

    fn texts(status: &AppStatus) -> Vec<&str> {
        status
            .recent_activity
            .iter()
            .map(|entry| entry.text.as_str())
            .collect()
    }

    #[test]
    fn keeps_the_latest_history_for_the_expanded_feed() {
        let base = Utc::now();
        let mut status = AppStatus::default();
        for i in 0..(ACTIVITY_HISTORY_LIMIT + 5) {
            status.record_activity_at(
                base + Duration::seconds(i as i64),
                format!("activity number {i}"),
            );
        }
        assert_eq!(status.recent_activity.len(), ACTIVITY_HISTORY_LIMIT);
        assert_eq!(status.recent_activity[0].text, "activity number 5");
        assert_eq!(
            status.recent_activity.last().unwrap().text,
            format!("activity number {}", ACTIVITY_HISTORY_LIMIT + 4)
        );
        assert!(
            ACTIVITY_HISTORY_LIMIT >= ACTIVITY_VISIBLE_LIMIT + 20,
            "the history must be able to hold 5 visible plus 20 expanded entries"
        );
    }

    #[test]
    fn repeated_activity_refreshes_instead_of_duplicating() {
        let base = Utc::now();
        let mut status = AppStatus::default();
        status.record_activity_at(base, "Reading huge.dmg");
        status.record_activity_at(base + Duration::seconds(1), "Uploading pack");
        status.record_activity_at(base + Duration::seconds(2), "Reading huge.dmg");
        assert_eq!(texts(&status), vec!["Uploading pack", "Reading huge.dmg"]);
        assert_eq!(
            status.recent_activity.last().unwrap().at,
            base + Duration::seconds(2)
        );
    }

    #[test]
    fn older_activities_stay_available_for_the_expanded_feed() {
        let base = Utc::now();
        let mut status = AppStatus::default();
        status.record_activity_at(base, "old activity");
        status.record_activity_at(base + Duration::seconds(60), "new activity");
        assert_eq!(texts(&status), vec!["old activity", "new activity"]);
    }

    #[test]
    fn empty_activity_is_ignored_and_begin_clears_the_feed() {
        let base = Utc::now();
        let mut status = AppStatus::default();
        status.record_activity_at(base, "   ");
        assert!(status.recent_activity.is_empty());
        status.record_activity_at(base, "working");
        status.begin("scanning", "source");
        assert!(status.recent_activity.is_empty());
    }
}
