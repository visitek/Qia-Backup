//! Dynamic, shared upload rate limiting.
//!
//! The limiter is a token bucket shared by every upload worker. It can be
//! enabled, disabled, or re-rated while an upload is already streaming, so the
//! Dashboard and Settings changes take effect without restarting the backup.

use std::io::{self, Read};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::core::engine::CancelToken;
use crate::core::error::{BackupError, Result};

/// Default maximum upload throughput: 3 MiB/s.
pub const DEFAULT_UPLOAD_LIMIT_BPS: u64 = 3 * 1024 * 1024;

/// The smallest meaningful rate. A 64 MiB pack still fits comfortably inside
/// the S3 request timeout at this rate.
pub const MIN_UPLOAD_LIMIT_BPS: u64 = 256 * 1024;

/// Tokens are consumed in small slices so cancellation and rate changes are
/// observed promptly instead of after a whole 64 MiB pack has been admitted.
const TOKEN_SLICE: u64 = 64 * 1024;
const MAX_WAIT: Duration = Duration::from_millis(100);
const MIN_WAIT: Duration = Duration::from_millis(1);
/// Window used for the live throughput estimate reported to the UI.
const LIVE_WINDOW_SECS: usize = 5;

struct LimiterState {
    enabled: bool,
    limit_bps: u64,
    tokens: f64,
    last_refill: Instant,
    /// Bytes admitted per whole second, used for the live throughput reading.
    /// Every byte that passes through the limiter is recorded, even when the
    /// limit is disabled, so the UI always reports real upload throughput.
    rate_origin: Instant,
    rate_seconds: [u64; LIVE_WINDOW_SECS],
    rate_bytes: [u64; LIVE_WINDOW_SECS],
}

/// A shared token bucket for upload throttling.
pub struct UploadLimiter {
    state: Mutex<LimiterState>,
    changed: Condvar,
}

impl UploadLimiter {
    pub fn new(enabled: bool, limit_bps: u64) -> Self {
        let now = Instant::now();
        Self {
            state: Mutex::new(LimiterState {
                enabled,
                limit_bps: limit_bps.max(MIN_UPLOAD_LIMIT_BPS),
                // Start empty so enabling a limit cannot produce a large burst.
                tokens: 0.0,
                last_refill: now,
                rate_origin: now,
                rate_seconds: [u64::MAX; LIVE_WINDOW_SECS],
                rate_bytes: [0; LIVE_WINDOW_SECS],
            }),
            changed: Condvar::new(),
        }
    }

    /// Change the limit while a backup is running. Waiters wake immediately
    /// and observe the new state, including when the limit is disabled.
    pub fn set(&self, enabled: bool, limit_bps: u64) {
        let mut state = self.state.lock().expect("upload limiter poisoned");
        let now = Instant::now();
        state.refill(now);
        state.enabled = enabled;
        state.limit_bps = limit_bps.max(MIN_UPLOAD_LIMIT_BPS);
        if !enabled {
            state.tokens = 0.0;
        } else {
            state.tokens = state.tokens.min(bucket_capacity(state.limit_bps));
        }
        state.last_refill = now;
        drop(state);
        self.changed.notify_all();
    }

    pub fn settings(&self) -> (bool, u64) {
        let state = self.state.lock().expect("upload limiter poisoned");
        (state.enabled, state.limit_bps)
    }

    /// Upload throughput actually admitted through the shared limiter over the
    /// last few seconds. This is the authoritative aggregate rate: it cannot
    /// exceed the configured cap and it does not depend on worker count.
    pub fn live_bps(&self) -> u64 {
        let state = self.state.lock().expect("upload limiter poisoned");
        state.live_bps(Instant::now())
    }

    /// Block until `bytes` may be sent. The wait is cancellation-aware and is
    /// split into small slices so a live rate change takes effect promptly.
    pub fn acquire(&self, bytes: u64, cancel: &CancelToken) -> Result<()> {
        let mut remaining = bytes;
        while remaining > 0 {
            if cancel.is_cancelled() {
                return Err(BackupError::Cancelled);
            }
            let slice = remaining.min(TOKEN_SLICE);
            self.acquire_slice(slice, cancel)?;
            remaining -= slice;
        }
        Ok(())
    }

    fn acquire_slice(&self, bytes: u64, cancel: &CancelToken) -> Result<()> {
        let mut state = self.state.lock().expect("upload limiter poisoned");
        loop {
            if cancel.is_cancelled() {
                return Err(BackupError::Cancelled);
            }
            if !state.enabled {
                state.record_live(bytes, Instant::now());
                return Ok(());
            }
            let now = Instant::now();
            state.refill(now);
            let need = bytes as f64;
            if state.tokens >= need {
                state.tokens -= need;
                state.record_live(bytes, now);
                return Ok(());
            }
            let missing = need - state.tokens;
            let wait = Duration::from_secs_f64(missing / state.limit_bps as f64)
                .clamp(MIN_WAIT, MAX_WAIT);
            let (next, _) = self
                .changed
                .wait_timeout(state, wait)
                .expect("upload limiter poisoned");
            state = next;
        }
    }
}

impl LimiterState {
    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.last_refill = now;
        if self.enabled {
            let capacity = bucket_capacity(self.limit_bps);
            self.tokens = (self.tokens + elapsed * self.limit_bps as f64).min(capacity);
        } else {
            self.tokens = 0.0;
        }
    }

    fn record_live(&mut self, bytes: u64, now: Instant) {
        let second = now.saturating_duration_since(self.rate_origin).as_secs();
        let index = (second as usize) % LIVE_WINDOW_SECS;
        if self.rate_seconds[index] != second {
            self.rate_seconds[index] = second;
            self.rate_bytes[index] = 0;
        }
        self.rate_bytes[index] = self.rate_bytes[index].saturating_add(bytes);
    }

    fn live_bps(&self, now: Instant) -> u64 {
        let second = now.saturating_duration_since(self.rate_origin).as_secs();
        let first = second.saturating_sub(LIVE_WINDOW_SECS as u64 - 1);
        let mut bytes = 0u64;
        for index in 0..LIVE_WINDOW_SECS {
            let bucket_second = self.rate_seconds[index];
            if bucket_second != u64::MAX && bucket_second >= first && bucket_second <= second {
                bytes = bytes.saturating_add(self.rate_bytes[index]);
            }
        }
        // Avoid reporting an artificially tiny rate during the first second.
        let elapsed = now.saturating_duration_since(self.rate_origin).as_secs_f64();
        let divisor = elapsed.clamp(1.0, LIVE_WINDOW_SECS as f64);
        (bytes as f64 / divisor) as u64
    }
}

fn bucket_capacity(limit_bps: u64) -> f64 {
    (limit_bps as f64 / 4.0).max(TOKEN_SLICE as f64)
}

/// A `Read` adapter that charges every byte to the shared limiter. `S3Store`
/// uses it for request bodies so the cap applies to bytes on the wire rather
/// than only to job admission.
pub struct ThrottledReader<R> {
    inner: R,
    limiter: Arc<UploadLimiter>,
    cancel: CancelToken,
}

impl<R: Read> ThrottledReader<R> {
    pub fn new(inner: R, limiter: Arc<UploadLimiter>, cancel: CancelToken) -> Self {
        Self {
            inner,
            limiter,
            cancel,
        }
    }
}

impl<R: Read> Read for ThrottledReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.limiter
                .acquire(n as u64, &self.cancel)
                .map_err(|e| io::Error::new(io::ErrorKind::Interrupted, e.to_string()))?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn throttled_reader_limits_streaming_reads() {
        let limiter = Arc::new(UploadLimiter::new(true, 512 * 1024));
        let source = vec![0u8; 128 * 1024];
        let mut reader = ThrottledReader::new(
            Cursor::new(source),
            Arc::clone(&limiter),
            CancelToken::new(),
        );
        let mut output = Vec::new();
        let start = Instant::now();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output.len(), 128 * 1024);
        assert!(
            start.elapsed() >= Duration::from_millis(180),
            "streaming 128 KiB at 512 KiB/s should take roughly 250 ms, took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn disabled_limiter_does_not_wait() {
        let limiter = UploadLimiter::new(false, DEFAULT_UPLOAD_LIMIT_BPS);
        let start = Instant::now();
        limiter.acquire(8 * 1024 * 1024, &CancelToken::new()).unwrap();
        assert!(start.elapsed() < Duration::from_millis(250));
    }

    #[test]
    fn limiter_enforces_a_rate_ceiling() {
        let limiter = UploadLimiter::new(true, 512 * 1024);
        let start = Instant::now();
        limiter
            .acquire(TOKEN_SLICE, &CancelToken::new())
            .unwrap();
        assert!(
            start.elapsed() >= Duration::from_millis(80),
            "a 64 KiB slice at 512 KiB/s should wait roughly 125 ms, waited {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn limiter_can_be_disabled_while_waiting() {
        let limiter = Arc::new(UploadLimiter::new(true, 512 * 1024));
        let worker = {
            let limiter = Arc::clone(&limiter);
            std::thread::spawn(move || {
                limiter.acquire(2 * 1024 * 1024, &CancelToken::new()).unwrap();
            })
        };
        std::thread::sleep(Duration::from_millis(40));
        let start = Instant::now();
        limiter.set(false, 512 * 1024);
        worker.join().unwrap();
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn cancellation_interrupts_a_throttled_wait() {
        let limiter = Arc::new(UploadLimiter::new(true, MIN_UPLOAD_LIMIT_BPS));
        let cancel = CancelToken::new();
        let worker = {
            let limiter = Arc::clone(&limiter);
            let cancel = cancel.clone();
            std::thread::spawn(move || limiter.acquire(16 * 1024 * 1024, &cancel))
        };
        std::thread::sleep(Duration::from_millis(50));
        let start = Instant::now();
        cancel.cancel();
        let result = worker.join().unwrap();
        assert!(matches!(result, Err(BackupError::Cancelled)));
        assert!(start.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn live_rate_reflects_admitted_bytes() {
        let limiter = UploadLimiter::new(false, DEFAULT_UPLOAD_LIMIT_BPS);
        assert_eq!(limiter.live_bps(), 0);
        limiter
            .acquire(256 * 1024, &CancelToken::new())
            .unwrap();
        let bps = limiter.live_bps();
        assert!(
            (128 * 1024..=512 * 1024).contains(&bps),
            "256 KiB in the first second should read as ~256 KiB/s, got {bps}"
        );
    }

    #[test]
    fn live_rate_is_shared_across_waiters() {
        let limit = 512 * 1024;
        let limiter = Arc::new(UploadLimiter::new(true, limit));
        let started = Instant::now();
        let workers: Vec<_> = (0..3)
            .map(|_| {
                let limiter = Arc::clone(&limiter);
                std::thread::spawn(move || {
                    limiter
                        .acquire(256 * 1024, &CancelToken::new())
                        .unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let elapsed = started.elapsed().as_secs_f64();
        let live = limiter.live_bps();
        assert!(
            live <= (limit as f64 * 1.3) as u64,
            "live rate {live} should not exceed the shared limit, took {elapsed:.2}s"
        );
    }

    #[test]
    fn settings_are_updated_live() {
        let limiter = UploadLimiter::new(true, DEFAULT_UPLOAD_LIMIT_BPS);
        assert_eq!(limiter.settings(), (true, DEFAULT_UPLOAD_LIMIT_BPS));
        limiter.set(false, 1024 * 1024);
        assert_eq!(limiter.settings(), (false, 1024 * 1024));
    }
}
