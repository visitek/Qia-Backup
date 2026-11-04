//! Bounded parallel upload worker pool.
//!
//! The backup engine produces uploads on its walk thread. This pool moves the
//! slow `BlobStore::put` calls onto a small set of worker threads so the walk
//! can keep reading, chunking and deduplicating while packs are in flight.
//!
//! Backpressure is byte-based rather than item-based: a job reserves its size
//! against `limit` before it is queued, and the reservation is released by the
//! worker when the upload finishes. That keeps memory bounded even when packs
//! are large.

use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::Scope;
use std::time::{Duration, Instant};

use crate::core::engine::CancelToken;
use crate::core::error::{BackupError, Result};
use crate::core::status::TransferMeter;
use crate::core::store::BlobStore;
use crate::core::throttle::UploadLimiter;

/// Hard cap on upload workers. The pool starts with a small target and scales
/// up only when measured throughput justifies it.
pub const MAX_UPLOAD_THREADS: usize = 16;

/// How often the concurrency controller may change the target.
const SCALE_INTERVAL: Duration = Duration::from_secs(5);
/// Measurement window used for control decisions.
const CONTROL_WINDOW: Duration = Duration::from_secs(30);
/// Time allowed after a probe before comparing per-worker throughput.
const PROBE_SETTLE: Duration = Duration::from_secs(30);
/// Quiet period after a probe so the pool does not oscillate.
const PROBE_COOLDOWN: Duration = Duration::from_secs(60);
/// Below this worker utilization the producer is the bottleneck, not the link.
const UTILIZATION_LOW: f64 = 0.35;
/// Above this utilization every worker is busy and more may help.
const UTILIZATION_HIGH: f64 = 0.75;
/// Keep a probe only if per-worker throughput stays within 15% of the
/// pre-probe baseline. A collapse means the extra workers shared a saturated
/// link instead of adding capacity.
const PROBE_RETENTION: f64 = 0.85;

/// What an upload belongs to, so the engine can update its index when the
/// worker reports success or failure.
#[derive(Debug)]
pub enum UploadKind {
    Pack { path: String, blob_ids: Vec<String> },
    Standalone { blob_id: String },
}

pub struct UploadJob {
    key: String,
    data: Vec<u8>,
    kind: UploadKind,
}

pub struct UploadDone {
    pub key: String,
    pub bytes: u64,
    pub kind: UploadKind,
    pub result: Result<()>,
    /// Worker that ran this transfer (diagnostics / overlap accounting).
    pub worker: usize,
    pub started_at: Instant,
    pub finished_at: Instant,
}

struct Budget {
    limit: usize,
    used: Mutex<usize>,
    ready: Condvar,
}

impl Budget {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            used: Mutex::new(0),
            ready: Condvar::new(),
        }
    }

    /// Reserve `bytes` of in-flight upload budget, waiting while the pool is
    /// full. A single job larger than the whole limit is allowed through so a
    /// large pack can never deadlock the walk.
    fn acquire(&self, bytes: usize) {
        let mut used = self.used.lock().expect("upload budget poisoned");
        while *used > 0 && used.saturating_add(bytes) > self.limit {
            used = self.ready.wait(used).expect("upload budget poisoned");
        }
        *used = used.saturating_add(bytes);
    }

    fn release(&self, bytes: usize) {
        let mut used = self.used.lock().expect("upload budget poisoned");
        *used = used.saturating_sub(bytes);
        self.ready.notify_all();
    }
}

/// Releases a reservation even if an upload panics, so a failing worker can
/// never leave the walk blocked on a permanently exhausted budget.
struct Reservation {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

struct GateState {
    target: usize,
    active: usize,
}

/// Limits how many workers may upload at once. Workers may be blocked on the
/// queue or on this gate; raising the target immediately wakes them.
struct ConcurrencyGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl ConcurrencyGate {
    fn new(target: usize) -> Self {
        Self {
            state: Mutex::new(GateState {
                target: target.max(1),
                active: 0,
            }),
            changed: Condvar::new(),
        }
    }

    fn acquire(&self) {
        let mut state = self.state.lock().expect("upload gate poisoned");
        while state.active >= state.target {
            state = self.changed.wait(state).expect("upload gate poisoned");
        }
        state.active += 1;
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("upload gate poisoned");
        state.active = state.active.saturating_sub(1);
        self.changed.notify_all();
    }

    fn set_target(&self, target: usize) {
        let mut state = self.state.lock().expect("upload gate poisoned");
        state.target = target.max(1);
        self.changed.notify_all();
    }

    fn active(&self) -> usize {
        self.state.lock().expect("upload gate poisoned").active
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UploadPoolStats {
    /// Live aggregate throughput: active workers x measured per-worker rate.
    /// This reflects present activity instead of "objects completed in a fixed
    /// window", so it does not collapse to zero between large packs.
    pub speed_bps: u64,
    /// Aggregate throughput measured over the meter window from attributed
    /// in-flight time.
    pub measured_bps: u64,
    /// Throughput of one worker, measured as bytes / active-worker-seconds.
    pub per_worker_bps: u64,
    /// Fraction of the configured worker capacity that was actually uploading.
    pub utilization: f64,
    pub target_threads: usize,
    pub max_threads: usize,
    pub in_flight: usize,
}

/// Concurrency controller driven by measured per-worker throughput.
///
/// It never derives a worker count from aggregate speed alone: aggregate speed
/// grows simply because more workers run. Instead it probes upward while the
/// pool is saturated, then keeps the extra workers only if per-worker
/// throughput stays close to the pre-probe baseline. If per-worker throughput
/// collapses, the link was already saturated and the probe is reverted.
pub struct ScaleController {
    target: usize,
    min: usize,
    max: usize,
    last_change: Instant,
    cooldown_until: Instant,
    probe: Option<Probe>,
}

#[derive(Clone, Copy)]
struct Probe {
    from: usize,
    baseline_per_worker_bps: u64,
}

impl ScaleController {
    pub fn new(initial: usize, max: usize, now: Instant) -> Self {
        let max = max.max(1);
        Self {
            target: initial.clamp(1, max),
            min: 1,
            max,
            last_change: now,
            cooldown_until: now,
            probe: None,
        }
    }

    pub fn target(&self) -> usize {
        self.target
    }

    /// Feed a measurement and return a new target when it changes.
    pub fn observe(
        &mut self,
        now: Instant,
        per_worker_bps: u64,
        utilization: f64,
    ) -> Option<usize> {
        if let Some(probe) = self.probe {
            if now.duration_since(self.last_change) < PROBE_SETTLE {
                return None;
            }
            let retained = self.target > probe.from
                && probe.baseline_per_worker_bps > 0
                && (per_worker_bps as f64
                    >= probe.baseline_per_worker_bps as f64 * PROBE_RETENTION);
            self.probe = None;
            self.cooldown_until = now + PROBE_COOLDOWN;
            self.last_change = now;
            if retained {
                return None;
            }
            self.target = probe.from;
            return Some(self.target);
        }

        if now < self.cooldown_until || now.duration_since(self.last_change) < SCALE_INTERVAL
        {
            return None;
        }

        // Workers are idle: the walker (not the link) is the bottleneck, so
        // more workers cannot help. Trim clearly over-provisioned targets, but
        // never before the first measurement (utilization is zero then).
        if utilization < UTILIZATION_LOW && per_worker_bps > 0 {
            if self.target > self.min {
                self.target -= 1;
                self.last_change = now;
                return Some(self.target);
            }
            return None;
        }

        // All workers are busy. Probe upward and let the next evaluation decide
        // whether the extra concurrency actually added throughput.
        if utilization >= UTILIZATION_HIGH
            && self.target < self.max
            && per_worker_bps > 0
        {
            self.probe = Some(Probe {
                from: self.target,
                baseline_per_worker_bps: per_worker_bps,
            });
            self.target = (self.target + 2).min(self.max);
            self.last_change = now;
            return Some(self.target);
        }

        None
    }
}

pub struct UploadPool {
    tx: Option<SyncSender<UploadJob>>,
    rx: Receiver<UploadDone>,
    budget: Arc<Budget>,
    in_flight: usize,
    gate: Arc<ConcurrencyGate>,
    max_threads: usize,
    target_threads: usize,
    controller: ScaleController,
    /// Duration-attributed bytes and worker-seconds.
    meter: TransferMeter,
    /// EWMA of a single transfer's bytes/second. Only used as a fallback for
    /// stores that do not expose a shared limiter.
    worker_rate_bps: u64,
    /// The shared limiter, when present. Its live counter is the real
    /// aggregate wire rate, so it is preferred over the per-worker estimate.
    limiter: Option<Arc<UploadLimiter>>,
    measured_bps: u64,
    per_worker_bps: u64,
    utilization: f64,
    last_scale: Instant,
}

impl UploadPool {
    /// Start `threads` workers scoped to `scope`. The workers borrow `store`
    /// and stop when the pool (and therefore its sender) is dropped.
    pub fn start<'scope, 'env>(
        scope: &'scope Scope<'scope, 'env>,
        store: &'env dyn BlobStore,
        cancel: CancelToken,
        threads: usize,
        budget_bytes: usize,
    ) -> Self {
        Self::start_with_target(scope, store, cancel, threads, threads, budget_bytes)
    }

    /// Start with a maximum worker count and a conservative initial
    /// concurrency target. All workers are created up front and gated, so
    /// scaling never requires spawning threads on the hot path.
    pub fn start_with_target<'scope, 'env>(
        scope: &'scope Scope<'scope, 'env>,
        store: &'env dyn BlobStore,
        cancel: CancelToken,
        max_threads: usize,
        initial_threads: usize,
        budget_bytes: usize,
    ) -> Self {
        let max_threads = max_threads.clamp(1, MAX_UPLOAD_THREADS);
        let initial_threads = initial_threads.clamp(1, max_threads);
        let gate = Arc::new(ConcurrencyGate::new(initial_threads));
        let budget = Arc::new(Budget::new(budget_bytes));
        // The byte budget is the real bound; the queue only needs enough room
        // for workers that are waiting on the concurrency gate.
        let (tx, job_rx) = sync_channel::<UploadJob>((max_threads * 2).max(2));
        let (done_tx, rx) = std::sync::mpsc::channel::<UploadDone>();
        let job_rx = Arc::new(Mutex::new(job_rx));
        let limiter = store.upload_limiter();

        for worker in 0..max_threads {
            let job_rx = Arc::clone(&job_rx);
            let done_tx = done_tx.clone();
            let budget = Arc::clone(&budget);
            let gate = Arc::clone(&gate);
            let cancel = cancel.clone();
            scope.spawn(move || {
                loop {
                    let job = {
                        let receiver = job_rx.lock().expect("upload queue poisoned");
                        receiver.recv()
                    };
                    let Ok(job) = job else { break };

                    gate.acquire();
                    let started_at = Instant::now();
                    let bytes = job.data.len();
                    let _reservation = Reservation {
                        budget: Arc::clone(&budget),
                        bytes,
                    };
                    let mut result = if cancel.is_cancelled() {
                        Err(BackupError::Cancelled)
                    } else {
                        // A panic in a store implementation must not take down
                        // the whole app or leave the engine without a result
                        // for this blob. Convert it into an ordinary upload
                        // failure so the engine can abort consistently.
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            store.put(&job.key, &job.data)
                        }))
                        .unwrap_or_else(|payload| {
                            Err(BackupError::Other(format!(
                                "upload worker panicked: {}",
                                panic_message(&*payload)
                            )))
                        })
                    };
                    if cancel.is_cancelled() {
                        // A throttled body can be interrupted mid-request and
                        // surface as an I/O error; report the user's intent.
                        result = Err(BackupError::Cancelled);
                    }
                    let finished_at = Instant::now();
                    gate.release();

                    let done = UploadDone {
                        key: job.key,
                        bytes: bytes as u64,
                        kind: job.kind,
                        result,
                        worker,
                        started_at,
                        finished_at,
                    };
                    if done_tx.send(done).is_err() {
                        break;
                    }
                }
            });
        }
        drop(done_tx);

        let now = Instant::now();
        Self {
            tx: Some(tx),
            rx,
            budget,
            in_flight: 0,
            gate,
            max_threads,
            target_threads: initial_threads,
            controller: ScaleController::new(initial_threads, max_threads, now),
            meter: TransferMeter::new(),
            worker_rate_bps: 0,
            limiter,
            measured_bps: 0,
            per_worker_bps: 0,
            utilization: 0.0,
            last_scale: now,
        }
    }

    /// Queue `data` for upload. This may block until enough in-flight budget
    /// is available, but it never blocks on the actual network transfer.
    pub fn submit(&mut self, key: String, data: Vec<u8>, kind: UploadKind) -> Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| BackupError::Other("upload pool is already closed".into()))?;
        let bytes = data.len();
        self.budget.acquire(bytes);
        if tx
            .send(UploadJob { key, data, kind })
            .map_err(|_| ())
            .is_err()
        {
            self.budget.release(bytes);
            return Err(BackupError::Other(
                "upload workers stopped unexpectedly".into(),
            ));
        }
        self.in_flight += 1;
        Ok(())
    }

    /// Return a completed upload if one is ready, without blocking.
    pub fn try_take(&mut self) -> Option<UploadDone> {
        match self.rx.try_recv() {
            Ok(done) => {
                self.in_flight = self.in_flight.saturating_sub(1);
                self.observe_done(&done);
                Some(done)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.in_flight = 0;
                None
            }
        }
    }

    /// Block until one in-flight upload completes. Returns `None` when there
    /// is nothing left to wait for.
    pub fn wait_one(&mut self) -> Option<UploadDone> {
        if self.in_flight == 0 {
            return None;
        }
        match self.rx.recv() {
            Ok(done) => {
                self.in_flight -= 1;
                self.observe_done(&done);
                Some(done)
            }
            Err(_) => {
                self.in_flight = 0;
                None
            }
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Update the throughput measurement and scale the worker target. Call this
    /// periodically while uploads are running so a long pack does not leave the
    /// target stale.
    pub fn tick(&mut self) {
        self.maybe_scale(Instant::now());
    }

    pub fn stats(&mut self) -> UploadPoolStats {
        let now = Instant::now();
        self.refresh(now);
        let active = self.gate.active();
        // Prefer the shared limiter's real admitted-byte rate. The fallback
        // (active workers x per-worker EWMA) over-reports whenever the pool
        // scales up, because the per-worker rate was sampled at a lower
        // concurrency. That made a shared 3 MB/s cap look like per-worker
        // limits totalling 5-9 MB/s.
        let speed_bps = match &self.limiter {
            Some(limiter) => limiter.live_bps(),
            None => active as u64 * self.worker_rate_bps,
        };
        UploadPoolStats {
            speed_bps,
            measured_bps: self.measured_bps,
            per_worker_bps: self.per_worker_bps,
            utilization: self.utilization,
            target_threads: self.target_threads,
            max_threads: self.max_threads,
            in_flight: active,
        }
    }

    fn observe_done(&mut self, done: &UploadDone) {
        if done.result.is_err() || done.bytes == 0 {
            return;
        }
        let duration = done.finished_at.saturating_duration_since(done.started_at);
        self.meter.record(done.finished_at, done.bytes, duration);
        let seconds = duration.as_secs_f64().max(0.001);
        let rate = (done.bytes as f64 / seconds) as u64;
        // Smooth per-transfer rate; each transfer is one worker's throughput.
        self.worker_rate_bps = if self.worker_rate_bps == 0 {
            rate
        } else {
            ((self.worker_rate_bps as u128 * 3 + rate as u128) / 4) as u64
        };
        self.maybe_scale(done.finished_at);
    }

    fn refresh(&mut self, now: Instant) {
        let sample = self.meter.sample_window(now, CONTROL_WINDOW);
        let window = CONTROL_WINDOW.as_secs_f64();
        self.measured_bps = (sample.bytes as f64 / window) as u64;
        self.per_worker_bps = if sample.worker_seconds > 0.1 {
            (sample.bytes as f64 / sample.worker_seconds) as u64
        } else {
            self.worker_rate_bps
        };
        let target = self.target_threads.max(1) as f64;
        self.utilization = (sample.worker_seconds / (target * window)).clamp(0.0, 1.0);
    }

    fn maybe_scale(&mut self, now: Instant) {
        if now.duration_since(self.last_scale) < SCALE_INTERVAL {
            return;
        }
        self.refresh(now);
        if let Some(next) = self
            .controller
            .observe(now, self.per_worker_bps, self.utilization)
        {
            self.target_threads = next;
            self.gate.set_target(next);
        }
        self.last_scale = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::core::store::{LocalStore, ObjectInfo};

    struct SlowStore {
        inner: LocalStore,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl BlobStore for SlowStore {
        fn put(&self, key: &str, data: &[u8]) -> Result<()> {
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(now, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
            let result = self.inner.put(key, data);
            self.active.fetch_sub(1, Ordering::SeqCst);
            result
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

    #[test]
    fn uploads_run_concurrently_and_drain() {
        let dir = std::env::temp_dir().join(format!("upload-pool-test-{}", uuid::Uuid::new_v4()));
        let store = SlowStore {
            inner: LocalStore::new(&dir).unwrap(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        };

        std::thread::scope(|scope| {
            let mut pool = UploadPool::start(scope, &store, CancelToken::new(), 4, 1 << 20);
            for i in 0..8 {
                pool.submit(
                    format!("blobpacks/{i}"),
                    vec![i as u8; 1024],
                    UploadKind::Standalone {
                        blob_id: format!("id-{i}"),
                    },
                )
                .unwrap();
            }
            assert_eq!(pool.in_flight(), 8);

            let mut done = 0;
            while let Some(item) = pool.wait_one() {
                item.result.unwrap();
                done += 1;
            }
            assert_eq!(done, 8);
            assert_eq!(pool.in_flight(), 0);
        });

        assert!(
            store.max_active.load(Ordering::SeqCst) > 1,
            "expected uploads to overlap, max was {}",
            store.max_active.load(Ordering::SeqCst)
        );
        assert_eq!(store.list("blobpacks/").unwrap().len(), 8);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn controller_probes_up_and_keeps_gain() {
        let t0 = Instant::now();
        let mut controller = ScaleController::new(2, MAX_UPLOAD_THREADS, t0);
        let t1 = t0 + SCALE_INTERVAL + Duration::from_secs(1);
        assert_eq!(
            controller.observe(t1, 1024 * 1024, 0.9),
            Some(4),
            "saturated workers should trigger an upward probe"
        );
        let t2 = t1 + PROBE_SETTLE + Duration::from_secs(1);
        assert_eq!(
            controller.observe(t2, 1024 * 1024, 0.9),
            None,
            "per-worker throughput held, so the extra workers stay"
        );
        assert_eq!(controller.target(), 4);
    }

    #[test]
    fn controller_reverts_when_per_worker_throughput_collapses() {
        let t0 = Instant::now();
        let mut controller = ScaleController::new(2, MAX_UPLOAD_THREADS, t0);
        let t1 = t0 + SCALE_INTERVAL + Duration::from_secs(1);
        assert_eq!(controller.observe(t1, 2 * 1024 * 1024, 0.9), Some(4));
        let t2 = t1 + PROBE_SETTLE + Duration::from_secs(1);
        assert_eq!(
            controller.observe(t2, 512 * 1024, 0.9),
            Some(2),
            "a collapsed per-worker rate means the link was already saturated"
        );
    }

    #[test]
    fn controller_does_not_add_workers_when_the_producer_is_the_bottleneck() {
        let t0 = Instant::now();
        let mut controller = ScaleController::new(4, MAX_UPLOAD_THREADS, t0);
        let t1 = t0 + SCALE_INTERVAL + Duration::from_secs(1);
        assert_eq!(
            controller.observe(t1, 1024 * 1024, 0.1),
            Some(3),
            "idle workers are trimmed instead of adding more"
        );
        let t2 = t1 + SCALE_INTERVAL + Duration::from_secs(1);
        assert_eq!(controller.observe(t2, 1024 * 1024, 0.1), Some(2));
        let t3 = t2 + SCALE_INTERVAL + Duration::from_secs(1);
        assert_eq!(controller.observe(t3, 1024 * 1024, 0.1), Some(1));
    }

    #[test]
    fn controller_respects_the_maximum() {
        let t0 = Instant::now();
        let mut controller = ScaleController::new(15, 16, t0);
        let t1 = t0 + SCALE_INTERVAL + Duration::from_secs(1);
        assert_eq!(controller.observe(t1, 1024 * 1024, 0.9), Some(16));
    }

    #[test]
    fn initial_target_limits_slow_connections() {
        let dir = std::env::temp_dir().join(format!("upload-pool-slow-{}", uuid::Uuid::new_v4()));
        let store = SlowStore {
            inner: LocalStore::new(&dir).unwrap(),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        };

        std::thread::scope(|scope| {
            let mut pool =
                UploadPool::start_with_target(scope, &store, CancelToken::new(), 4, 1, 1 << 20);
            for i in 0..6 {
                pool.submit(
                    format!("blobpacks/slow-{i}"),
                    vec![i as u8; 1024],
                    UploadKind::Standalone {
                        blob_id: format!("slow-{i}"),
                    },
                )
                .unwrap();
            }
            while let Some(item) = pool.wait_one() {
                item.result.unwrap();
            }
            assert_eq!(pool.stats().target_threads, 1);
        });

        assert_eq!(
            store.max_active.load(Ordering::SeqCst),
            1,
            "a slow initial target must not start multiple uploads"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

}
