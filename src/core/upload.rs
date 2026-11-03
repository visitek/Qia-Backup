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

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::Scope;
use std::time::{Duration, Instant};

use crate::core::engine::CancelToken;
use crate::core::error::{BackupError, Result};
use crate::core::store::BlobStore;

/// Hard cap on upload workers. The pool starts with a small target and scales
/// up only when measured throughput justifies it.
pub const MAX_UPLOAD_THREADS: usize = 16;

const SCALE_INTERVAL: Duration = Duration::from_secs(5);
const SPEED_WINDOW: Duration = Duration::from_secs(15);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadPoolStats {
    pub speed_bps: u64,
    pub target_threads: usize,
    pub max_threads: usize,
    pub in_flight: usize,
}

/// Map measured upload throughput to a useful worker count.
///
/// 5 MB/s lands on four workers, matching four 64 MiB packs in roughly a
/// minute. Slow links stay at one or two workers; fast links can climb to the
/// configured maximum.
pub fn desired_threads(speed_bps: u64, max_threads: usize) -> usize {
    let mb = speed_bps as f64 / (1024.0 * 1024.0);
    let desired = if mb < 0.5 {
        1
    } else if mb < 1.5 {
        2
    } else if mb < 3.0 {
        3
    } else if mb < 7.0 {
        4
    } else if mb < 12.0 {
        6
    } else if mb < 20.0 {
        8
    } else if mb < 40.0 {
        12
    } else {
        MAX_UPLOAD_THREADS
    };
    desired.clamp(1, max_threads.max(1))
}

/// Move the current target toward the desired one with hysteresis: add at most
/// two workers per scale window, remove at most one. This avoids oscillating
/// around a noisy throughput reading.
pub fn next_target(current: usize, desired: usize, max_threads: usize) -> usize {
    let next = if desired > current {
        (current + 2).min(desired)
    } else if desired < current {
        current.saturating_sub(1).max(1)
    } else {
        current
    };
    next.clamp(1, max_threads.max(1))
}

pub struct UploadPool {
    tx: Option<SyncSender<UploadJob>>,
    rx: Receiver<UploadDone>,
    budget: Arc<Budget>,
    in_flight: usize,
    gate: Arc<ConcurrencyGate>,
    max_threads: usize,
    target_threads: usize,
    speed_bps: u64,
    samples: VecDeque<(Instant, u64)>,
    last_completion: Option<Instant>,
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

        for _ in 0..max_threads {
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
                    gate.release();

                    let done = UploadDone {
                        key: job.key,
                        bytes: bytes as u64,
                        kind: job.kind,
                        result,
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
            speed_bps: 0,
            samples: VecDeque::new(),
            last_completion: None,
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
                self.observe_done(done.bytes);
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
                self.observe_done(done.bytes);
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

    /// Update the rolling throughput and scale the worker target. Call this
    /// periodically while uploads are running so a long pack does not leave
    /// the target stale.
    pub fn tick(&mut self) {
        self.maybe_scale(Instant::now());
    }

    pub fn stats(&self) -> UploadPoolStats {
        UploadPoolStats {
            speed_bps: self.speed_bps,
            target_threads: self.target_threads,
            max_threads: self.max_threads,
            in_flight: self.gate.active(),
        }
    }

    fn observe_done(&mut self, bytes: u64) {
        let now = Instant::now();
        self.samples.push_back((now, bytes));
        self.prune_samples(now);
        self.last_completion = Some(now);
        self.maybe_scale(now);
    }

    fn prune_samples(&mut self, now: Instant) {
        while self
            .samples
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > SPEED_WINDOW)
        {
            self.samples.pop_front();
        }
    }

    fn maybe_scale(&mut self, now: Instant) {
        if now.duration_since(self.last_scale) < SCALE_INTERVAL {
            return;
        }
        self.prune_samples(now);
        self.speed_bps = window_speed(&self.samples, now);

        let progressing = self.gate.active() > 0;
        let stalled = self
            .last_completion
            .is_some_and(|at| now.duration_since(at) > Duration::from_secs(30));
        if self.samples.is_empty() && progressing && !stalled {
            // A large pack is still in flight. Keep the current target rather
            // than punishing the connection for not completing a 64 MiB object
            // inside one scale window.
            self.last_scale = now;
            return;
        }

        let desired = desired_threads(self.speed_bps, self.max_threads);
        let next = next_target(self.target_threads, desired, self.max_threads);
        if next != self.target_threads {
            self.target_threads = next;
            self.gate.set_target(next);
        }
        self.last_scale = now;
    }
}

fn window_speed(samples: &VecDeque<(Instant, u64)>, now: Instant) -> u64 {
    let bytes: u64 = samples
        .iter()
        .filter(|(at, _)| now.duration_since(*at) <= SPEED_WINDOW)
        .map(|(_, bytes)| *bytes)
        .sum();
    (bytes as f64 / SPEED_WINDOW.as_secs_f64()) as u64
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
    fn scaling_policy_matches_expected_throughput() {
        assert_eq!(desired_threads(0, MAX_UPLOAD_THREADS), 1);
        assert_eq!(desired_threads(512 * 1024, MAX_UPLOAD_THREADS), 2);
        assert_eq!(desired_threads(2 * 1024 * 1024, MAX_UPLOAD_THREADS), 3);
        assert_eq!(desired_threads(5 * 1024 * 1024, MAX_UPLOAD_THREADS), 4);
        assert_eq!(desired_threads(10 * 1024 * 1024, MAX_UPLOAD_THREADS), 6);
        assert_eq!(desired_threads(30 * 1024 * 1024, MAX_UPLOAD_THREADS), 12);
        assert_eq!(desired_threads(50 * 1024 * 1024, MAX_UPLOAD_THREADS), 16);
        assert_eq!(desired_threads(5 * 1024 * 1024, 2), 2);
    }

    #[test]
    fn target_moves_with_hysteresis() {
        assert_eq!(next_target(2, 8, MAX_UPLOAD_THREADS), 4);
        assert_eq!(next_target(4, 8, MAX_UPLOAD_THREADS), 6);
        assert_eq!(next_target(8, 2, MAX_UPLOAD_THREADS), 7);
        assert_eq!(next_target(1, 1, MAX_UPLOAD_THREADS), 1);
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

    #[test]
    fn speed_window_ignores_expired_completions() {
        let now = Instant::now();
        let mib = 1024 * 1024;
        let mut samples = VecDeque::new();
        samples.push_back((now - Duration::from_secs(16), 10 * mib));
        samples.push_back((now - Duration::from_secs(5), 5 * mib));
        samples.push_back((now, 10 * mib));
        assert_eq!(window_speed(&samples, now), mib);
    }
}
