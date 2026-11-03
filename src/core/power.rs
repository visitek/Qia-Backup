//! Power-aware backup policy.
//!
//! The macOS shell feeds sleep/wake notifications and battery state into this
//! pure state machine. Keeping the policy here makes it testable without any
//! platform APIs and keeps the app shell responsible only for observation.

use std::time::{Duration, Instant};

/// After sleep or battery use, wait this long on stable power before starting
/// a backup. This avoids immediately resuming on a flaky charger or a brief
/// wake-up.
pub const STABLE_POWER_WINDOW: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone)]
pub struct PowerPolicy {
    asleep: bool,
    on_battery: bool,
    stable_since: Option<Instant>,
    requires_stable_window: bool,
    stable_window: Duration,
}

impl PowerPolicy {
    pub fn new(now: Instant) -> Self {
        Self::new_with_window(now, STABLE_POWER_WINDOW)
    }

    pub fn new_with_window(now: Instant, stable_window: Duration) -> Self {
        Self {
            asleep: false,
            on_battery: false,
            stable_since: Some(now),
            requires_stable_window: false,
            stable_window,
        }
    }

    pub fn set_asleep(&mut self, asleep: bool, now: Instant) {
        if self.asleep == asleep {
            return;
        }
        self.asleep = asleep;
        if asleep {
            self.stable_since = None;
            self.requires_stable_window = true;
        } else if !self.on_battery {
            self.stable_since = Some(now);
        }
    }

    pub fn set_on_battery(&mut self, on_battery: bool, now: Instant) {
        if self.on_battery == on_battery {
            return;
        }
        self.on_battery = on_battery;
        if on_battery {
            self.stable_since = None;
            self.requires_stable_window = true;
        } else if !self.asleep {
            self.stable_since = Some(now);
        }
    }

    pub fn can_run(&self, now: Instant) -> bool {
        if self.asleep || self.on_battery {
            return false;
        }
        if !self.requires_stable_window {
            return true;
        }
        self.stable_since
            .is_some_and(|since| now.duration_since(since) >= self.stable_window)
    }

    pub fn is_asleep(&self) -> bool {
        self.asleep
    }

    pub fn is_on_battery(&self) -> bool {
        self.on_battery
    }

    pub fn stable_remaining(&self, now: Instant) -> Option<Duration> {
        if self.asleep || self.on_battery || !self.requires_stable_window {
            return None;
        }
        let since = self.stable_since?;
        let elapsed = now.duration_since(since);
        if elapsed >= self.stable_window {
            None
        } else {
            Some(self.stable_window - elapsed)
        }
    }

    /// Human-readable reason why backups are paused, or `None` when they may
    /// run. The app puts this in the status banner.
    pub fn message(&self, now: Instant) -> Option<String> {
        if self.asleep {
            return Some("Backups are paused while the Mac is asleep.".into());
        }
        if self.on_battery {
            return Some("Backups are paused on battery power. Connect power to continue.".into());
        }
        if let Some(remaining) = self.stable_remaining(now) {
            let minutes = remaining.as_secs().div_ceil(60).max(1);
            return Some(format!(
                "Waiting for {minutes} minute{} of stable power before resuming.",
                if minutes == 1 { "" } else { "s" }
            ));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ac_power_starts_ready() {
        let now = Instant::now();
        let policy = PowerPolicy::new(now);
        assert!(policy.can_run(now));
        assert!(policy.message(now).is_none());
    }

    #[test]
    fn battery_pauses_and_ac_waits_for_stable_window() {
        let now = Instant::now();
        let mut policy = PowerPolicy::new_with_window(now, Duration::from_secs(300));
        policy.set_on_battery(true, now);
        assert!(!policy.can_run(now));
        assert!(policy.message(now).unwrap().contains("battery"));

        policy.set_on_battery(false, now + Duration::from_secs(10));
        assert!(!policy.can_run(now + Duration::from_secs(10)));
        assert!(!policy.can_run(now + Duration::from_secs(309)));
        assert!(policy.can_run(now + Duration::from_secs(310)));
    }

    #[test]
    fn stable_power_hint_clears_at_the_run_boundary() {
        let now = Instant::now();
        let window = Duration::from_secs(300);
        let mut policy = PowerPolicy::new_with_window(now, window);
        policy.set_on_battery(true, now);
        let connected = now + Duration::from_secs(10);
        policy.set_on_battery(false, connected);
        let ready = connected + window;
        let before = ready - Duration::from_nanos(1);
        assert!(!policy.can_run(before));
        assert_eq!(
            policy.stable_remaining(before),
            Some(Duration::from_nanos(1))
        );
        assert!(policy.message(before).is_some());
        for instant in [ready, ready + Duration::from_nanos(1)] {
            assert!(policy.can_run(instant));
            assert_eq!(policy.stable_remaining(instant), None);
            assert_eq!(policy.message(instant), None);
        }
    }

    #[test]
    fn sleep_and_wake_require_stable_power() {
        let now = Instant::now();
        let mut policy = PowerPolicy::new_with_window(now, Duration::from_secs(300));
        policy.set_asleep(true, now);
        assert!(!policy.can_run(now));
        assert!(policy.message(now).unwrap().contains("asleep"));

        policy.set_asleep(false, now + Duration::from_secs(60));
        assert!(!policy.can_run(now + Duration::from_secs(60)));
        assert!(policy.can_run(now + Duration::from_secs(360)));
    }

    #[test]
    fn battery_while_asleep_waits_for_wake_and_ac() {
        let now = Instant::now();
        let mut policy = PowerPolicy::new_with_window(now, Duration::from_secs(300));
        policy.set_asleep(true, now);
        policy.set_on_battery(true, now + Duration::from_secs(10));
        policy.set_on_battery(false, now + Duration::from_secs(20));
        assert!(!policy.can_run(now + Duration::from_secs(400)));

        policy.set_asleep(false, now + Duration::from_secs(500));
        assert!(!policy.can_run(now + Duration::from_secs(500)));
        assert!(policy.can_run(now + Duration::from_secs(800)));
    }
}
