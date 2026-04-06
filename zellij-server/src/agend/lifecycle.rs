//! Instance lifecycle — start, stop, restart, crash recovery.
//!
//! Manages the state machine for each agent instance:
//!   Stopped → Starting → Ready → (crash) → Restarting → Ready
//!
//! Health checks detect crashed windows and trigger auto-respawn with
//! configurable backoff (exponential or linear).

use super::config::{FleetConfig, RestartPolicy};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// State of an instance.
#[derive(Debug, Clone, PartialEq)]
pub enum InstanceState {
    Stopped,
    Starting,
    Ready,
    Crashed,
    CrashLoop,
}

/// Per-instance lifecycle state.
#[derive(Debug)]
pub struct InstanceLifecycle {
    pub name: String,
    pub state: InstanceState,
    pub crash_count: u32,
    pub rapid_crash_count: u32,
    pub last_spawn_at: Option<Instant>,
    pub last_crash_at: Option<Instant>,
    pub restart_policy: RestartPolicy,
}

impl InstanceLifecycle {
    pub fn new(name: String, policy: RestartPolicy) -> Self {
        Self {
            name,
            state: InstanceState::Stopped,
            crash_count: 0,
            rapid_crash_count: 0,
            last_spawn_at: None,
            last_crash_at: None,
            restart_policy: policy,
        }
    }

    /// Mark instance as starting.
    pub fn mark_starting(&mut self) {
        self.state = InstanceState::Starting;
        self.last_spawn_at = Some(Instant::now());
    }

    /// Mark instance as ready.
    pub fn mark_ready(&mut self) {
        self.state = InstanceState::Ready;
        self.rapid_crash_count = 0;
    }

    /// Mark instance as crashed. Returns the backoff duration before respawn,
    /// or None if we should give up.
    pub fn mark_crashed(&mut self) -> Option<Duration> {
        self.state = InstanceState::Crashed;
        self.last_crash_at = Some(Instant::now());

        // Rapid crash detection: died within 60s of spawn
        if let Some(spawn_at) = self.last_spawn_at {
            if spawn_at.elapsed() < Duration::from_secs(60) {
                self.rapid_crash_count += 1;
                if self.rapid_crash_count >= 3 {
                    log::error!(
                        "agend lifecycle: instance '{}' in crash loop (3 rapid crashes), stopping",
                        self.name
                    );
                    self.state = InstanceState::CrashLoop;
                    return None;
                }
            } else {
                self.rapid_crash_count = 0;
            }
        }

        // Reset crash count if enough time has passed
        if let Some(last_crash) = self.last_crash_at {
            if last_crash.elapsed() > Duration::from_secs(self.restart_policy.reset_after) {
                self.crash_count = 0;
            }
        }

        self.crash_count += 1;
        if self.crash_count > self.restart_policy.max_retries {
            log::error!(
                "agend lifecycle: instance '{}' exceeded max retries ({}), giving up",
                self.name,
                self.restart_policy.max_retries
            );
            return None;
        }

        let backoff = self.calculate_backoff();
        log::info!(
            "agend lifecycle: instance '{}' crashed (attempt {}/{}), backoff {:.1}s",
            self.name,
            self.crash_count,
            self.restart_policy.max_retries,
            backoff.as_secs_f64()
        );

        Some(backoff)
    }

    fn calculate_backoff(&self) -> Duration {
        let ms = match self.restart_policy.backoff.as_str() {
            "exponential" => {
                let base = 1000u64 * 2u64.saturating_pow(self.crash_count.saturating_sub(1));
                base.min(60_000)
            },
            "linear" => {
                (1000u64 * self.crash_count as u64).min(60_000)
            },
            _ => 5000,
        };
        Duration::from_millis(ms)
    }

    /// Check if enough time has passed to reset crash count.
    pub fn maybe_reset_crashes(&mut self) {
        if let Some(last_crash) = self.last_crash_at {
            if last_crash.elapsed() > Duration::from_secs(self.restart_policy.reset_after) {
                self.crash_count = 0;
            }
        }
    }
}

/// Manages lifecycle for all instances.
pub struct LifecycleManager {
    instances: HashMap<String, InstanceLifecycle>,
}

impl LifecycleManager {
    pub fn from_config(config: &FleetConfig) -> Self {
        let mut instances = HashMap::new();
        for (name, _ic) in &config.instances {
            let policy = config.defaults.restart_policy.clone();
            instances.insert(name.clone(), InstanceLifecycle::new(name.clone(), policy));
        }
        Self { instances }
    }

    pub fn get(&self, name: &str) -> Option<&InstanceLifecycle> {
        self.instances.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut InstanceLifecycle> {
        self.instances.get_mut(name)
    }

    pub fn all_states(&self) -> Vec<(&str, &InstanceState)> {
        self.instances
            .iter()
            .map(|(n, i)| (n.as_str(), &i.state))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_policy() -> RestartPolicy {
        RestartPolicy {
            max_retries: 3,
            backoff: "exponential".into(),
            reset_after: 300,
        }
    }

    #[test]
    fn normal_lifecycle() {
        let mut inst = InstanceLifecycle::new("test".into(), default_policy());
        assert_eq!(inst.state, InstanceState::Stopped);

        inst.mark_starting();
        assert_eq!(inst.state, InstanceState::Starting);

        inst.mark_ready();
        assert_eq!(inst.state, InstanceState::Ready);
    }

    #[test]
    fn crash_with_exponential_backoff() {
        let mut inst = InstanceLifecycle::new("test".into(), default_policy());
        inst.mark_starting();
        // Simulate non-rapid crash (spawn > 60s ago)
        inst.last_spawn_at = Some(Instant::now() - Duration::from_secs(120));

        let backoff = inst.mark_crashed().unwrap();
        assert_eq!(inst.crash_count, 1);
        assert!(backoff >= Duration::from_millis(900)); // ~1000ms

        inst.mark_starting();
        inst.last_spawn_at = Some(Instant::now() - Duration::from_secs(120));
        let backoff = inst.mark_crashed().unwrap();
        assert_eq!(inst.crash_count, 2);
        assert!(backoff >= Duration::from_millis(1900)); // ~2000ms
    }

    #[test]
    fn max_retries_exceeded() {
        let mut inst = InstanceLifecycle::new("test".into(), default_policy());

        for _ in 0..3 {
            inst.mark_starting();
            inst.last_spawn_at = Some(Instant::now() - Duration::from_secs(120));
            inst.mark_crashed();
        }

        inst.mark_starting();
        inst.last_spawn_at = Some(Instant::now() - Duration::from_secs(120));
        let result = inst.mark_crashed();
        assert!(result.is_none()); // gave up
    }

    #[test]
    fn rapid_crash_detection() {
        let mut inst = InstanceLifecycle::new("test".into(), default_policy());

        // 3 rapid crashes (within 60s of spawn)
        for _ in 0..3 {
            inst.mark_starting();
            // last_spawn_at is NOW, so crash within 60s
            inst.mark_crashed();
        }

        assert_eq!(inst.state, InstanceState::CrashLoop);
    }
}
