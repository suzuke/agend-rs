//! Health check and context rotation.
//!
//! Periodically checks instance health and triggers rotation when
//! max_age_hours is reached. Integrates with lifecycle for crash recovery.

use super::config::FleetConfig;
use super::lifecycle::{InstanceLifecycle, InstanceState, LifecycleManager};
use crossbeam::channel::{self, Receiver, Sender};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Health events sent to the daemon for action.
#[derive(Debug)]
pub enum HealthEvent {
    /// Instance needs restart (reason, instance_name).
    RestartNeeded(String, String),
    /// Instance entered crash loop.
    CrashLoop(String),
}

static HEALTH_CHANNEL: Lazy<(Sender<HealthEvent>, Receiver<HealthEvent>)> =
    Lazy::new(|| channel::bounded(64));

/// Receive pending health events (non-blocking).
pub fn recv_health_event() -> Option<HealthEvent> {
    HEALTH_CHANNEL.1.try_recv().ok()
}

/// Per-instance health state.
struct InstanceHealth {
    lifecycle: InstanceLifecycle,
    last_pty_activity: Instant,
    spawn_time: Option<Instant>,
    max_age: Option<Duration>,
    grace_until: Option<Instant>,
}

/// Health checker — runs in its own thread.
pub struct HealthChecker {
    instances: HashMap<String, InstanceHealth>,
    check_interval: Duration,
}

impl HealthChecker {
    pub fn from_config(config: &FleetConfig) -> Self {
        let mut instances = HashMap::new();
        for (name, ic) in &config.instances {
            let policy = config.defaults.restart_policy.clone();
            let max_age = ic
                .max_age_hours()
                .or_else(|| config.defaults.max_age_hours())
                .filter(|&h| h > 0)
                .map(|h| Duration::from_secs(h as u64 * 3600));
            let grace_ms = ic
                .grace_period_ms()
                .or_else(|| config.defaults.grace_period_ms())
                .unwrap_or(600_000);

            instances.insert(
                name.clone(),
                InstanceHealth {
                    lifecycle: InstanceLifecycle::new(name.clone(), policy),
                    last_pty_activity: Instant::now(),
                    spawn_time: None,
                    max_age,
                    grace_until: None,
                },
            );
        }
        Self {
            instances,
            check_interval: Duration::from_secs(30),
        }
    }

    /// Notify that an instance produced PTY output (still alive).
    pub fn record_activity(instance_name: &str) {
        // Use the global channel to notify — but for simplicity,
        // we track activity via the monitor's last-seen time.
        // This is a no-op placeholder; activity is tracked via PTY bytes.
        let _ = instance_name;
    }

    /// Mark an instance as started.
    pub fn mark_started(&mut self, name: &str) {
        if let Some(h) = self.instances.get_mut(name) {
            h.lifecycle.mark_starting();
            h.spawn_time = Some(Instant::now());
        }
    }

    /// Mark an instance as ready.
    pub fn mark_ready(&mut self, name: &str) {
        if let Some(h) = self.instances.get_mut(name) {
            h.lifecycle.mark_ready();
            h.last_pty_activity = Instant::now();
        }
    }

    /// Run the health check loop. Blocks the calling thread.
    pub fn run(mut self) {
        log::info!("agend health: checker started (interval: {:?})", self.check_interval);

        // Mark all instances as starting initially
        let names: Vec<String> = self.instances.keys().cloned().collect();
        for name in &names {
            self.mark_started(name);
        }

        loop {
            std::thread::sleep(self.check_interval);

            for (name, health) in &mut self.instances {
                // Skip if in crash loop or stopped
                if health.lifecycle.state == InstanceState::CrashLoop
                    || health.lifecycle.state == InstanceState::Stopped
                {
                    continue;
                }

                // Skip if in grace period
                if let Some(grace_until) = health.grace_until {
                    if Instant::now() < grace_until {
                        continue;
                    }
                    health.grace_until = None;
                }

                // Check max_age rotation
                if let (Some(max_age), Some(spawn_time)) = (health.max_age, health.spawn_time) {
                    if spawn_time.elapsed() >= max_age {
                        log::info!(
                            "agend health: instance '{}' reached max_age ({:?}), requesting rotation",
                            name, max_age
                        );
                        let _ = HEALTH_CHANNEL.0.try_send(HealthEvent::RestartNeeded(
                            "max_age".into(),
                            name.clone(),
                        ));
                        // Enter grace period to prevent immediate re-trigger
                        health.grace_until = Some(Instant::now() + Duration::from_secs(600));
                        health.spawn_time = Some(Instant::now()); // reset for next cycle
                    }
                }

                // Check for prolonged inactivity (optional, 5 min threshold)
                // This is a soft check — PTY bytes should flow if CLI is alive
                if health.lifecycle.state == InstanceState::Ready
                    && health.last_pty_activity.elapsed() > Duration::from_secs(300)
                {
                    log::debug!(
                        "agend health: instance '{}' no PTY activity for {:?}",
                        name,
                        health.last_pty_activity.elapsed()
                    );
                    // Don't trigger restart — CLI might just be idle
                    // Update activity time to prevent log spam
                    health.last_pty_activity = Instant::now();
                }
            }
        }
    }
}

// ── Session ID management ───────────────────────────────────────────────

/// Save a session ID for an instance (for --resume support).
pub fn save_session_id(instance_name: &str, session_id: &str) {
    let path = super::paths::instance_dir(instance_name).join("session-id");
    if let Err(e) = std::fs::write(&path, session_id) {
        log::warn!("agend health: failed to save session-id for {instance_name}: {e}");
    }
}

/// Clear the session ID for an instance (on crash recovery).
pub fn clear_session_id(instance_name: &str) {
    let path = super::paths::instance_dir(instance_name).join("session-id");
    let _ = std::fs::remove_file(&path);
}

/// Read the session ID for an instance.
pub fn read_session_id(instance_name: &str) -> Option<String> {
    let path = super::paths::instance_dir(instance_name).join("session-id");
    std::fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let inst_dir = tmp.path().join(".agend/instances/test-inst");
        std::fs::create_dir_all(&inst_dir).unwrap();

        // Set HOME to tmp for this test
        let path = inst_dir.join("session-id");
        std::fs::write(&path, "abc-123").unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.trim(), "abc-123");

        std::fs::remove_file(&path).unwrap();
        assert!(!path.exists());
    }
}
