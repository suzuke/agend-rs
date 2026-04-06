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
    /// Instance appears hung (instance_name, idle_seconds).
    HangDetected(String, u64),
}

static HEALTH_CHANNEL: Lazy<(Sender<HealthEvent>, Receiver<HealthEvent>)> =
    Lazy::new(|| channel::bounded(64));

/// Receive pending health events (non-blocking).
pub fn recv_health_event() -> Option<HealthEvent> {
    HEALTH_CHANNEL.1.try_recv().ok()
}

/// Default hang threshold: 15 minutes without PTY activity.
const HANG_THRESHOLD_SECS: u64 = 15 * 60;

/// Minimum wait between rotation warning and actual kill+restart.
const ROTATION_WAIT_SECS: u64 = 30;

/// Per-instance health state.
struct InstanceHealth {
    lifecycle: InstanceLifecycle,
    backend: String,
    spawn_time: Option<Instant>,
    max_age: Option<Duration>,
    grace_until: Option<Instant>,
    /// Whether a hang notification has been sent (reset on next PTY activity).
    hang_notified: bool,
    /// Last known activity timestamp when hang was notified.
    hang_notified_at: u64,
    /// When a rotation warning was injected (two-phase rotation).
    /// Phase 1: inject warning → set this. Phase 2: elapsed >= 30s → kill + restart.
    rotation_scheduled_at: Option<Instant>,
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
            let _grace_ms = ic
                .grace_period_ms()
                .or_else(|| config.defaults.grace_period_ms())
                .unwrap_or(600_000);

            instances.insert(
                name.clone(),
                InstanceHealth {
                    lifecycle: InstanceLifecycle::new(name.clone(), policy),
                    backend: ic.backend_or(&config.defaults).to_owned(),
                    spawn_time: None,
                    max_age,
                    grace_until: None,
                    hang_notified: false,
                    hang_notified_at: 0,
                    rotation_scheduled_at: None,
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

                // Two-phase context rotation:
                // Phase 2: rotation was scheduled → wait elapsed → kill + restart
                if let Some(scheduled) = health.rotation_scheduled_at {
                    if scheduled.elapsed().as_secs() >= ROTATION_WAIT_SECS {
                        log::info!(
                            "agend health: instance '{}' rotation wait complete, restarting",
                            name
                        );
                        restart_instance(name);
                        let _ = HEALTH_CHANNEL.0.try_send(HealthEvent::RestartNeeded(
                            "context_rotation".into(),
                            name.clone(),
                        ));
                        health.grace_until = Some(Instant::now() + Duration::from_secs(600));
                        health.spawn_time = Some(Instant::now());
                        health.rotation_scheduled_at = None;
                    }
                    continue; // skip other checks while rotation is pending
                }

                // Phase 1: max_age exceeded → inject warning → schedule rotation
                if let (Some(max_age), Some(spawn_time)) = (health.max_age, health.spawn_time) {
                    if spawn_time.elapsed() >= max_age {
                        log::info!(
                            "agend health: instance '{}' reached max_age ({:?}), injecting rotation warning",
                            name, max_age
                        );
                        // Inject warning so agent can save context via post_decision
                        let warning = format!(
                            "\n[system:context-rotation] Your session will rotate in {} seconds. \
                             Use post_decision to save any important context you want to preserve.\n",
                            ROTATION_WAIT_SECS
                        );
                        if let Some(tid) = super::terminal_for_instance(name) {
                            super::send_daemon_action(super::DaemonAction::Write(
                                tid,
                                warning.into_bytes(),
                            ));
                        }
                        health.rotation_scheduled_at = Some(Instant::now());
                    }
                }

                // Hang detection: no PTY activity for 15 minutes
                // Skip: pre-ready instances, opencode (subprocess mode idles normally)
                if health.lifecycle.state == InstanceState::Ready
                    && health.backend != "opencode"
                {
                    let last = super::monitor::last_activity_secs(name);
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let idle_secs = now.saturating_sub(last);

                    if idle_secs >= HANG_THRESHOLD_SECS {
                        // Only notify once per hang episode (reset when activity resumes)
                        if !health.hang_notified || health.hang_notified_at != last {
                            log::warn!(
                                "agend health: instance '{}' appears hung (no PTY activity for {}s)",
                                name, idle_secs
                            );
                            let _ = HEALTH_CHANNEL.0.try_send(HealthEvent::HangDetected(
                                name.clone(),
                                idle_secs,
                            ));
                            health.hang_notified = true;
                            health.hang_notified_at = last;
                        }
                    } else if health.hang_notified {
                        // Activity resumed — reset hang state
                        health.hang_notified = false;
                    }
                }
            }
        }
    }
}

// ── Instance restart ────────────────────────────────────────────────────

/// Restart an instance: close old tab, clear session-id, create new tab.
fn restart_instance(instance_name: &str) {
    log::info!("agend health: restarting instance '{}'", instance_name);

    // Clear session-id so next spawn starts fresh
    clear_session_id(instance_name);

    // Close old tab
    super::send_daemon_action(super::DaemonAction::CloseTab(instance_name.to_owned()));

    // Read config and create new tab
    if let Ok(config) = super::config::FleetConfig::load_default() {
        if let Some(ic) = config.instances.get(instance_name) {
            let backend = ic.backend_or(&config.defaults);
            let instance_dir = super::paths::instance_dir(instance_name);
            let socket_path = instance_dir.join("channel.sock");

            let zellij_binary = std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "zellij".into());

            let resolved_prompt = ic.resolve_system_prompt();
            let bcfg = super::backend::BackendConfig {
                instance_name,
                display_name: ic.display_name.as_deref(),
                instance_dir: &instance_dir,
                working_directory: &ic.working_directory,
                mcp_server_binary: &zellij_binary,
                socket_path: &socket_path,
                system_prompt: resolved_prompt.as_deref(),
                skip_permissions: ic.skip_permissions,
                model: ic.model.as_deref().or(config.defaults.model.as_deref()),
                tool_set: ic.tool_set.as_deref().unwrap_or("full"),
                session_id: None, // fresh start
            };

            match super::backend::write_config(backend, &bcfg) {
                Ok(spawn) => {
                    let parts: Vec<&str> = spawn.command.split_whitespace().collect();
                    if !parts.is_empty() {
                        super::send_daemon_action(super::DaemonAction::NewTab {
                            name: instance_name.to_owned(),
                            command: parts[0].to_owned(),
                            args: parts[1..].iter().map(|s| s.to_string()).collect(),
                            cwd: ic.working_directory.clone(),
                        });
                    }
                },
                Err(e) => log::error!("agend health: failed to write config for restart: {e}"),
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
