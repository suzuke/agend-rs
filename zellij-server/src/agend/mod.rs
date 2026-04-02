pub mod backend;
pub mod config;
pub mod daemon;
pub mod db;
pub mod fleet;
pub mod health;
pub mod ipc;
pub mod lifecycle;
pub mod mcp;
pub mod monitor;
pub mod paths;
pub mod routing;
pub mod telegram;
#[cfg(test)]
mod tests;

use crate::panes::PaneId;
use config::FleetConfig;
use crossbeam::channel::{self, Receiver, Sender};
use fleet::FleetManager;
use monitor::{Monitor, MonitorAction, PtyEvent};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

/// Debug log to file (since the server daemonizes and stdout is lost).
#[allow(dead_code)]
pub fn debug_log(msg: &str) {
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/agend-debug.log")
    {
        let _ = writeln!(f, "[{}] {}", chrono::Utc::now().format("%H:%M:%S%.3f"), msg);
    }
}

// ── Daemon output channel (daemon thread → screen thread → pty_writer) ──

/// Actions the daemon wants to perform on panes/tabs.
pub enum DaemonAction {
    /// Write bytes to a terminal pane (terminal_id, bytes).
    Write(u32, Vec<u8>),
    /// Create a new tab with a command (tab_name, command, args, cwd).
    NewTab {
        name: String,
        command: String,
        args: Vec<String>,
        cwd: std::path::PathBuf,
    },
    /// Close a tab by name.
    CloseTab(String),
}

static DAEMON_CHANNEL: Lazy<(Sender<DaemonAction>, Receiver<DaemonAction>)> =
    Lazy::new(|| channel::bounded(256));

/// Send a daemon action to the screen thread for pty_writer delivery.
pub fn send_daemon_action(action: DaemonAction) {
    let _ = DAEMON_CHANNEL.0.try_send(action);
}

fn recv_daemon_action() -> Option<DaemonAction> {
    DAEMON_CHANNEL.1.try_recv().ok()
}

// ── Terminal registry (shared: monitor registers, daemon reads) ─────────

/// Global mapping: instance_name → terminal_id.
/// Updated by the monitor when panes are registered.
/// Read by the daemon to route messages to the right pane.
static TERMINAL_REGISTRY: Lazy<RwLock<HashMap<String, u32>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Register a terminal_id for an instance name.
/// Updates BOTH the global registry (for daemon) AND notifies the monitor
/// (for dialog detection and ready pattern matching).
pub fn register_terminal(instance_name: &str, terminal_id: u32) {
    log::info!("agend: register_terminal {} → tid {}", instance_name, terminal_id);
    monitor::send_pty_event(PtyEvent::Register(terminal_id, instance_name.to_owned()));

    if let Ok(mut reg) = TERMINAL_REGISTRY.write() {
        reg.insert(instance_name.to_owned(), terminal_id);
    }
}

/// Look up the terminal_id for an instance name.
pub fn terminal_for_instance(instance_name: &str) -> Option<u32> {
    TERMINAL_REGISTRY
        .read()
        .ok()
        .and_then(|reg| reg.get(instance_name).copied())
}

// ── Public API (called from Zellij hooks) ───────────────────────────────

/// Generate a KDL layout string from fleet.yaml config.
/// Also writes per-instance config files (mcp-config.json, backend configs).
/// Called from main.rs to inject into the Zellij startup.
pub fn generate_layout_from_config(config_dir: Option<&str>) -> Result<String, String> {
    let config = match config_dir {
        Some(dir) => FleetConfig::load(&PathBuf::from(dir)),
        None => FleetConfig::load_default(),
    };
    let config = config.map_err(|e| format!("Failed to load fleet.yaml: {e}"))?;

    if config.instances.is_empty() {
        return Err("No instances defined in fleet.yaml".into());
    }

    let zellij_binary = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "zellij".into());

    // Write instance metadata
    if let Err(e) = FleetManager::write_instance_metadata(&config) {
        log::warn!("agend: failed to write instance metadata: {e}");
    }

    // Generate layout (this also writes backend configs via backend::write_config)
    let layout = FleetManager::generate_layout(&config, &zellij_binary);
    log::info!("agend: generated layout for {} instances", config.instances.len());
    log::debug!("agend: layout:\n{layout}");
    Ok(layout)
}

/// Forward PTY bytes to the agend monitor.
/// Called from the screen thread's PtyBytes handler (hook #2).
#[inline]
pub fn on_pty_bytes(terminal_id: u32, bytes: &[u8]) {
    monitor::send_pty_event(PtyEvent::Bytes(terminal_id, bytes.to_vec()));
}

/// Notify the monitor about a new pane.
/// Called from the screen thread's NewPane handler (hook #4).
#[inline]
pub fn on_new_pane(pid: PaneId, pane_name: Option<&str>) {
    if let PaneId::Terminal(tid) = pid {
        if let Some(name) = pane_name {
            if !name.is_empty() {
                monitor::send_pty_event(PtyEvent::Register(tid, name.to_owned()));
            }
        }
    }
}

/// Start the agend monitor thread AND the daemon (IPC servers + tool routing).
/// Called during server session init (hook #3).
pub fn start_monitor() {
    log::info!("agend: start_monitor() — starting PTY monitor + daemon + health");

    // Start PTY monitor thread
    std::thread::Builder::new()
        .name("agend_monitor".into())
        .spawn(|| {
            let monitor = match FleetConfig::load_default() {
                Ok(config) => {
                    log::info!(
                        "agend monitor: loaded fleet config with {} instances",
                        config.instances.len()
                    );
                    Monitor::with_config(&config)
                },
                Err(e) => {
                    log::warn!("agend monitor: failed to load fleet config: {e}, using empty config");
                    Monitor::new()
                },
            };
            monitor.run();
        })
        .expect("failed to spawn agend monitor thread");
    log::info!("agend: monitor thread started");

    // Start daemon (IPC servers + tool routing + Telegram)
    std::thread::Builder::new()
        .name("agend_daemon".into())
        .spawn(|| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match FleetConfig::load_default() {
                    Ok(config) => {
                        log::info!("agend daemon: starting with {} instances", config.instances.len());
                        let daemon = daemon::Daemon::start(config);
                        daemon.run(); // blocks
                    },
                    Err(e) => {
                        log::error!("agend daemon: failed to load fleet config: {e}");
                    },
                }
            }));
            if let Err(e) = result {
                debug_log(&format!("DAEMON PANICKED: {:?}", e));
                log::error!("agend daemon: thread panicked: {:?}", e);
            }
        })
        .expect("failed to spawn agend daemon thread");
    log::info!("agend: daemon thread started");

    // Start health checker (max_age rotation, crash recovery)
    std::thread::Builder::new()
        .name("agend_health".into())
        .spawn(|| {
            match FleetConfig::load_default() {
                Ok(config) => {
                    let checker = health::HealthChecker::from_config(&config);
                    checker.run(); // blocks
                },
                Err(e) => {
                    log::error!("agend health: failed to load fleet config: {e}");
                },
            }
        })
        .expect("failed to spawn agend health thread");
    log::info!("agend: health checker started");
}

/// Drain pending actions (from both monitor and daemon) and write them to terminals.
/// Called from the screen event loop.
///
/// `write_fn`: writes bytes to a terminal's PTY stdin.
/// `tab_fn`: creates/closes tabs (receives DaemonAction::NewTab or CloseTab).
pub fn drain_actions<W, T>(mut write_fn: W, mut tab_fn: T)
where
    W: FnMut(u32, Vec<u8>),
    T: FnMut(DaemonAction),
{
    // Drain monitor actions (dialog dismissal, etc.)
    while let Some(action) = monitor::recv_action() {
        match action {
            MonitorAction::Write(tid, bytes) => write_fn(tid, bytes),
        }
    }
    // Drain daemon actions (message injection + tab operations)
    while let Some(action) = recv_daemon_action() {
        match action {
            DaemonAction::Write(tid, bytes) => write_fn(tid, bytes),
            other => tab_fn(other),
        }
    }
}
