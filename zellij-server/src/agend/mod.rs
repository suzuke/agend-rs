pub mod backend;
pub mod config;
pub mod daemon;
pub mod db;
pub mod fleet;
pub mod ipc;
pub mod lifecycle;
pub mod mcp;
pub mod monitor;
pub mod routing;
pub mod telegram;
#[cfg(test)]
mod tests;

use crate::panes::PaneId;
use config::FleetConfig;
use fleet::FleetManager;
use monitor::{Monitor, MonitorAction, PtyEvent};
use std::path::PathBuf;

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
        })
        .expect("failed to spawn agend daemon thread");
    log::info!("agend: daemon thread started");
}

/// Drain pending monitor actions and write them to terminals.
/// Called from the screen event loop.
pub fn drain_actions<F>(mut write_fn: F)
where
    F: FnMut(u32, Vec<u8>),
{
    while let Some(action) = monitor::recv_action() {
        match action {
            MonitorAction::Write(tid, bytes) => write_fn(tid, bytes),
        }
    }
}
