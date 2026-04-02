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

    // Write per-instance config files (mcp-config.json, instance.json)
    let zellij_binary = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "zellij".into());
    if let Err(e) = FleetManager::write_instance_configs(&config, &zellij_binary) {
        log::warn!("agend: failed to write instance configs: {e}");
    }

    let layout = FleetManager::generate_layout(&config);
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

/// Start the agend monitor thread.
/// Called during server session init (hook #3).
pub fn start_monitor() {
    // Load fleet config in the monitor thread for instance→backend mapping
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
