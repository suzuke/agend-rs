pub mod config;
pub mod fleet;
pub mod mcp;
pub mod monitor;
pub mod routing;
#[cfg(test)]
mod tests;

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

/// Start the agend monitor thread.
/// Called during server session init (hook #3).
pub fn start_monitor() {
    std::thread::Builder::new()
        .name("agend_monitor".into())
        .spawn(|| {
            let monitor = Monitor::new();
            monitor.run();
        })
        .expect("failed to spawn agend monitor thread");
    log::info!("agend: monitor thread started");
}

/// Drain pending monitor actions and write them to terminals.
/// Called from the screen event loop or a dedicated agend thread.
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
