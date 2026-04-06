//! Shared path helpers — eliminates repeated HOME + ".agend" construction.

use std::path::PathBuf;

/// AgEnD base directory: $AGEND_HOME or ~/.agend
pub fn agend_home() -> PathBuf {
    if let Ok(custom) = std::env::var("AGEND_HOME") {
        return PathBuf::from(custom);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".agend")
}

/// Per-instance directory: <agend_home>/instances/<name>
pub fn instance_dir(name: &str) -> PathBuf {
    agend_home().join("instances").join(name)
}

/// Per-instance IPC socket: <agend_home>/instances/<name>/channel.sock
pub fn instance_socket(name: &str) -> PathBuf {
    instance_dir(name).join("channel.sock")
}

/// Database path: <agend_home>/agend.db
pub fn db_path() -> PathBuf {
    agend_home().join("agend.db")
}

/// Instances base directory: <agend_home>/instances/
pub fn instances_base() -> PathBuf {
    agend_home().join("instances")
}
