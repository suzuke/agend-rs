//! Fleet manager — generates layout and tracks instance↔terminal mappings.

use super::backend::{self, BackendConfig, SpawnCommand};
use super::config::{Defaults, FleetConfig, InstanceConfig};
use super::mcp::generate_mcp_config;
use std::path::PathBuf;

/// Information about a running instance.
#[derive(Debug, Clone)]
pub struct InstanceInfo {
    pub name: String,
    pub backend: String,
    pub terminal_id: Option<u32>,
}

/// Manages the mapping between fleet instances and Zellij panes.
pub struct FleetManager {
    pub instances: Vec<InstanceInfo>,
}

impl FleetManager {
    pub fn from_config(config: &FleetConfig) -> Self {
        let instances = config
            .instances
            .iter()
            .map(|(name, ic)| InstanceInfo {
                name: name.clone(),
                backend: ic.backend_or(&config.defaults).to_owned(),
                terminal_id: None,
            })
            .collect();
        Self { instances }
    }

    /// Generate a KDL layout string that creates one tab per instance,
    /// each running the backend CLI command with full config (--mcp-config, etc.).
    pub fn generate_layout(config: &FleetConfig, zellij_binary: &str) -> String {
        let instances_base = super::paths::instances_base();

        let mut kdl = String::from("layout {\n");
        for (name, ic) in &config.instances {
            let backend = ic.backend_or(&config.defaults);
            let instance_dir = instances_base.join(name);
            let socket_path = instance_dir.join("channel.sock");

            // Use backend config writer to get the full command
            let bcfg = BackendConfig {
                instance_name: name,
                instance_dir: &instance_dir,
                working_directory: &ic.working_directory,
                mcp_server_binary: zellij_binary,
                socket_path: &socket_path,
                system_prompt: None, // TODO: from config
                skip_permissions: ic.skip_permissions,
                model: ic.model.as_deref().or(config.defaults.model.as_deref()),
                tool_set: "full",
                session_id: read_session_id(&instance_dir),
            };

            let spawn = match backend::write_config(backend, &bcfg) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("agend: failed to write config for '{name}': {e}");
                    // Fallback to simple command
                    SpawnCommand {
                        command: format!("{backend}"),
                        env: vec![],
                    }
                },
            };

            // Parse command into (binary, args)
            let (cmd_binary, cmd_args) = parse_command(&spawn.command);
            let cwd = ic.working_directory.display();

            // Build env string for KDL
            let mut env_parts: Vec<String> = spawn
                .env
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            // Always set TERM
            env_parts.insert(0, "TERM=xterm-256color".into());

            kdl.push_str(&format!("    tab name=\"{name}\" {{\n"));

            if cmd_args.is_empty() && env_parts.is_empty() {
                kdl.push_str(&format!(
                    "        pane command=\"{cmd_binary}\" cwd=\"{cwd}\" name=\"{name}\"\n"
                ));
            } else {
                kdl.push_str(&format!(
                    "        pane command=\"{cmd_binary}\" cwd=\"{cwd}\" name=\"{name}\" {{\n"
                ));
                if !cmd_args.is_empty() {
                    let args_str = cmd_args
                        .iter()
                        .map(|a| format!("\"{}\"", a.replace('"', "\\\"")))
                        .collect::<Vec<_>>()
                        .join(" ");
                    kdl.push_str(&format!("            args {args_str}\n"));
                }
                kdl.push_str("        }\n");
            }
            kdl.push_str("    }\n");
        }
        kdl.push_str("}\n");
        kdl
    }

    /// Write per-instance config files (mcp-config.json, instance.json).
    /// This is now called as part of generate_layout() via backend::write_config().
    pub fn write_instance_metadata(config: &FleetConfig) -> std::io::Result<()> {
        let base = super::paths::instances_base();

        for (name, ic) in &config.instances {
            let instance_dir = base.join(name);
            std::fs::create_dir_all(&instance_dir)?;

            let meta = serde_json::json!({
                "name": name,
                "backend": ic.backend_or(&config.defaults),
                "working_directory": ic.working_directory.display().to_string(),
                "description": ic.description,
                "tags": ic.tags,
            });
            std::fs::write(
                instance_dir.join("instance.json"),
                serde_json::to_string_pretty(&meta).unwrap(),
            )?;
        }
        Ok(())
    }
}

/// Parse a shell command string into (binary, args).
/// Handles simple quoting.
fn parse_command(cmd: &str) -> (String, Vec<String>) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return (String::new(), vec![]);
    }
    let binary = parts[0].to_owned();
    let args: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
    (binary, args)
}

/// Get a simple command + args for a backend (no config writing).
pub fn simple_command(backend: &str) -> (String, Vec<String>) {
    match backend {
        "claude-code" => ("claude".into(), vec![]),
        "codex" => ("codex".into(), vec![]),
        "gemini-cli" => ("gemini".into(), vec!["--yolo".into()]),
        "opencode" => ("opencode".into(), vec![]),
        other => (other.into(), vec![]),
    }
}

/// Read saved session ID for resume support.
fn read_session_id(instance_dir: &PathBuf) -> Option<String> {
    let path = instance_dir.join("session-id");
    std::fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}
