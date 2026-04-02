//! Fleet manager — generates layout and tracks instance↔terminal mappings.

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
    /// each running the backend CLI command.
    pub fn generate_layout(config: &FleetConfig) -> String {
        let mut kdl = String::from("layout {\n");
        for (name, ic) in &config.instances {
            let backend = ic.backend_or(&config.defaults);
            let (cmd, args) = build_command(backend, ic, &config.defaults);
            let cwd = ic.working_directory.display();

            kdl.push_str(&format!("    tab name=\"{name}\" {{\n"));
            kdl.push_str(&format!("        pane command=\"{cmd}\" cwd=\"{cwd}\" name=\"{name}\""));
            if args.is_empty() {
                kdl.push_str("\n");
            } else {
                kdl.push_str(" {\n");
                let args_str = args
                    .iter()
                    .map(|a| format!("\"{}\"", a.replace('"', "\\\"")))
                    .collect::<Vec<_>>()
                    .join(" ");
                kdl.push_str(&format!("            args {args_str}\n"));
                kdl.push_str("        }\n");
            }
            kdl.push_str("    }\n");
        }
        kdl.push_str("}\n");
        kdl
    }

    /// Write per-instance config files (mcp-config.json, etc.) to the agend
    /// instance directories. Returns the instance dir base path.
    pub fn write_instance_configs(
        config: &FleetConfig,
        zellij_binary: &str,
    ) -> std::io::Result<PathBuf> {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        let base = PathBuf::from(home).join(".agend").join("instances");

        for (name, ic) in &config.instances {
            let instance_dir = base.join(name);
            std::fs::create_dir_all(&instance_dir)?;

            // Write mcp-config.json
            let socket_path = instance_dir.join("channel.sock");
            let tool_set = "full"; // TODO: per-instance tool_set config
            let mcp_config = generate_mcp_config(
                zellij_binary,
                &socket_path.to_string_lossy(),
                tool_set,
            );
            let mcp_config_path = instance_dir.join("mcp-config.json");
            std::fs::write(
                &mcp_config_path,
                serde_json::to_string_pretty(&mcp_config).unwrap(),
            )?;

            // Write instance metadata
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
        Ok(base)
    }
}

/// Build the CLI command + args for a backend.
fn build_command(
    backend: &str,
    instance: &InstanceConfig,
    defaults: &Defaults,
) -> (String, Vec<String>) {
    let model = instance
        .model
        .as_deref()
        .or(defaults.model.as_deref());

    match backend {
        "claude-code" => {
            let mut args = Vec::new();
            if instance.skip_permissions {
                args.push("--dangerously-skip-permissions".into());
            }
            if let Some(m) = model {
                args.push("--model".into());
                args.push(m.into());
            }
            ("claude".into(), args)
        },
        "codex" => {
            let mut args = Vec::new();
            if let Some(m) = model {
                args.push("--model".into());
                args.push(m.into());
            }
            ("codex".into(), args)
        },
        "gemini-cli" => {
            let mut args = Vec::new();
            if let Some(m) = model {
                args.push("--model".into());
                args.push(m.into());
            }
            ("gemini".into(), args)
        },
        "opencode" => ("opencode".into(), vec![]),
        other => {
            // Treat as raw command
            (other.into(), vec![])
        },
    }
}
