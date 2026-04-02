//! Fleet manager — generates layout and tracks instance↔terminal mappings.

use super::config::{Defaults, FleetConfig, InstanceConfig};

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
