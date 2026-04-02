//! Backend config writers — generate per-backend config files and commands.
//!
//! Each backend (claude-code, codex, gemini-cli, opencode) has different
//! configuration requirements. This module handles writing the right files
//! and building the right command line for each.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};

// Shared constants to avoid magic strings
const MCP_SERVER_KEY: &str = "agend";
const MCP_SUBCOMMAND: &str = "agend-mcp-server";
const ENV_SOCKET_PATH: &str = "AGEND_SOCKET_PATH";
const ENV_TOOL_SET: &str = "AGEND_TOOL_SET";
const ENV_INSTANCE_NAME: &str = "AGEND_INSTANCE_NAME";

/// Configuration needed to launch a backend.
pub struct BackendConfig<'a> {
    pub instance_name: &'a str,
    pub instance_dir: &'a Path,
    pub working_directory: &'a Path,
    pub mcp_server_binary: &'a str,
    pub socket_path: &'a Path,
    pub system_prompt: Option<&'a str>,
    pub skip_permissions: bool,
    pub model: Option<&'a str>,
    pub tool_set: &'a str,
    pub session_id: Option<String>,
}

/// Result of writing config: the command to run.
pub struct SpawnCommand {
    pub command: String,
    pub env: Vec<(String, String)>,
}

// ── Shared helpers ──────────────────────────────────────────────────────

/// Build standard MCP server JSON config block.
fn mcp_server_entry(binary: &str, socket_path: &Path, instance_name: &str, tool_set: &str) -> Value {
    json!({
        "command": binary,
        "args": [MCP_SUBCOMMAND],
        "env": {
            ENV_SOCKET_PATH: socket_path.to_string_lossy(),
            ENV_INSTANCE_NAME: instance_name,
            ENV_TOOL_SET: tool_set,
        }
    })
}

/// Write system prompt to a file if provided.
fn write_system_prompt(cfg: &BackendConfig, path: &Path) -> std::io::Result<()> {
    if let Some(prompt) = cfg.system_prompt {
        std::fs::write(path, prompt)?;
    }
    Ok(())
}

/// Standard instance name env var tuple.
fn instance_env(name: &str) -> (String, String) {
    (ENV_INSTANCE_NAME.into(), name.into())
}

// ── Claude Code ─────────────────────────────────────────────────────────

pub fn write_claude_code_config(cfg: &BackendConfig) -> std::io::Result<SpawnCommand> {
    std::fs::create_dir_all(cfg.instance_dir)?;

    let mcp_config = json!({
        "mcpServers": { MCP_SERVER_KEY: mcp_server_entry(cfg.mcp_server_binary, cfg.socket_path, cfg.instance_name, cfg.tool_set) }
    });
    let mcp_path = cfg.instance_dir.join("mcp-config.json");
    std::fs::write(&mcp_path, serde_json::to_string_pretty(&mcp_config).unwrap())?;

    // Zellij handles statusline so this is minimal
    let settings_path = cfg.instance_dir.join("claude-settings.json");
    std::fs::write(&settings_path, "{}")?;

    let prompt_path = cfg.instance_dir.join("system-prompt.md");
    write_system_prompt(cfg, &prompt_path)?;

    // Prevents interactive API key approval prompt on startup
    pre_approve_claude_api_key();

    // Build command
    let mut args = vec![
        format!("--settings {}", settings_path.display()),
        format!("--mcp-config {}", mcp_path.display()),
    ];
    if cfg.skip_permissions {
        args.push("--dangerously-skip-permissions".into());
    }
    if let Some(sid) = &cfg.session_id {
        args.push(format!("--resume {sid}"));
    }
    if let Some(m) = cfg.model {
        args.push(format!("--model {m}"));
    }
    if cfg.system_prompt.is_some() {
        args.push(format!("--system-prompt \"{}\"", prompt_path.display()));
    }

    let mut env = vec![
        instance_env(cfg.instance_name),
    ];
    // Forward API keys if set
    if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
        env.push(("ANTHROPIC_API_KEY".into(), key));
    }
    if let Ok(url) = std::env::var("ANTHROPIC_BASE_URL") {
        env.push(("ANTHROPIC_BASE_URL".into(), url));
    }

    Ok(SpawnCommand {
        command: format!("claude {}", args.join(" ")),
        env,
    })
}

fn pre_approve_claude_api_key() {
    let api_key = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(k) if k.len() > 20 => k,
        _ => return,
    };
    let fingerprint = &api_key[api_key.len() - 20..];
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    let claude_json = PathBuf::from(&home).join(".claude.json");

    let mut cfg: serde_json::Value = std::fs::read_to_string(&claude_json)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(json!({}));

    let approved = cfg
        .get("customApiKeyResponses")
        .and_then(|v| v.get("approved"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    if !approved.iter().any(|v| v.as_str() == Some(fingerprint)) {
        let mut new_approved = approved;
        new_approved.push(json!(fingerprint));
        cfg["customApiKeyResponses"] = json!({
            "approved": new_approved,
            "rejected": cfg.get("customApiKeyResponses")
                .and_then(|v| v.get("rejected"))
                .cloned()
                .unwrap_or(json!([])),
        });
        let _ = std::fs::write(&claude_json, serde_json::to_string_pretty(&cfg).unwrap());
    }
}

// ── Codex ───────────────────────────────────────────────────────────────

pub fn write_codex_config(cfg: &BackendConfig) -> std::io::Result<SpawnCommand> {
    std::fs::create_dir_all(cfg.instance_dir)?;

    // System prompt
    if let Some(prompt) = cfg.system_prompt {
        std::fs::write(cfg.instance_dir.join("system-prompt.md"), prompt)?;
    }

    // MCP setup script
    let setup = format!(
        "codex mcp add {key} --env {env_name}=\"{name}\" --env {env_sock}=\"{sock}\" -- {bin} {sub} 2>/dev/null || true\n",
        key = MCP_SERVER_KEY,
        env_name = ENV_INSTANCE_NAME, name = cfg.instance_name,
        env_sock = ENV_SOCKET_PATH, sock = cfg.socket_path.display(),
        bin = cfg.mcp_server_binary, sub = MCP_SUBCOMMAND,
    );
    let setup_path = cfg.instance_dir.join("setup-mcp.sh");
    std::fs::write(&setup_path, &setup)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&setup_path, std::fs::Permissions::from_mode(0o755))?;
    }

    let mut args = Vec::new();
    if cfg.skip_permissions {
        args.push("--dangerously-bypass-approvals-and-sandbox".into());
    } else {
        args.push("--full-auto".into());
    }
    if let Some(m) = cfg.model {
        args.push(format!("--model \"{m}\""));
    }

    Ok(SpawnCommand {
        command: format!("codex {}", args.join(" ")),
        env: vec![instance_env(cfg.instance_name)],
    })
}

// ── Gemini CLI ──────────────────────────────────────────────────────────

pub fn write_gemini_config(cfg: &BackendConfig) -> std::io::Result<SpawnCommand> {
    std::fs::create_dir_all(cfg.instance_dir)?;

    // .gemini/settings.json in working directory
    let gemini_dir = cfg.working_directory.join(".gemini");
    std::fs::create_dir_all(&gemini_dir)?;
    let settings = json!({
        "mcpServers": { MCP_SERVER_KEY: mcp_server_entry(cfg.mcp_server_binary, cfg.socket_path, cfg.instance_name, cfg.tool_set) }
    });
    std::fs::write(
        gemini_dir.join("settings.json"),
        serde_json::to_string_pretty(&settings).unwrap(),
    )?;

    write_system_prompt(cfg, &gemini_dir.join("GEMINI.md"))?;

    // Pre-trust working directory
    pre_trust_gemini(cfg.working_directory);

    let mut args: Vec<String> = vec!["--yolo".into(), "--resume".into(), "latest".into()];
    if let Some(m) = cfg.model {
        args.push("--model".into());
        args.push(m.into());
    }

    Ok(SpawnCommand {
        command: format!("gemini {}", args.join(" ")),
        env: vec![instance_env(cfg.instance_name)],
    })
}

fn pre_trust_gemini(work_dir: &Path) {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    let trust_file = PathBuf::from(&home).join(".gemini").join("trustedFolders.json");

    let mut trusted: serde_json::Map<String, serde_json::Value> =
        std::fs::read_to_string(&trust_file)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

    let wd = work_dir.display().to_string();
    let mut changed = false;

    if !trusted.contains_key(&wd) {
        trusted.insert(wd.clone(), json!("TRUST_FOLDER"));
        changed = true;
    }
    // Also trust parent
    if let Some(parent) = work_dir.parent() {
        let pd = parent.display().to_string();
        if pd != wd && !trusted.contains_key(&pd) {
            trusted.insert(pd, json!("TRUST_PARENT"));
            changed = true;
        }
    }

    if changed {
        std::fs::create_dir_all(trust_file.parent().unwrap()).ok();
        let _ = std::fs::write(
            &trust_file,
            serde_json::to_string_pretty(&serde_json::Value::Object(trusted)).unwrap(),
        );
    }
}

// ── OpenCode ────────────────────────────────────────────────────────────

pub fn write_opencode_config(cfg: &BackendConfig) -> std::io::Result<SpawnCommand> {
    std::fs::create_dir_all(cfg.instance_dir)?;

    // OpenCode uses a different MCP format (local type, command array)
    let mut mcp = serde_json::Map::new();
    mcp.insert(
        MCP_SERVER_KEY.into(),
        json!({
            "type": "local",
            "command": [cfg.mcp_server_binary, MCP_SUBCOMMAND],
            "environment": {
                ENV_SOCKET_PATH: cfg.socket_path.to_string_lossy(),
                ENV_INSTANCE_NAME: cfg.instance_name,
                ENV_TOOL_SET: cfg.tool_set,
            }
        }),
    );

    let mut oc_config = json!({"mcp": mcp});
    if cfg.system_prompt.is_some() {
        oc_config["instructions"] = json!([".opencode-instructions.md"]);
    }
    std::fs::write(
        cfg.working_directory.join("opencode.json"),
        serde_json::to_string_pretty(&oc_config).unwrap(),
    )?;

    if let Some(prompt) = cfg.system_prompt {
        std::fs::write(
            cfg.working_directory.join(".opencode-instructions.md"),
            prompt,
        )?;
    }

    Ok(SpawnCommand {
        command: "opencode".into(),
        env: vec![instance_env(cfg.instance_name)],
    })
}

// ── Dispatch ────────────────────────────────────────────────────────────

/// Write config and build command for the given backend.
pub fn write_config(backend: &str, cfg: &BackendConfig) -> std::io::Result<SpawnCommand> {
    match backend {
        "claude-code" => write_claude_code_config(cfg),
        "codex" => write_codex_config(cfg),
        "gemini-cli" => write_gemini_config(cfg),
        "opencode" => write_opencode_config(cfg),
        _ => {
            // Unknown backend — just run the command
            Ok(SpawnCommand {
                command: backend.into(),
                env: vec![],
            })
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn claude_code_config_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let instance_dir = tmp.path().join("instance");
        let socket_path = instance_dir.join("channel.sock");

        let cfg = BackendConfig {
            instance_name: "test",
            instance_dir: &instance_dir,
            working_directory: tmp.path(),
            mcp_server_binary: "/usr/bin/zellij",
            socket_path: &socket_path,
            system_prompt: Some("You are helpful"),
            skip_permissions: true,
            model: Some("opus"),
            tool_set: "full",
            session_id: None,
        };

        let cmd = write_claude_code_config(&cfg).unwrap();
        assert!(cmd.command.contains("claude"));
        assert!(cmd.command.contains("--dangerously-skip-permissions"));
        assert!(cmd.command.contains("--model opus"));
        assert!(cmd.command.contains("--system-prompt"));

        // Verify files exist
        assert!(instance_dir.join("mcp-config.json").exists());
        assert!(instance_dir.join("claude-settings.json").exists());
        assert!(instance_dir.join("system-prompt.md").exists());
    }

    #[test]
    fn gemini_config_with_pretrust() {
        let tmp = tempfile::tempdir().unwrap();
        let instance_dir = tmp.path().join("instance");
        let work_dir = tmp.path().join("project");
        std::fs::create_dir_all(&work_dir).unwrap();
        let socket_path = instance_dir.join("channel.sock");

        let cfg = BackendConfig {
            instance_name: "gemini-test",
            instance_dir: &instance_dir,
            working_directory: &work_dir,
            mcp_server_binary: "zellij",
            socket_path: &socket_path,
            system_prompt: None,
            skip_permissions: false,
            model: Some("gemini-2.5-pro"),
            tool_set: "standard",
            session_id: None,
        };

        let cmd = write_gemini_config(&cfg).unwrap();
        assert!(cmd.command.contains("gemini"));
        assert!(cmd.command.contains("--yolo"));
        assert!(cmd.command.contains("--model gemini-2.5-pro"));

        // Verify .gemini/settings.json
        let settings_path = work_dir.join(".gemini").join("settings.json");
        assert!(settings_path.exists());
    }

    #[test]
    fn opencode_config() {
        let tmp = tempfile::tempdir().unwrap();
        let instance_dir = tmp.path().join("instance");
        let work_dir = tmp.path().join("project");
        std::fs::create_dir_all(&work_dir).unwrap();
        let socket_path = instance_dir.join("channel.sock");

        let cfg = BackendConfig {
            instance_name: "oc-test",
            instance_dir: &instance_dir,
            working_directory: &work_dir,
            mcp_server_binary: "zellij",
            socket_path: &socket_path,
            system_prompt: Some("Be helpful"),
            skip_permissions: false,
            model: None,
            tool_set: "full",
            session_id: None,
        };

        let cmd = write_opencode_config(&cfg).unwrap();
        assert_eq!(cmd.command, "opencode");
        assert!(work_dir.join("opencode.json").exists());
        assert!(work_dir.join(".opencode-instructions.md").exists());
    }
}
