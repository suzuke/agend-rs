//! Configuration — reads and validates fleet.yaml.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct FleetConfig {
    pub channel: Option<ChannelConfig>,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub instances: HashMap<String, InstanceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelConfig {
    #[serde(rename = "type")]
    pub channel_type: String, // "telegram" or "discord"
    #[serde(default = "default_channel_mode")]
    pub mode: String, // "topic" or "dm"
    pub bot_token_env: Option<String>,
    pub group_id: Option<i64>,
    pub access: Option<AccessConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessConfig {
    #[serde(default = "default_access_mode")]
    pub mode: String, // "locked" or "pairing"
    #[serde(default)]
    pub allowed_users: Vec<i64>,
}

fn default_channel_mode() -> String {
    "topic".into()
}
fn default_access_mode() -> String {
    "locked".into()
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Defaults {
    #[serde(default = "default_backend")]
    pub backend: String,
    pub model: Option<String>,
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    pub context_guardian: Option<ContextGuardianConfig>,
}

impl Defaults {
    pub fn max_age_hours(&self) -> Option<u32> {
        self.context_guardian.as_ref().map(|c| c.max_age_hours)
    }
    pub fn grace_period_ms(&self) -> Option<u64> {
        self.context_guardian.as_ref().map(|c| c.grace_period_ms)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContextGuardianConfig {
    #[serde(default)]
    pub grace_period_ms: u64,
    #[serde(default)]
    pub max_age_hours: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestartPolicy {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_backoff")]
    pub backoff: String,
    #[serde(default = "default_reset_after")]
    pub reset_after: u64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff: default_backoff(),
            reset_after: default_reset_after(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceConfig {
    pub working_directory: PathBuf,
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub backend: Option<String>,
    pub model: Option<String>,
    #[serde(default = "default_true")]
    pub skip_permissions: bool,
    pub topic_id: Option<i64>,
    #[serde(default)]
    pub general_topic: bool,
    pub context_guardian: Option<ContextGuardianConfig>,
    pub display_name: Option<String>,
    /// System prompt: inline string or "file:path/to/prompt.md" reference.
    pub system_prompt: Option<String>,
    /// Tool set profile: "full" (default), "standard", or "minimal".
    pub tool_set: Option<String>,
}

impl InstanceConfig {
    /// Resolve the backend name, falling back to fleet defaults.
    pub fn backend_or<'a>(&'a self, defaults: &'a Defaults) -> &'a str {
        self.backend.as_deref().unwrap_or(&defaults.backend)
    }

    /// Resolve the system prompt. If it starts with "file:", read from that path.
    pub fn resolve_system_prompt(&self) -> Option<String> {
        let raw = self.system_prompt.as_deref()?;
        if let Some(path) = raw.strip_prefix("file:") {
            match std::fs::read_to_string(path.trim()) {
                Ok(content) => Some(content),
                Err(e) => {
                    log::warn!("agend config: failed to read system prompt from {}: {e}", path);
                    None
                },
            }
        } else {
            Some(raw.to_owned())
        }
    }

    /// Display name, falling back to instance name.
    pub fn display_name_or<'a>(&'a self, instance_name: &'a str) -> &'a str {
        self.display_name.as_deref().unwrap_or(instance_name)
    }

    pub fn max_age_hours(&self) -> Option<u32> {
        self.context_guardian.as_ref().map(|c| c.max_age_hours)
    }
    pub fn grace_period_ms(&self) -> Option<u64> {
        self.context_guardian.as_ref().map(|c| c.grace_period_ms)
    }
}

fn default_true() -> bool {
    true
}
fn default_backend() -> String {
    "claude-code".into()
}
fn default_max_retries() -> u32 {
    10
}
fn default_backoff() -> String {
    "exponential".into()
}
fn default_reset_after() -> u64 {
    300
}

impl FleetConfig {
    /// Load fleet.yaml from a directory (looks for `fleet.yaml` or `fleet.yml`).
    pub fn load(dir: &Path) -> anyhow::Result<Self> {
        let yaml_path = dir.join("fleet.yaml");
        let yml_path = dir.join("fleet.yml");
        let path = if yaml_path.exists() {
            yaml_path
        } else if yml_path.exists() {
            yml_path
        } else {
            anyhow::bail!("fleet.yaml not found in {}", dir.display());
        };
        let contents = std::fs::read_to_string(&path)?;
        let config: FleetConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    /// Load fleet.yaml from the default AgEnD config directory (~/.agend/).
    pub fn load_default() -> anyhow::Result<Self> {
        Self::load(&super::paths::agend_home())
    }
}
