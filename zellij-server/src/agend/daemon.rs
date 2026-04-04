//! Daemon — central hub that routes IPC tool calls to the right handler.
//!
//! Each instance has an IPC server (Unix socket). Tool calls from MCP servers
//! arrive here and are dispatched to: channel adapter, fleet manager, database,
//! or other instances.

use super::config::FleetConfig;
use super::db::AgendDb;
use super::ipc::{IpcRequest, IpcResponse, IpcServer};
use super::routing::RoutingEngine;
use super::telegram::{OutboundAction, TelegramAdapter, TelegramSender};
use crossbeam::channel::Receiver;
use isahc::ReadResponseExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Runtime instance info for dynamically created instances.
#[derive(Debug, Clone, serde::Serialize)]
struct RuntimeInstance {
    name: String,
    backend: String,
    working_directory: String,
    description: Option<String>,
}

/// The daemon manages all IPC servers and routes tool calls.
pub struct Daemon {
    config: FleetConfig,
    db: AgendDb,
    routing: Arc<RwLock<RoutingEngine>>,
    /// Instance name → IPC request receiver
    ipc_receivers: HashMap<String, Receiver<IpcRequest>>,
    /// Telegram adapter sender (if configured)
    telegram: Option<TelegramSender>,
    /// Dynamically created instances (not in fleet.yaml)
    runtime_instances: Arc<RwLock<HashMap<String, RuntimeInstance>>>,
}

impl Daemon {
    /// Create and start the daemon from fleet config.
    pub fn start(config: FleetConfig) -> Self {
        let db_path = super::paths::db_path();
        std::fs::create_dir_all(db_path.parent().unwrap()).ok();

        let db = AgendDb::open(&db_path).expect("failed to open agend database");
        log::info!("agend daemon: database opened at {}", db_path.display());

        let mut routing = RoutingEngine::new();
        routing.rebuild(&config);

        // Start IPC servers for each instance
        let mut ipc_receivers = HashMap::new();
        let instances_dir = super::paths::instances_base();

        for name in config.instances.keys() {
            let socket_path = instances_dir.join(name).join("channel.sock");
            std::fs::create_dir_all(socket_path.parent().unwrap()).ok();

            let (server, rx) = IpcServer::new(socket_path);
            server.start(name.clone());
            ipc_receivers.insert(name.clone(), rx);
        }

        let routing = Arc::new(RwLock::new(routing));

        // Start Telegram adapter if configured (uses plain HTTP, no tokio)
        let telegram = TelegramAdapter::from_config(&config, Arc::clone(&routing))
            .map(|adapter| {
                log::info!("agend daemon: starting Telegram adapter");
                adapter.run()
            });

        Self {
            config,
            db,
            routing,
            runtime_instances: Arc::new(RwLock::new(HashMap::new())),
            ipc_receivers,
            telegram,
        }
    }

    /// Run the daemon event loop. Blocks the calling thread.
    pub fn run(self) {
        log::info!(
            "agend daemon: running with {} instances",
            self.ipc_receivers.len()
        );

        log::info!("agend daemon: select loop with {} IPC receivers", self.ipc_receivers.len());

        // Merge all IPC receivers into a single select loop
        let mut sel = crossbeam::channel::Select::new();
        let names: Vec<String> = self.ipc_receivers.keys().cloned().collect();
        let receivers: Vec<&Receiver<IpcRequest>> =
            names.iter().map(|n| &self.ipc_receivers[n]).collect();

        for rx in &receivers {
            sel.recv(rx);
        }

        // Also listen for inbound Telegram messages
        let telegram_inbound = self.telegram.as_ref().map(|t| &t.inbound_rx);
        if let Some(rx) = telegram_inbound {
            sel.recv(rx);
        }

        loop {
            let oper = sel.select();
            let index = oper.index();

            if index < receivers.len() {
                // IPC request from an instance
                if let Ok(req) = oper.recv(receivers[index]) {
                    self.handle_ipc_request(req);
                }
            } else if let Some(rx) = telegram_inbound {
                // Telegram inbound message
                if let Ok(msg) = oper.recv(rx) {
                    self.handle_telegram_inbound(msg);
                }
            }
        }
    }

    fn handle_ipc_request(&self, req: IpcRequest) {
        let instance_name = &req.instance_name;
        let msg = &req.message;
        let request_id = msg.request_id.unwrap_or(0);

        match msg.msg_type.as_str() {
            "tool_call" => {
                let tool = msg.tool.as_deref().unwrap_or("");
                let args = msg.args.as_ref().cloned().unwrap_or(json!({}));

                let result = self.handle_tool_call(instance_name, tool, &args);
                let _ = req.reply_tx.send(result_to_response(request_id, result));
            },
            "mcp_ready" => {
                let session = msg.session_name.as_deref().unwrap_or(instance_name);
                log::info!("agend daemon: MCP ready for '{session}' (instance: {instance_name})");
                let _ = req.reply_tx.send(IpcResponse {
                    request_id,
                    result: Some(json!("ok")),
                    error: None,
                });
            },
            other => {
                log::warn!("agend daemon: unknown message type '{other}' from {instance_name}");
                let _ = req.reply_tx.send(IpcResponse {
                    request_id,
                    result: None,
                    error: Some(format!("unknown message type: {other}")),
                });
            },
        }
    }

    fn handle_tool_call(
        &self,
        instance_name: &str,
        tool: &str,
        args: &Value,
    ) -> Result<Value, String> {
        match tool {
            // ── Channel tools → Telegram ────────────────────────────────
            "reply" | "react" | "edit_message" => {
                self.route_to_telegram(tool, args)
            },
            "download_attachment" => self.handle_download_attachment(args),

            // ── Cross-instance tools ────────────────────────────────────
            "send_to_instance" => self.handle_send_to_instance(instance_name, args),
            "broadcast" => self.handle_broadcast(instance_name, args),
            "list_instances" => self.handle_list_instances(args),
            "describe_instance" => self.handle_describe_instance(args),
            "request_information" => {
                let target = args["target_instance"].as_str().unwrap_or("");
                let question = args["question"].as_str().unwrap_or("");
                let mut send_args = json!({
                    "instance_name": target,
                    "message": question,
                    "request_kind": "query",
                    "requires_reply": true,
                });
                if let Some(ctx) = args.get("context") {
                    send_args["message"] = json!(format!("{}\n\nContext: {}", question, ctx));
                }
                self.handle_send_to_instance(instance_name, &send_args)
            },
            "delegate_task" => {
                let target = args["target_instance"].as_str().unwrap_or("");
                let task = args["task"].as_str().unwrap_or("");
                let send_args = json!({
                    "instance_name": target,
                    "message": task,
                    "request_kind": "task",
                    "requires_reply": true,
                    "task_summary": args.get("success_criteria"),
                });
                self.handle_send_to_instance(instance_name, &send_args)
            },
            "report_result" => {
                let target = args["target_instance"].as_str().unwrap_or("");
                let summary = args["summary"].as_str().unwrap_or("");
                let send_args = json!({
                    "instance_name": target,
                    "message": summary,
                    "request_kind": "report",
                    "correlation_id": args.get("correlation_id"),
                });
                self.handle_send_to_instance(instance_name, &send_args)
            },

            // ── Decision CRUD ───────────────────────────────────────────
            "post_decision" => {
                let wd = self.working_dir_for(instance_name);
                let tags: Vec<String> = args["tags"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect())
                    .unwrap_or_default();
                match self.db.create_decision(
                    &wd,
                    args["scope"].as_str().unwrap_or("project"),
                    args["title"].as_str().unwrap_or(""),
                    args["content"].as_str().unwrap_or(""),
                    &tags,
                    instance_name,
                    args["ttl_days"].as_u64().map(|d| d as u32),
                    args["supersedes"].as_str(),
                ) {
                    Ok(d) => Ok(serde_json::to_value(d).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "list_decisions" => {
                let wd = self.working_dir_for(instance_name);
                let tags: Vec<String> = args["tags"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect())
                    .unwrap_or_default();
                match self.db.list_decisions(
                    &wd,
                    args["include_archived"].as_bool().unwrap_or(false),
                    &tags,
                ) {
                    Ok(list) => Ok(serde_json::to_value(list).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "update_decision" => {
                let id = args["id"].as_str().unwrap_or("");
                let tags: Option<Vec<String>> = args["tags"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect());
                match self.db.update_decision(
                    id,
                    args["content"].as_str(),
                    tags.as_deref(),
                    args["ttl_days"].as_u64().map(|d| d as u32),
                    args["archive"].as_bool().unwrap_or(false),
                ) {
                    Ok(d) => Ok(serde_json::to_value(d).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },

            // ── Task CRUD ───────────────────────────────────────────────
            "task" => self.handle_task(instance_name, args),

            // ── Schedule CRUD ───────────────────────────────────────────
            "create_schedule" => {
                match self.db.create_schedule(
                    args["cron"].as_str().unwrap_or(""),
                    args["message"].as_str().unwrap_or(""),
                    instance_name,
                    args["target"].as_str().unwrap_or(instance_name),
                    args["label"].as_str(),
                    args["timezone"].as_str(),
                ) {
                    Ok(s) => Ok(serde_json::to_value(s).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "list_schedules" => {
                match self.db.list_schedules(args["target"].as_str()) {
                    Ok(list) => Ok(serde_json::to_value(list).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "update_schedule" => {
                let id = args["id"].as_str().unwrap_or("");
                match self.db.update_schedule(
                    id,
                    args["cron"].as_str(),
                    args["message"].as_str(),
                    args["target"].as_str(),
                    args["label"].as_str(),
                    args["timezone"].as_str(),
                    args["enabled"].as_bool(),
                ) {
                    Ok(s) => Ok(serde_json::to_value(s).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "delete_schedule" => {
                let id = args["id"].as_str().unwrap_or("");
                match self.db.delete_schedule(id) {
                    Ok(()) => Ok(json!("ok")),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },

            // ── Instance management ──────────────────────────────────────
            "start_instance" | "create_instance" => {
                self.handle_create_or_start_instance(instance_name, args)
            },
            "delete_instance" => {
                let name = args["name"].as_str().unwrap_or("");
                log::info!("agend daemon: delete_instance '{name}' requested");
                super::send_daemon_action(super::DaemonAction::CloseTab(name.to_owned()));
                Ok(json!({"deleted": true, "name": name}))
            },

            // ── Repo checkout ────────────────────────────────────────────
            "checkout_repo" => self.handle_checkout_repo(args),
            "release_repo" => self.handle_release_repo(args),

            // ── Role management ─────────────────────────────────────────
            "set_role" => self.handle_set_role(instance_name, args),
            "get_role" => self.handle_get_role(args),

            // ── Unknown ─────────────────────────────────────────────────
            _ => Err(format!("unknown tool: {tool}")),
        }
    }

    // ── Fleet tool handlers ─────────────────────────────────────────────

    fn handle_send_to_instance(
        &self,
        sender: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let target = args["instance_name"].as_str().unwrap_or("");
        let message = args["message"].as_str().unwrap_or("");

        if !self.config.instances.contains_key(target) {
            return Err(format!("instance not found: {target}"));
        }

        let correlation_id = args["correlation_id"]
            .as_str()
            .map(|s| s.to_owned())
            .unwrap_or_else(|| {
                format!("cid-{}-{}", chrono::Utc::now().timestamp_millis(), &uuid::Uuid::new_v4().to_string()[..6])
            });

        // Inject message directly into the target pane's PTY stdin
        let sender_display = self.config.instances.get(sender)
            .map(|ic| ic.display_name_or(sender))
            .unwrap_or(sender);
        let formatted = format!(
            "[from:{}] {}\n(Reply using send_to_instance tool, NOT direct text)\n",
            sender_display, message
        );
        inject_message_to_instance(target, &formatted);

        // Post visibility to Telegram if available
        if let Some(ref telegram) = self.telegram {
            if let Some(topic_id) = self.config.instances.get(target).and_then(|i| i.topic_id) {
                let group_id = self.config.channel.as_ref().and_then(|c| c.group_id);
                if let Some(gid) = group_id {
                    let _ = telegram.outbound_tx.try_send(OutboundAction::SendText {
                        chat_id: gid.to_string(),
                        text: format!("← {sender}:\n{message}"),
                        thread_id: Some(topic_id.to_string()),
                        reply_to: None,
                        format: None,
                    });
                }
            }
        }

        log::info!("agend daemon: {} → {}: {}", sender, target, &message[..message.len().min(100)]);

        Ok(json!({
            "sent": true,
            "target": target,
            "correlation_id": correlation_id,
        }))
    }

    fn handle_broadcast(&self, sender: &str, args: &Value) -> Result<Value, String> {
        let message = args["message"].as_str().unwrap_or("");
        let filter_tags: Vec<String> = args["tags"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect())
            .unwrap_or_default();

        let targets: Vec<String> = args["targets"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect())
            .unwrap_or_else(|| {
                self.config.instances.iter()
                    .filter(|(n, ic)| {
                        n.as_str() != sender
                            && (filter_tags.is_empty()
                                || filter_tags.iter().any(|t| ic.tags.contains(t)))
                    })
                    .map(|(n, _)| n.clone())
                    .collect()
            });

        let mut sent = 0;
        for target in &targets {
            let send_args = json!({
                "instance_name": target,
                "message": message,
                "request_kind": args.get("request_kind"),
                "requires_reply": args.get("requires_reply"),
            });
            if self.handle_send_to_instance(sender, &send_args).is_ok() {
                sent += 1;
            }
        }

        Ok(json!({ "sent": sent, "targets": targets }))
    }

    fn handle_list_instances(&self, args: &Value) -> Result<Value, String> {
        let filter_tags: Vec<String> = args["tags"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect())
            .unwrap_or_default();

        // Merge fleet.yaml instances + runtime (dynamically created) instances
        let mut instances: Vec<Value> = self
            .config
            .instances
            .iter()
            .filter(|(_, ic)| {
                filter_tags.is_empty()
                    || filter_tags.iter().any(|t| ic.tags.contains(t))
            })
            .map(|(name, ic)| {
                json!({
                    "name": name,
                    "display_name": ic.display_name_or(name),
                    "backend": ic.backend_or(&self.config.defaults),
                    "working_directory": ic.working_directory.display().to_string(),
                    "description": ic.description,
                    "tags": ic.tags,
                    "topic_id": ic.topic_id,
                })
            })
            .collect();

        // Add runtime instances
        let runtime = self.runtime_instances.read().unwrap_or_else(|e| e.into_inner());
        for (name, ri) in runtime.iter() {
            if !self.config.instances.contains_key(name) {
                instances.push(json!({
                    "name": name,
                    "backend": ri.backend,
                    "working_directory": ri.working_directory,
                    "description": ri.description,
                    "tags": [],
                    "dynamic": true,
                }));
            }
        }

        Ok(json!(instances))
    }

    fn handle_describe_instance(&self, args: &Value) -> Result<Value, String> {
        let name = args["name"].as_str().unwrap_or("");
        match self.config.instances.get(name) {
            Some(ic) => Ok(json!({
                "name": name,
                "display_name": ic.display_name_or(name),
                "backend": ic.backend_or(&self.config.defaults),
                "working_directory": ic.working_directory.display().to_string(),
                "description": ic.description,
                "tags": ic.tags,
                "topic_id": ic.topic_id,
                "model": ic.model,
                "skip_permissions": ic.skip_permissions,
            })),
            None => Err(format!("instance not found: {name}")),
        }
    }

    fn handle_create_or_start_instance(
        &self,
        _caller: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let directory = args
            .get("directory")
            .or_else(|| args.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if directory.is_empty() {
            return Err("directory or name is required".into());
        }

        // For start_instance, look up existing config
        if let Some(ic) = self.config.instances.get(directory) {
            let backend = ic.backend_or(&self.config.defaults);
            let instance_dir = super::paths::instance_dir(directory);
            let socket_path = instance_dir.join("channel.sock");

            let zellij_binary = std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "zellij".into());

            let bcfg = super::backend::BackendConfig {
                instance_name: directory,
                display_name: ic.display_name.as_deref(),
                instance_dir: &instance_dir,
                working_directory: &ic.working_directory,
                mcp_server_binary: &zellij_binary,
                socket_path: &socket_path,
                system_prompt: None,
                skip_permissions: ic.skip_permissions,
                model: ic.model.as_deref().or(self.config.defaults.model.as_deref()),
                tool_set: "full",
                session_id: super::health::read_session_id(directory),
            };

            match super::backend::write_config(backend, &bcfg) {
                Ok(spawn) => {
                    let (cmd_binary, cmd_args) = {
                        let parts: Vec<&str> = spawn.command.split_whitespace().collect();
                        if parts.is_empty() {
                            return Err("empty command".into());
                        }
                        (
                            parts[0].to_owned(),
                            parts[1..].iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                        )
                    };

                    super::send_daemon_action(super::DaemonAction::NewTab {
                        name: directory.to_owned(),
                        command: cmd_binary,
                        args: cmd_args,
                        cwd: ic.working_directory.clone(),
                    });

                    log::info!("agend daemon: starting instance '{directory}'");
                    Ok(json!({"started": true, "name": directory}))
                },
                Err(e) => Err(format!("failed to write config: {e}")),
            }
        } else {
            // create_instance: new instance not in fleet.yaml
            let dir = args["directory"].as_str().unwrap_or(directory);
            let backend = args["backend"]
                .as_str()
                .unwrap_or(&self.config.defaults.backend);
            let name = args["topic_name"]
                .as_str()
                .or_else(|| {
                    std::path::Path::new(dir)
                        .file_name()
                        .and_then(|f| f.to_str())
                })
                .unwrap_or("new-instance");

            // Write full backend config (with --mcp-config etc.)
            let instance_dir = super::paths::instance_dir(name);
            let socket_path = instance_dir.join("channel.sock");
            let zellij_binary = std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "zellij".into());
            let work_dir = PathBuf::from(dir);

            let bcfg = super::backend::BackendConfig {
                instance_name: name,
                display_name: None,
                instance_dir: &instance_dir,
                working_directory: &work_dir,
                mcp_server_binary: &zellij_binary,
                socket_path: &socket_path,
                system_prompt: args["description"].as_str(),
                skip_permissions: true,
                model: args["model"].as_str().or(self.config.defaults.model.as_deref()),
                tool_set: "full",
                session_id: None,
            };

            match super::backend::write_config(backend, &bcfg) {
                Ok(spawn) => {
                    let parts: Vec<&str> = spawn.command.split_whitespace().collect();
                    if !parts.is_empty() {
                        super::send_daemon_action(super::DaemonAction::NewTab {
                            name: name.to_owned(),
                            command: parts[0].to_owned(),
                            args: parts[1..].iter().map(|s| s.to_string()).collect(),
                            cwd: work_dir,
                        });
                    }
                },
                Err(e) => return Err(format!("failed to write config: {e}")),
            }

            // Register in runtime instances so list_instances shows it
            if let Ok(mut rt) = self.runtime_instances.write() {
                rt.insert(name.to_owned(), RuntimeInstance {
                    name: name.to_owned(),
                    backend: backend.to_owned(),
                    working_directory: dir.to_owned(),
                    description: args["description"].as_str().map(|s| s.to_owned()),
                });
            }

            // Create Telegram forum topic and register in routing
            let mut topic_id: Option<i64> = None;
            if self.telegram.is_some() {
                match super::telegram::create_topic(&self.config, name) {
                    Ok(tid) => {
                        if let Ok(mut r) = self.routing.write() {
                            r.register(tid, name.to_owned());
                        }
                        topic_id = Some(tid);
                        log::info!("agend daemon: created Telegram topic {tid} for '{name}'");
                    },
                    Err(e) => {
                        log::warn!("agend daemon: failed to create Telegram topic for '{name}': {e}");
                    },
                }
            }

            log::info!("agend daemon: creating instance '{name}' at {dir}");
            Ok(json!({"created": true, "name": name, "directory": dir, "topic_id": topic_id}))
        }
    }

    fn handle_set_role(
        &self,
        caller: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let target = args["instance"].as_str().unwrap_or(caller);
        let role = args["role"].as_str().unwrap_or("");
        let append = args["append"].as_bool().unwrap_or(false);

        // Safety: agent cannot set its own role
        if target == caller {
            return Err("cannot set your own role — ask another instance or the user".into());
        }

        if role.is_empty() && !append {
            return Err("role text is required".into());
        }

        let role_path = super::paths::instance_dir(target).join("role.md");
        std::fs::create_dir_all(role_path.parent().unwrap())
            .map_err(|e| format!("mkdir: {e}"))?;

        if append {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&role_path)
                .map_err(|e| format!("open: {e}"))?;
            writeln!(f, "\n{}", role).map_err(|e| format!("write: {e}"))?;
        } else {
            std::fs::write(&role_path, role).map_err(|e| format!("write: {e}"))?;
        }

        log::info!("agend daemon: set_role for '{}' (append={})", target, append);
        Ok(json!({"ok": true, "instance": target, "append": append}))
    }

    fn handle_get_role(&self, args: &Value) -> Result<Value, String> {
        let target = args["instance"].as_str().unwrap_or("");
        if target.is_empty() {
            return Err("instance name is required".into());
        }
        let role_path = super::paths::instance_dir(target).join("role.md");
        match std::fs::read_to_string(&role_path) {
            Ok(content) => Ok(json!({"instance": target, "role": content})),
            Err(_) => Ok(json!({"instance": target, "role": null})),
        }
    }

    fn handle_checkout_repo(&self, args: &Value) -> Result<Value, String> {
        let source = args["source"].as_str().unwrap_or("");
        let branch = args["branch"].as_str().unwrap_or("HEAD");

        if source.is_empty() {
            return Err("source is required".into());
        }

        // Resolve source: could be instance name or absolute path
        let repo_path = if let Some(ic) = self.config.instances.get(source) {
            ic.working_directory.clone()
        } else {
            let expanded = if source.starts_with('~') {
                let home = std::env::var("HOME").unwrap_or_default();
                PathBuf::from(source.replacen('~', &home, 1))
            } else {
                PathBuf::from(source)
            };
            if !expanded.exists() {
                return Err(format!("path not found: {source}"));
            }
            expanded
        };

        // Create a git worktree for read-only access
        let worktree_base = super::paths::agend_home().join("worktrees");
        std::fs::create_dir_all(&worktree_base).map_err(|e| format!("mkdir: {e}"))?;

        let worktree_name = format!(
            "{}-{}",
            repo_path.file_name().and_then(|f| f.to_str()).unwrap_or("repo"),
            &uuid::Uuid::new_v4().to_string()[..8]
        );
        let worktree_path = worktree_base.join(&worktree_name);

        let output = std::process::Command::new("git")
            .args(["worktree", "add", "--detach", &worktree_path.to_string_lossy(), branch])
            .current_dir(&repo_path)
            .output()
            .map_err(|e| format!("git worktree add: {e}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("git worktree add failed: {stderr}"));
        }

        log::info!("agend daemon: checked out {} ({}) → {}", source, branch, worktree_path.display());
        Ok(json!({
            "path": worktree_path.to_string_lossy(),
            "source": source,
            "branch": branch,
        }))
    }

    fn handle_release_repo(&self, args: &Value) -> Result<Value, String> {
        let path = args["path"].as_str().unwrap_or("");
        if path.is_empty() {
            return Err("path is required".into());
        }

        // Verify the path is under our worktrees directory
        let worktree_base = super::paths::agend_home().join("worktrees");
        let path = PathBuf::from(path);
        if !path.starts_with(&worktree_base) {
            return Err("path is not a managed worktree".into());
        }

        let output = std::process::Command::new("git")
            .args(["worktree", "remove", "--force", &path.to_string_lossy()])
            .output()
            .map_err(|e| format!("git worktree remove: {e}"))?;

        if !output.status.success() {
            // Fallback: just remove the directory
            let _ = std::fs::remove_dir_all(&path);
        }

        log::info!("agend daemon: released worktree {}", path.display());
        Ok(json!({"released": true, "path": path.to_string_lossy()}))
    }

    fn handle_task(&self, instance_name: &str, args: &Value) -> Result<Value, String> {
        let action = args["action"].as_str().unwrap_or("");
        match action {
            "create" => {
                let deps: Vec<String> = args["depends_on"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_owned())).collect())
                    .unwrap_or_default();
                match self.db.create_task(
                    args["title"].as_str().unwrap_or(""),
                    args["description"].as_str(),
                    args["priority"].as_str().unwrap_or("normal"),
                    args["assignee"].as_str(),
                    instance_name,
                    &deps,
                ) {
                    Ok(t) => Ok(serde_json::to_value(t).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "list" => {
                match self.db.list_tasks(
                    args["filter_assignee"].as_str(),
                    args["filter_status"].as_str(),
                ) {
                    Ok(list) => Ok(serde_json::to_value(list).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "claim" => {
                let id = args["id"].as_str().unwrap_or("");
                match self.db.claim_task(id, instance_name) {
                    Ok(t) => Ok(serde_json::to_value(t).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "done" => {
                let id = args["id"].as_str().unwrap_or("");
                match self.db.complete_task(id, args["result"].as_str()) {
                    Ok(t) => Ok(serde_json::to_value(t).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            "update" => {
                let id = args["id"].as_str().unwrap_or("");
                match self.db.update_task(
                    id,
                    args["status"].as_str(),
                    args["priority"].as_str(),
                    args["assignee"].as_str(),
                ) {
                    Ok(t) => Ok(serde_json::to_value(t).unwrap()),
                    Err(e) => Err(format!("db error: {e}")),
                }
            },
            _ => Err(format!("unknown task action: {action}")),
        }
    }

    fn handle_download_attachment(&self, args: &Value) -> Result<Value, String> {
        let file_id = args["file_id"].as_str().unwrap_or("");
        if file_id.is_empty() {
            return Err("file_id is required".into());
        }

        // Get bot token from config
        let bot_token_env = self.config.channel.as_ref()
            .and_then(|c| c.bot_token_env.as_deref())
            .unwrap_or("AGEND_BOT_TOKEN");
        let bot_token = std::env::var(bot_token_env)
            .map_err(|_| format!("bot token not set (env: {bot_token_env})"))?;

        // Use Telegram Bot API to download
        let base_url = format!("https://api.telegram.org/bot{}", bot_token);

        // Step 1: getFile to get file_path
        let get_file_url = format!("{}/getFile", base_url);
        let get_file_body = serde_json::to_string(&json!({"file_id": file_id}))
            .map_err(|e| format!("json: {e}"))?;
        let req = isahc::Request::post(&get_file_url)
            .header("Content-Type", "application/json")
            .body(get_file_body)
            .map_err(|e| format!("build: {e}"))?;
        let mut resp = isahc::send(req).map_err(|e| format!("send: {e}"))?;
        let resp_text = resp.text().map_err(|e| format!("read: {e}"))?;
        let parsed: Value = serde_json::from_str(&resp_text).map_err(|e| format!("parse: {e}"))?;
        let tg_file_path = parsed["result"]["file_path"]
            .as_str()
            .ok_or("no file_path in Telegram response")?;

        // Step 2: download file
        let filename = tg_file_path.rsplit('/').next().unwrap_or("attachment");
        let inbox = super::paths::agend_home().join("inbox");
        std::fs::create_dir_all(&inbox).map_err(|e| format!("mkdir: {e}"))?;
        let local_path = inbox.join(format!("{}_{}", chrono::Utc::now().timestamp(), filename));

        let download_url = format!("https://api.telegram.org/file/bot{}/{}", bot_token, tg_file_path);
        let mut dl_resp = isahc::get(&download_url).map_err(|e| format!("download: {e}"))?;
        let bytes = dl_resp.bytes().map_err(|e| format!("read file: {e}"))?;
        std::fs::write(&local_path, &bytes).map_err(|e| format!("write: {e}"))?;

        log::info!("agend daemon: downloaded attachment {} → {}", file_id, local_path.display());
        Ok(json!(local_path.to_string_lossy()))
    }

    fn route_to_telegram(&self, tool: &str, args: &Value) -> Result<Value, String> {
        let telegram = self.telegram.as_ref().ok_or("Telegram not configured")?;
        let action = match tool {
            "reply" => OutboundAction::SendText {
                chat_id: args["chat_id"].as_str().unwrap_or("").into(),
                text: args["text"].as_str().unwrap_or("").into(),
                thread_id: args["thread_id"].as_str().map(|s| s.into()),
                reply_to: args["reply_to"].as_str().map(|s| s.into()),
                format: args["format"].as_str().map(|s| s.into()),
            },
            "edit_message" => OutboundAction::EditMessage {
                chat_id: args["chat_id"].as_str().unwrap_or("").into(),
                message_id: args["message_id"].as_str().unwrap_or("").into(),
                text: args["text"].as_str().unwrap_or("").into(),
            },
            "react" => OutboundAction::React {
                chat_id: args["chat_id"].as_str().unwrap_or("").into(),
                message_id: args["message_id"].as_str().unwrap_or("").into(),
                emoji: args["emoji"].as_str().unwrap_or("").into(),
            },
            _ => return Err(format!("unsupported channel tool: {tool}")),
        };
        telegram
            .outbound_tx
            .try_send(action)
            .map_err(|e| format!("telegram send failed: {e}"))?;
        Ok(json!("ok"))
    }

    fn handle_telegram_inbound(&self, msg: super::telegram::InboundMessage) {
        if let Some(ref target) = msg.target_instance {
            let thread_id = msg.thread_id.as_deref().unwrap_or("");
            let attachment_info = msg.attachment_file_id.as_ref()
                .map(|fid| format!(" attachment_file_id:{}", fid))
                .unwrap_or_default();
            let formatted = format!(
                "[user:{} chat_id:{} thread_id:{}{}] {}\n(Reply using the reply tool with chat_id=\"{}\")\n",
                msg.username, msg.chat_id, thread_id, attachment_info, msg.text, msg.chat_id
            );
            inject_message_to_instance(target, &formatted);
            log::info!(
                "agend daemon: telegram {} → {}: {}",
                msg.username, target, &msg.text[..msg.text.len().min(100)]
            );
        } else {
            log::debug!("agend daemon: telegram message without target instance, ignoring");
        }
    }

    fn working_dir_for(&self, instance_name: &str) -> String {
        self.config
            .instances
            .get(instance_name)
            .map(|ic| ic.working_directory.display().to_string())
            .unwrap_or_default()
    }
}

/// Inject a formatted message into an instance's terminal pane.
/// Uses the global terminal registry to find the terminal_id,
/// then sends a DaemonAction::Write via the global channel.
///
/// For OpenCode instances (detected by opencode-binary marker file),
/// wraps the message as an `opencode run --continue` command instead
/// of injecting raw text, since OpenCode's TUI doesn't accept raw input
/// in daemon mode.
fn inject_message_to_instance(instance_name: &str, formatted_text: &str) {
    if let Some(tid) = super::terminal_for_instance(instance_name) {
        let instance_dir = super::paths::instance_dir(instance_name);
        let oc_binary_path = instance_dir.join("opencode-binary");

        let inject_text = if oc_binary_path.exists() {
            // OpenCode instance: spawn `opencode run --continue` as a subprocess
            // directly from the daemon, bypassing terminal injection entirely.
            // This is more reliable than injecting commands into the pane shell.
            let oc_binary = std::fs::read_to_string(&oc_binary_path)
                .unwrap_or_else(|_| "opencode".into())
                .trim()
                .to_owned();
            // Resolve working directory from instance config
            let work_dir = instance_dir.join("..").join(".."); // fallback
            // Read working directory from instance.json if available
            let work_dir = std::fs::read_to_string(instance_dir.join("instance.json"))
                .ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| v["working_directory"].as_str().map(|s| std::path::PathBuf::from(s)))
                .unwrap_or(work_dir);

            let msg = formatted_text.to_owned();
            let name = instance_name.to_owned();
            std::thread::Builder::new()
                .name(format!("oc_run_{name}"))
                .spawn(move || {
                    log::info!("agend: spawning opencode run for '{name}'");
                    match std::process::Command::new(&oc_binary)
                        .args(["run", "--continue", &msg])
                        .current_dir(&work_dir)
                        .env("PATH", format!("{}:{}",
                            std::path::Path::new(&oc_binary).parent().unwrap_or(std::path::Path::new("")).display(),
                            std::env::var("PATH").unwrap_or_default()))
                        .output()
                    {
                        Ok(output) => {
                            if !output.status.success() {
                                let stderr = String::from_utf8_lossy(&output.stderr);
                                log::error!("agend: opencode run failed for '{name}': {stderr}");
                            } else {
                                log::info!("agend: opencode run completed for '{name}'");
                            }
                        },
                        Err(e) => log::error!("agend: failed to spawn opencode for '{name}': {e}"),
                    }
                })
                .ok();
            return; // Don't inject into terminal
        } else {
            // Other backends (Claude Code, etc.): inject raw text
            formatted_text.to_owned()
        };

        let mut bytes = Vec::with_capacity(inject_text.len() + 1);
        bytes.extend_from_slice(inject_text.as_bytes());
        bytes.push(b'\r'); // Enter to submit
        super::send_daemon_action(super::DaemonAction::Write(tid, bytes));

        log::debug!(
            "agend daemon: injected {} bytes into terminal {} (instance '{}')",
            inject_text.len(), tid, instance_name
        );
    } else {
        log::warn!(
            "agend daemon: no terminal registered for instance '{}', message dropped",
            instance_name
        );
    }
}

fn result_to_response(request_id: u64, result: Result<Value, String>) -> IpcResponse {
    match result {
        Ok(v) => IpcResponse {
            request_id,
            result: Some(v),
            error: None,
        },
        Err(e) => IpcResponse {
            request_id,
            result: None,
            error: Some(e),
        },
    }
}
