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
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// The daemon manages all IPC servers and routes tool calls.
pub struct Daemon {
    config: FleetConfig,
    db: AgendDb,
    routing: Arc<RwLock<RoutingEngine>>,
    /// Instance name → IPC request receiver
    ipc_receivers: HashMap<String, Receiver<IpcRequest>>,
    /// Telegram adapter sender (if configured)
    telegram: Option<TelegramSender>,
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

        // Start Telegram adapter if configured
        let telegram = TelegramAdapter::from_config(&config).map(|adapter| {
            log::info!("agend daemon: starting Telegram adapter");
            adapter.run()
        });

        Self {
            config,
            db,
            routing: Arc::new(RwLock::new(routing)),
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
            "reply" | "react" | "edit_message" | "download_attachment" => {
                self.route_to_telegram(tool, args)
            },

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

            // ── Repo checkout (stub) ────────────────────────────────────
            "checkout_repo" | "release_repo" => {
                Err(format!("tool '{tool}' not yet implemented in agend-rs"))
            },

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
        let request_kind = args.get("request_kind").and_then(|v| v.as_str()).unwrap_or("update");
        let formatted = format!(
            "[from:{}] {}\n(Reply using send_to_instance tool, NOT direct text)\n",
            sender, message
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

        let instances: Vec<Value> = self
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
                    "backend": ic.backend_or(&self.config.defaults),
                    "working_directory": ic.working_directory.display().to_string(),
                    "description": ic.description,
                    "tags": ic.tags,
                    "topic_id": ic.topic_id,
                })
            })
            .collect();
        Ok(json!(instances))
    }

    fn handle_describe_instance(&self, args: &Value) -> Result<Value, String> {
        let name = args["name"].as_str().unwrap_or("");
        match self.config.instances.get(name) {
            Some(ic) => Ok(json!({
                "name": name,
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

            // Create a simple tab with the backend command
            let (cmd, cmd_args) = super::fleet::simple_command(backend);
            super::send_daemon_action(super::DaemonAction::NewTab {
                name: name.to_owned(),
                command: cmd,
                args: cmd_args,
                cwd: PathBuf::from(dir),
            });

            log::info!("agend daemon: creating instance '{name}' at {dir}");
            Ok(json!({"created": true, "name": name, "directory": dir}))
        }
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
            let formatted = format!(
                "[user:{} chat_id:{} thread_id:{}] {}\n(Reply using the reply tool with chat_id=\"{}\")\n",
                msg.username, msg.chat_id, thread_id, msg.text, msg.chat_id
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
fn inject_message_to_instance(instance_name: &str, formatted_text: &str) {
    if let Some(tid) = super::terminal_for_instance(instance_name) {
        // Type text directly into the terminal (like tmux send-keys -l).
        // Do NOT use bracketed paste — Claude Code's TUI handles pasted
        // text differently and may not submit on Enter after paste-end.
        let mut bytes = Vec::with_capacity(formatted_text.len() + 1);
        bytes.extend_from_slice(formatted_text.as_bytes());
        bytes.push(b'\r'); // Enter to submit
        super::send_daemon_action(super::DaemonAction::Write(tid, bytes));

        super::debug_log(&format!("inject: tid={} instance={} len={}", tid, instance_name, formatted_text.len()));
        log::debug!(
            "agend daemon: injected {} bytes into terminal {} (instance '{}')",
            formatted_text.len(), tid, instance_name
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
