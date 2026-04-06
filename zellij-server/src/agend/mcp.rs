//! MCP server — JSON-RPC over stdio for tool integration.
//!
//! Implements the Model Context Protocol (MCP) so that CLI agents (Claude Code,
//! etc.) can call AgEnD tools (reply, send_to_instance, list_instances, ...).
//!
//! Communication:
//!   Claude Code ←(JSON-RPC/stdio)→ this process ←(Unix socket)→ agend daemon

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};

// ── JSON-RPC types ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }
    fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError { code, message }),
        }
    }
}

// ── MCP Tool definitions ────────────────────────────────────────────────

/// Admin-only tools excluded from "standard" profile.
const ADMIN_TOOLS: &[&str] = &[
    "create_instance", "delete_instance", "start_instance",
    "set_role", "create_team", "update_team", "delete_team",
    "create_schedule", "update_schedule", "delete_schedule",
    "checkout_repo", "release_repo",
];

/// Tools included in "minimal" profile.
const MINIMAL_TOOLS: &[&str] = &[
    "reply", "react", "edit_message",
    "send_to_instance", "list_instances",
];

fn tool_definitions() -> Value {
    json!({
        "tools": [
            {
                "name": "reply",
                "description": "Reply on the channel. Pass chat_id and thread_id from the inbound <channel> block.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "chat_id": {"type": "string", "description": "chat_id from the inbound <channel> block"},
                        "text": {"type": "string"},
                        "reply_to": {"type": "string", "description": "Message ID to thread under"},
                        "thread_id": {"type": "string", "description": "Topic thread ID from the inbound <channel> block"},
                        "files": {"type": "array", "items": {"type": "string"}, "description": "Absolute file paths to attach"},
                        "format": {"type": "string", "enum": ["text", "markdown"], "description": "Rendering mode. Default: 'text'"}
                    },
                    "required": ["chat_id", "text"]
                }
            },
            {
                "name": "react",
                "description": "Add an emoji reaction to a channel message.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "chat_id": {"type": "string"},
                        "message_id": {"type": "string"},
                        "emoji": {"type": "string"}
                    },
                    "required": ["chat_id", "message_id", "emoji"]
                }
            },
            {
                "name": "edit_message",
                "description": "Edit a message the bot previously sent.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "chat_id": {"type": "string"},
                        "message_id": {"type": "string"},
                        "text": {"type": "string"},
                        "format": {"type": "string", "enum": ["text", "markdown"]}
                    },
                    "required": ["chat_id", "message_id", "text"]
                }
            },
            {
                "name": "download_attachment",
                "description": "Download a file attachment from a channel message. Returns the local file path.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "file_id": {"type": "string", "description": "The attachment_file_id from inbound meta"}
                    },
                    "required": ["file_id"]
                }
            },
            {
                "name": "send_to_instance",
                "description": "Send a message to another instance for cross-instance communication.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "instance_name": {"type": "string", "description": "Target instance name"},
                        "message": {"type": "string", "description": "The message to send"},
                        "request_kind": {"type": "string", "enum": ["query", "task", "report", "update"]},
                        "requires_reply": {"type": "boolean"},
                        "correlation_id": {"type": "string"},
                        "task_summary": {"type": "string"},
                        "working_directory": {"type": "string"},
                        "branch": {"type": "string"}
                    },
                    "required": ["instance_name", "message"]
                }
            },
            {
                "name": "broadcast",
                "description": "Send a message to multiple instances at once. Use tags to filter by role/project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "message": {"type": "string"},
                        "targets": {"type": "array", "items": {"type": "string"}, "description": "Specific instance names. Omit to use tags or send to all."},
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "Filter by tags (e.g. ['dev'], ['reviewer']). Only used when targets is omitted."},
                        "team": {"type": "string", "description": "Filter by team name. Only used when targets is omitted."},
                        "task_summary": {"type": "string"},
                        "request_kind": {"type": "string", "enum": ["query", "task", "update"]},
                        "requires_reply": {"type": "boolean"}
                    },
                    "required": ["message"]
                }
            },
            {
                "name": "list_instances",
                "description": "List all currently running instances. Optionally filter by tags.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "Filter by tags (e.g. ['dev'], ['reviewer']). Returns instances matching ANY tag."}
                    }
                }
            },
            {
                "name": "describe_instance",
                "description": "Get detailed information about a specific instance.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Instance name to describe"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "start_instance",
                "description": "Start a stopped instance by name.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "create_instance",
                "description": "Create a new instance bound to a project directory.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "directory": {"type": "string", "description": "Absolute path to the project directory"},
                        "topic_name": {"type": "string"},
                        "description": {"type": "string"},
                        "model": {"type": "string", "enum": ["sonnet", "opus", "haiku"]},
                        "backend": {"type": "string", "enum": ["claude-code", "gemini-cli", "codex", "opencode"]},
                        "branch": {"type": "string"},
                        "detach": {"type": "boolean"}
                    },
                    "required": ["directory"]
                }
            },
            {
                "name": "delete_instance",
                "description": "Delete an instance: stop daemon, remove config.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "delete_topic": {"type": "boolean"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "request_information",
                "description": "Ask another instance a question and expect a reply.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target_instance": {"type": "string"},
                        "question": {"type": "string"},
                        "context": {"type": "string"}
                    },
                    "required": ["target_instance", "question"]
                }
            },
            {
                "name": "delegate_task",
                "description": "Delegate a task to another instance.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target_instance": {"type": "string"},
                        "task": {"type": "string"},
                        "success_criteria": {"type": "string"},
                        "context": {"type": "string"}
                    },
                    "required": ["target_instance", "task"]
                }
            },
            {
                "name": "report_result",
                "description": "Report results back to the requesting instance.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target_instance": {"type": "string"},
                        "correlation_id": {"type": "string"},
                        "summary": {"type": "string"},
                        "artifacts": {"type": "string"}
                    },
                    "required": ["target_instance", "summary"]
                }
            },
            {
                "name": "post_decision",
                "description": "Record a project or fleet-wide decision.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                        "scope": {"type": "string", "enum": ["project", "fleet"]},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "ttl_days": {"type": "number"},
                        "supersedes": {"type": "string"}
                    },
                    "required": ["title", "content"]
                }
            },
            {
                "name": "list_decisions",
                "description": "List active decisions for this project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "include_archived": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}}
                    }
                }
            },
            {
                "name": "update_decision",
                "description": "Update or archive an existing decision.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "content": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "ttl_days": {"type": "number"},
                        "archive": {"type": "boolean"}
                    },
                    "required": ["id"]
                }
            },
            {
                "name": "task",
                "description": "Manage fleet task board. Actions: create, list, claim, done, update.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "action": {"type": "string", "enum": ["create", "list", "claim", "done", "update"]},
                        "title": {"type": "string"},
                        "description": {"type": "string"},
                        "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]},
                        "assignee": {"type": "string"},
                        "depends_on": {"type": "array", "items": {"type": "string"}},
                        "id": {"type": "string"},
                        "result": {"type": "string"},
                        "status": {"type": "string", "enum": ["open", "claimed", "done", "blocked", "cancelled"]},
                        "filter_assignee": {"type": "string"},
                        "filter_status": {"type": "string"}
                    },
                    "required": ["action"]
                }
            },
            {
                "name": "create_schedule",
                "description": "Create a cron-based schedule.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cron": {"type": "string"},
                        "message": {"type": "string"},
                        "target": {"type": "string"},
                        "label": {"type": "string"},
                        "timezone": {"type": "string"}
                    },
                    "required": ["cron", "message"]
                }
            },
            {
                "name": "list_schedules",
                "description": "List all schedules.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target": {"type": "string"}
                    }
                }
            },
            {
                "name": "update_schedule",
                "description": "Update an existing schedule.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "cron": {"type": "string"},
                        "message": {"type": "string"},
                        "target": {"type": "string"},
                        "label": {"type": "string"},
                        "timezone": {"type": "string"},
                        "enabled": {"type": "boolean"}
                    },
                    "required": ["id"]
                }
            },
            {
                "name": "delete_schedule",
                "description": "Delete a schedule by ID.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"}
                    },
                    "required": ["id"]
                }
            },
            {
                "name": "checkout_repo",
                "description": "Mount another repo as a read-only worktree.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "branch": {"type": "string"}
                    },
                    "required": ["source"]
                }
            },
            {
                "name": "release_repo",
                "description": "Remove a previously checked-out repo worktree.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"}
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "set_role",
                "description": "Set the role/system prompt for another instance. Cannot set your own role.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "instance": {"type": "string", "description": "Target instance name"},
                        "role": {"type": "string", "description": "Role description text"},
                        "append": {"type": "boolean", "description": "Append to existing role instead of replacing. Default: false"}
                    },
                    "required": ["instance", "role"]
                }
            },
            {
                "name": "get_role",
                "description": "Get the current role/system prompt for an instance.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "instance": {"type": "string", "description": "Instance name"}
                    },
                    "required": ["instance"]
                }
            },
            {
                "name": "create_team",
                "description": "Create a team for grouping instances.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "description": {"type": "string"},
                        "members": {"type": "array", "items": {"type": "string"}, "description": "Instance names"}
                    },
                    "required": ["name", "members"]
                }
            },
            {
                "name": "list_teams",
                "description": "List all teams.",
                "inputSchema": {"type": "object", "properties": {}}
            },
            {
                "name": "update_team",
                "description": "Update a team's description or members.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "description": {"type": "string"},
                        "members": {"type": "array", "items": {"type": "string"}}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "delete_team",
                "description": "Delete a team.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "list_events",
                "description": "Query the event log. Returns recent events for auditing and observability.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "instance": {"type": "string", "description": "Filter by instance name"},
                        "event_type": {"type": "string", "description": "Filter by event type (e.g. telegram_message, instance_created, crash_respawn)"},
                        "since": {"type": "string", "description": "ISO 8601 timestamp to filter events after"},
                        "limit": {"type": "number", "description": "Max events to return (default 50)"}
                    }
                }
            }
        ]
    })
}

// ── IPC bridge to agend daemon ──────────────────────────────────────────

mod ipc {
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

    #[derive(Debug, Serialize)]
    struct IpcRequest {
        #[serde(rename = "type")]
        msg_type: String,
        tool: String,
        args: Value,
        #[serde(rename = "requestId")]
        request_id: u64,
    }

    #[derive(Debug, Deserialize)]
    struct IpcResponse {
        #[serde(rename = "requestId")]
        request_id: u64,
        result: Option<Value>,
        error: Option<String>,
    }

    /// Send a tool call to the daemon via Unix socket and wait for response.
    pub fn call_daemon(
        socket_path: &str,
        tool: &str,
        args: &Value,
    ) -> Result<Value, String> {
        let stream = UnixStream::connect(socket_path)
            .map_err(|e| format!("failed to connect to daemon socket: {e}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .ok();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .ok();

        let rid = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let request = IpcRequest {
            msg_type: "tool_call".into(),
            tool: tool.into(),
            args: args.clone(),
            request_id: rid,
        };

        let mut writer = stream.try_clone().map_err(|e| format!("clone: {e}"))?;
        let msg = serde_json::to_string(&request).map_err(|e| format!("serialize: {e}"))?;
        writer
            .write_all(msg.as_bytes())
            .map_err(|e| format!("write: {e}"))?;
        writer
            .write_all(b"\n")
            .map_err(|e| format!("write newline: {e}"))?;
        writer.flush().map_err(|e| format!("flush: {e}"))?;

        let reader = BufReader::new(stream);
        for line in reader.lines() {
            let line = line.map_err(|e| format!("read: {e}"))?;
            if line.is_empty() {
                continue;
            }
            let resp: IpcResponse =
                serde_json::from_str(&line).map_err(|e| format!("parse response: {e}"))?;
            if resp.request_id == rid {
                if let Some(err) = resp.error {
                    return Err(err);
                }
                return Ok(resp.result.unwrap_or(Value::Null));
            }
        }
        Err("daemon connection closed without response".into())
    }
}

// ── Tool call handler ───────────────────────────────────────────────────

fn handle_tool_call(tool_name: &str, arguments: &Value) -> Value {
    let socket_path = std::env::var("AGEND_SOCKET_PATH").unwrap_or_default();

    if socket_path.is_empty() {
        // No daemon connection — return stub responses for testing
        return json!({
            "content": [{
                "type": "text",
                "text": format!("agend daemon not connected (AGEND_SOCKET_PATH not set). Tool '{}' called with: {}", tool_name, arguments)
            }]
        });
    }

    match ipc::call_daemon(&socket_path, tool_name, arguments) {
        Ok(result) => {
            let text = match result {
                Value::String(s) => s,
                other => serde_json::to_string_pretty(&other).unwrap_or_default(),
            };
            json!({
                "content": [{"type": "text", "text": text}]
            })
        },
        Err(e) => {
            json!({
                "content": [{"type": "text", "text": format!("error: {e}")}],
                "isError": true
            })
        },
    }
}

// ── Main server loop ────────────────────────────────────────────────────

/// Run the MCP server, reading JSON-RPC from stdin and writing to stdout.
pub fn run_stdio_server() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let reader = stdin.lock();
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse::error(
                    Value::Null,
                    -32700,
                    format!("Parse error: {e}"),
                );
                let _ = writeln!(stdout, "{}", serde_json::to_string(&resp).unwrap());
                let _ = stdout.flush();
                continue;
            },
        };

        let id = request.id.clone().unwrap_or(Value::Null);

        // Notifications (no id) — just ignore
        if request.id.is_none() {
            continue;
        }

        let response = match request.method.as_str() {
            "initialize" => JsonRpcResponse::success(
                id,
                json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "agend",
                        "version": "0.3.0"
                    }
                }),
            ),

            "tools/list" => {
                let profile = std::env::var("AGEND_TOOL_SET").unwrap_or_else(|_| "full".into());
                let mut defs = tool_definitions();
                if profile != "full" {
                    if let Some(tools) = defs.get_mut("tools").and_then(|t| t.as_array_mut()) {
                        tools.retain(|tool| {
                            let name = tool["name"].as_str().unwrap_or("");
                            match profile.as_str() {
                                "minimal" => MINIMAL_TOOLS.contains(&name),
                                "standard" => !ADMIN_TOOLS.contains(&name),
                                _ => true, // unknown profile = full
                            }
                        });
                    }
                }
                JsonRpcResponse::success(id, defs)
            },

            "tools/call" => {
                let tool_name = request
                    .params
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let arguments = request
                    .params
                    .get("arguments")
                    .cloned()
                    .unwrap_or(json!({}));

                let result = handle_tool_call(tool_name, &arguments);
                JsonRpcResponse::success(id, result)
            },

            "ping" => JsonRpcResponse::success(id, json!({})),

            other => JsonRpcResponse::error(
                id,
                -32601,
                format!("Method not found: {other}"),
            ),
        };

        let resp_str = serde_json::to_string(&response).unwrap();
        let _ = writeln!(stdout, "{}", resp_str);
        let _ = stdout.flush();
    }
}

// ── MCP config writer ───────────────────────────────────────────────────

/// Generate the mcp-config.json content for a CLI agent instance.
/// The `zellij_binary` path should point to the zellij binary with agend feature.
/// The `socket_path` is the Unix socket for IPC with the daemon.
pub fn generate_mcp_config(zellij_binary: &str, socket_path: &str, tool_set: &str) -> Value {
    json!({
        "mcpServers": {
            "agend": {
                "command": zellij_binary,
                "args": ["agend-mcp-server"],
                "env": {
                    "AGEND_SOCKET_PATH": socket_path,
                    "AGEND_TOOL_SET": tool_set
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_definitions_has_all_tools() {
        let defs = tool_definitions();
        let tools = defs["tools"].as_array().unwrap();
        assert!(tools.len() >= 24, "expected at least 24 tools, got {}", tools.len());

        // Verify core tools exist
        let names: Vec<&str> = tools
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"reply"));
        assert!(names.contains(&"send_to_instance"));
        assert!(names.contains(&"list_instances"));
        assert!(names.contains(&"broadcast"));
        assert!(names.contains(&"task"));
        assert!(names.contains(&"post_decision"));
    }

    #[test]
    fn mcp_config_generation() {
        let config = generate_mcp_config("/usr/local/bin/zellij", "/tmp/test.sock", "full");
        let server = &config["mcpServers"]["agend"];
        assert_eq!(server["command"], "/usr/local/bin/zellij");
        assert_eq!(server["args"][0], "agend-mcp-server");
        assert_eq!(server["env"]["AGEND_SOCKET_PATH"], "/tmp/test.sock");
    }

    #[test]
    fn handle_tool_call_without_daemon() {
        // Without AGEND_SOCKET_PATH, should return stub response
        std::env::remove_var("AGEND_SOCKET_PATH");
        let result = handle_tool_call("list_instances", &json!({}));
        let text = result["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("not connected"));
    }

    #[test]
    fn json_rpc_protocol() {
        // Test initialize request parsing
        let req_str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let req: JsonRpcRequest = serde_json::from_str(req_str).unwrap();
        assert_eq!(req.method, "initialize");

        // Test response serialization
        let resp = JsonRpcResponse::success(json!(1), json!({"test": true}));
        let resp_str = serde_json::to_string(&resp).unwrap();
        assert!(resp_str.contains("\"jsonrpc\":\"2.0\""));
        assert!(resp_str.contains("\"result\""));
    }
}
