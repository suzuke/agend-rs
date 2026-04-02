//! IPC server — Unix socket for MCP server ↔ daemon communication.
//!
//! Protocol: newline-delimited JSON over Unix domain socket.

use crossbeam::channel::{self, Receiver, Sender};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// A message received from an MCP server client.
#[derive(Debug, Clone, Deserialize)]
pub struct IpcMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub tool: Option<String>,
    pub args: Option<Value>,
    #[serde(rename = "requestId")]
    pub request_id: Option<u64>,
    /// For cross-instance routing
    #[serde(rename = "fleetRequestId")]
    pub fleet_request_id: Option<String>,
    /// MCP ready signal
    #[serde(rename = "sessionName")]
    pub session_name: Option<String>,
}

/// A response to send back to the MCP server client.
#[derive(Debug, Clone, Serialize)]
pub struct IpcResponse {
    #[serde(rename = "requestId")]
    pub request_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Inbound request from MCP server, with reply handle.
pub struct IpcRequest {
    pub instance_name: String,
    pub message: IpcMessage,
    pub reply_tx: Sender<IpcResponse>,
}

/// IPC server for a single instance.
pub struct IpcServer {
    socket_path: PathBuf,
    request_tx: Sender<IpcRequest>,
}

impl IpcServer {
    /// Create a new IPC server. Returns the server and a receiver for incoming requests.
    pub fn new(socket_path: PathBuf) -> (Self, Receiver<IpcRequest>) {
        let (tx, rx) = channel::bounded(256);
        (Self { socket_path, request_tx: tx }, rx)
    }

    /// Start listening in a background thread.
    pub fn start(self, instance_name: String) {
        let socket_path = self.socket_path.clone();
        let request_tx = self.request_tx;

        std::thread::Builder::new()
            .name(format!("ipc_{instance_name}"))
            .spawn(move || {
                // Clean up stale socket
                let _ = std::fs::remove_file(&socket_path);

                let listener = match UnixListener::bind(&socket_path) {
                    Ok(l) => l,
                    Err(e) => {
                        log::error!("agend ipc: failed to bind {}: {e}", socket_path.display());
                        return;
                    },
                };

                // Set permissions (owner only)
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(
                        &socket_path,
                        std::fs::Permissions::from_mode(0o600),
                    );
                }

                log::info!(
                    "agend ipc: listening on {} for instance '{}'",
                    socket_path.display(),
                    instance_name
                );

                for stream in listener.incoming() {
                    match stream {
                        Ok(stream) => {
                            let tx = request_tx.clone();
                            let name = instance_name.clone();
                            std::thread::spawn(move || {
                                handle_client(stream, &name, &tx);
                            });
                        },
                        Err(e) => {
                            log::error!("agend ipc: accept error: {e}");
                        },
                    }
                }
            })
            .expect("failed to spawn IPC server thread");
    }
}

fn handle_client(stream: UnixStream, instance_name: &str, request_tx: &Sender<IpcRequest>) {
    let reader = BufReader::new(stream.try_clone().unwrap());
    let writer = Arc::new(Mutex::new(stream));

    for line in reader.lines() {
        let line = match line {
            Ok(l) if !l.is_empty() => l,
            Ok(_) => continue,
            Err(_) => break,
        };

        let msg: IpcMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("agend ipc: parse error from {instance_name}: {e}");
                continue;
            },
        };

        let request_id = msg.request_id.unwrap_or(0);

        // Create a per-request reply channel
        let (reply_tx, reply_rx) = channel::bounded(1);

        let req = IpcRequest {
            instance_name: instance_name.to_owned(),
            message: msg,
            reply_tx,
        };

        if request_tx.send(req).is_err() {
            break;
        }

        // Wait for response and send back
        if let Ok(response) = reply_rx.recv() {
            let w = writer.clone();
            let mut guard = match w.lock() {
                Ok(g) => g,
                Err(_) => break,
            };
            let resp_str = serde_json::to_string(&response).unwrap();
            let _ = writeln!(guard, "{}", resp_str);
            let _ = guard.flush();
        }
    }
}

/// Send a message to an instance via its IPC socket.
pub fn send_to_instance_socket(
    socket_path: &Path,
    msg_type: &str,
    content: &str,
    meta: &Value,
) -> Result<(), String> {
    let stream = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect: {e}"))?;
    let mut writer = stream;
    let msg = serde_json::json!({
        "type": msg_type,
        "content": content,
        "meta": meta,
    });
    let line = serde_json::to_string(&msg).map_err(|e| format!("serialize: {e}"))?;
    writer
        .write_all(line.as_bytes())
        .map_err(|e| format!("write: {e}"))?;
    writer
        .write_all(b"\n")
        .map_err(|e| format!("newline: {e}"))?;
    writer.flush().map_err(|e| format!("flush: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipc_message_parse() {
        let json = r#"{"type":"tool_call","tool":"list_instances","args":{},"requestId":42}"#;
        let msg: IpcMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.msg_type, "tool_call");
        assert_eq!(msg.tool.as_deref(), Some("list_instances"));
        assert_eq!(msg.request_id, Some(42));
    }

    #[test]
    fn ipc_response_serialize() {
        let resp = IpcResponse {
            request_id: 1,
            result: Some(serde_json::json!({"sent": true})),
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"requestId\":1"));
        assert!(json.contains("\"sent\":true"));
        assert!(!json.contains("error"));
    }
}
