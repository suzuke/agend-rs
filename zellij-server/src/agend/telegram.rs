//! Telegram adapter — bot polling, topic routing, bidirectional messaging.
//!
//! Uses raw HTTP calls to Telegram Bot API via `isahc` (already in Zellij deps).
//! No tokio runtime needed — runs in a plain thread with blocking HTTP.

use crossbeam::channel::{self, Receiver, Sender};
use isahc::prelude::*;
use isahc::ReadResponseExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use super::config::FleetConfig;
use super::routing::RoutingEngine;

const MAX_MESSAGE_LENGTH: usize = 4096;
const POLL_TIMEOUT_SECS: u64 = 30;

// ── Message types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct InboundMessage {
    pub chat_id: String,
    pub thread_id: Option<String>,
    pub message_id: String,
    pub user_id: String,
    pub username: String,
    pub text: String,
    pub timestamp: i64,
    pub reply_to_text: Option<String>,
    pub target_instance: Option<String>,
    /// Telegram file_id if message has an attachment (photo, document, etc.)
    pub attachment_file_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action")]
pub enum OutboundAction {
    #[serde(rename = "send_text")]
    SendText {
        chat_id: String,
        text: String,
        thread_id: Option<String>,
        reply_to: Option<String>,
        format: Option<String>,
    },
    #[serde(rename = "send_file")]
    SendFile {
        chat_id: String,
        file_path: String,
        thread_id: Option<String>,
    },
    #[serde(rename = "edit_message")]
    EditMessage {
        chat_id: String,
        message_id: String,
        text: String,
    },
    #[serde(rename = "react")]
    React {
        chat_id: String,
        message_id: String,
        emoji: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct OutboundResult {
    pub message_id: Option<String>,
    pub error: Option<String>,
}

// ── Channel pair ────────────────────────────────────────────────────────

pub fn create_channels() -> (TelegramSender, TelegramReceiver) {
    let (inbound_tx, inbound_rx) = channel::bounded(256);
    let (outbound_tx, outbound_rx) = channel::bounded(256);
    (
        TelegramSender { inbound_rx, outbound_tx },
        TelegramReceiver { inbound_tx, outbound_rx },
    )
}

pub struct TelegramSender {
    pub inbound_rx: Receiver<InboundMessage>,
    pub outbound_tx: Sender<OutboundAction>,
}

pub struct TelegramReceiver {
    pub inbound_tx: Sender<InboundMessage>,
    pub outbound_rx: Receiver<OutboundAction>,
}

// ── Telegram Bot API client ─────────────────────────────────────────────

struct BotApi {
    #[allow(dead_code)]
    token: String,
    base_url: String,
}

impl BotApi {
    fn new(token: &str) -> Self {
        Self {
            token: token.to_owned(),
            base_url: format!("https://api.telegram.org/bot{}", token),
        }
    }

    fn call(&self, method: &str, body: &Value) -> Result<Value, String> {
        let url = format!("{}/{}", self.base_url, method);
        let body_str = serde_json::to_string(body).map_err(|e| format!("json: {e}"))?;

        let timeout = if method == "getUpdates" {
            Duration::from_secs(POLL_TIMEOUT_SECS + 10)
        } else {
            Duration::from_secs(30)
        };

        let response = isahc::Request::post(&url)
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .body(body_str)
            .map_err(|e| format!("request build: {e}"))?
            .send()
            .map_err(|e| format!("send: {e}"))?;

        let mut response = response;
        let text = response.text().map_err(|e| format!("read: {e}"))?;
        let parsed: Value = serde_json::from_str(&text).map_err(|e| format!("parse: {e}"))?;

        if parsed["ok"].as_bool() == Some(true) {
            Ok(parsed["result"].clone())
        } else {
            Err(format!("Telegram API error: {}", parsed["description"].as_str().unwrap_or("unknown")))
        }
    }

    fn send_message(&self, chat_id: &str, text: &str, thread_id: Option<&str>, reply_to: Option<&str>, parse_mode: Option<&str>) -> Result<Value, String> {
        let mut body = json!({
            "chat_id": chat_id,
            "text": text,
        });
        if let Some(tid) = thread_id {
            if let Ok(t) = tid.parse::<i64>() {
                body["message_thread_id"] = json!(t);
            }
        }
        if let Some(rid) = reply_to {
            if let Ok(r) = rid.parse::<i64>() {
                body["reply_parameters"] = json!({"message_id": r});
            }
        }
        if let Some(pm) = parse_mode {
            body["parse_mode"] = json!(pm);
        }
        self.call("sendMessage", &body)
    }

    fn edit_message_text(&self, chat_id: &str, message_id: &str, text: &str) -> Result<Value, String> {
        self.call("editMessageText", &json!({
            "chat_id": chat_id,
            "message_id": message_id.parse::<i64>().unwrap_or(0),
            "text": text,
        }))
    }

    fn set_message_reaction(&self, chat_id: &str, message_id: &str, emoji: &str) -> Result<Value, String> {
        self.call("setMessageReaction", &json!({
            "chat_id": chat_id,
            "message_id": message_id.parse::<i64>().unwrap_or(0),
            "reaction": [{"type": "emoji", "emoji": emoji}],
        }))
    }

    #[allow(dead_code)]
    fn get_file(&self, file_id: &str) -> Result<String, String> {
        let result = self.call("getFile", &json!({"file_id": file_id}))?;
        result["file_path"]
            .as_str()
            .map(|s| s.to_owned())
            .ok_or_else(|| "no file_path in response".into())
    }

    #[allow(dead_code)]
    fn download_file(&self, file_path: &str, local_path: &std::path::Path) -> Result<(), String> {
        let url = format!("https://api.telegram.org/file/bot{}/{}", self.token, file_path);
        let mut response = isahc::get(&url).map_err(|e| format!("download: {e}"))?;
        let bytes = response.bytes().map_err(|e| format!("read: {e}"))?;
        std::fs::write(local_path, &bytes).map_err(|e| format!("write: {e}"))?;
        Ok(())
    }

    /// Download a Telegram file by file_id to local inbox directory.
    #[allow(dead_code)]
    fn download_attachment(&self, file_id: &str) -> Result<String, String> {
        let tg_path = self.get_file(file_id)?;
        let filename = tg_path.rsplit('/').next().unwrap_or("attachment");
        let inbox = super::paths::agend_home().join("inbox");
        std::fs::create_dir_all(&inbox).map_err(|e| format!("mkdir: {e}"))?;
        let local_path = inbox.join(format!("{}_{}", chrono::Utc::now().timestamp(), filename));
        self.download_file(&tg_path, &local_path)?;
        Ok(local_path.to_string_lossy().into_owned())
    }

    /// Create a forum topic in the group. Returns the topic thread_id.
    pub fn create_forum_topic(&self, chat_id: i64, name: &str) -> Result<i64, String> {
        let result = self.call("createForumTopic", &json!({
            "chat_id": chat_id,
            "name": name,
        }))?;
        result["message_thread_id"]
            .as_i64()
            .ok_or_else(|| "no message_thread_id in response".into())
    }

    fn get_updates(&self, offset: i64) -> Result<Vec<Value>, String> {
        let result = self.call("getUpdates", &json!({
            "offset": offset,
            "timeout": POLL_TIMEOUT_SECS,
            "allowed_updates": ["message"],
        }))?;
        Ok(result.as_array().cloned().unwrap_or_default())
    }
}

// ── Standalone topic creation ────────────────────────────────────────────

/// Create a Telegram forum topic without needing a full adapter.
/// Uses the bot token from the fleet config.
pub fn create_topic(config: &FleetConfig, name: &str) -> Result<i64, String> {
    let channel = config.channel.as_ref().ok_or("no channel configured")?;
    let bot_token_env = channel.bot_token_env.as_deref().unwrap_or("AGEND_BOT_TOKEN");
    let bot_token = std::env::var(bot_token_env)
        .map_err(|_| format!("env var {bot_token_env} not set"))?;
    let group_id = channel.group_id.ok_or("no group_id configured")?;
    let bot = BotApi::new(&bot_token);
    bot.create_forum_topic(group_id, name)
}

// ── Adapter ─────────────────────────────────────────────────────────────

pub struct TelegramAdapter {
    bot_token: String,
    group_id: i64,
    allowed_users: HashSet<i64>,
    routing: Arc<std::sync::RwLock<RoutingEngine>>,
}

impl TelegramAdapter {
    pub fn from_config(
        config: &FleetConfig,
        routing: Arc<std::sync::RwLock<RoutingEngine>>,
    ) -> Option<Self> {
        let channel = config.channel.as_ref()?;
        if channel.channel_type != "telegram" {
            return None;
        }

        let bot_token_env = channel.bot_token_env.as_deref().unwrap_or("AGEND_BOT_TOKEN");
        let bot_token = std::env::var(bot_token_env).ok()?;
        let group_id = channel.group_id?;

        let allowed_users: HashSet<i64> = channel
            .access
            .as_ref()
            .map(|a| a.allowed_users.iter().copied().collect())
            .unwrap_or_default();

        Some(Self {
            bot_token,
            group_id,
            allowed_users,
            routing,
        })
    }

    pub fn run(self) -> TelegramSender {
        let (sender, receiver) = create_channels();

        // Polling thread — plain blocking HTTP, no tokio needed
        let bot = BotApi::new(&self.bot_token);
        let group_id = self.group_id;
        let allowed_users = self.allowed_users;
        let routing = self.routing;
        let inbound_tx = receiver.inbound_tx;

        std::thread::Builder::new()
            .name("agend_tg_poll".into())
            .spawn(move || {
                log::info!("agend telegram: polling thread started");

                // Cancel any stale long-poll connection from a previous process
                // by doing a short getUpdates with timeout=0
                log::info!("agend telegram: cancelling stale connections...");
                let _ = bot.call("getUpdates", &json!({"timeout": 0, "limit": 1}));

                let mut offset: i64 = 0;

                loop {
                    match bot.get_updates(offset) {
                        Ok(updates) => {
                            for update in updates {
                                if let Some(uid) = update["update_id"].as_i64() {
                                    offset = uid + 1;
                                }
                                if let Some(msg) = update.get("message") {
                                    process_message(msg, group_id, &allowed_users, &routing, &inbound_tx);
                                }
                            }
                        },
                        Err(e) => {
                            if e.contains("Conflict") || e.contains("terminated by other") {
                                log::warn!("agend telegram: polling conflict (another process?), retrying in 10s...");
                                std::thread::sleep(Duration::from_secs(10));
                            } else {
                                log::warn!("agend telegram: getUpdates error: {e}");
                                std::thread::sleep(Duration::from_secs(5));
                            }
                        },
                    }
                }
            })
            .expect("failed to spawn telegram polling thread");

        // Outbound handler thread — sends messages via HTTP
        let bot2 = BotApi::new(&self.bot_token);
        let outbound_rx = receiver.outbound_rx;

        std::thread::Builder::new()
            .name("agend_tg_send".into())
            .spawn(move || {
                loop {
                    let action = match outbound_rx.recv() {
                        Ok(a) => a,
                        Err(_) => break,
                    };
                    handle_outbound(&bot2, action);
                }
            })
            .expect("failed to spawn telegram outbound thread");

        log::info!("agend telegram: adapter started (HTTP polling)");
        sender
    }
}

// ── Message processing ──────────────────────────────────────────────────

fn process_message(
    msg: &Value,
    group_id: i64,
    allowed_users: &HashSet<i64>,
    routing: &Arc<std::sync::RwLock<RoutingEngine>>,
    inbound_tx: &Sender<InboundMessage>,
) {
    let chat_id = msg["chat"]["id"].as_i64().unwrap_or(0);
    if chat_id != group_id {
        return;
    }

    let user_id = msg["from"]["id"].as_i64().unwrap_or(0);
    if !allowed_users.is_empty() && !allowed_users.contains(&user_id) {
        return;
    }

    // Extract text and/or attachment
    let text = msg["text"].as_str().or(msg["caption"].as_str()).unwrap_or("").to_owned();
    let attachment_file_id = extract_file_id(msg);
    if text.is_empty() && attachment_file_id.is_none() {
        return;
    }

    let thread_id = msg["message_thread_id"].as_i64().map(|t| t.to_string());
    let username = msg["from"]["username"]
        .as_str()
        .unwrap_or("")
        .to_owned();

    // Route: try exact thread_id first, then fall back to General topic ("1")
    // Telegram's General topic sometimes has no message_thread_id field
    let target = thread_id
        .as_deref()
        .and_then(|tid| {
            routing.read().ok().and_then(|r| r.instance_for_thread(tid).map(|s| s.to_owned()))
        })
        .or_else(|| {
            // No thread_id or no match — try General topic and "1" as fallback
            routing.read().ok().and_then(|r| {
                r.instance_for_thread("1")
                    .or_else(|| r.general_instance())
                    .map(|s| s.to_owned())
            })
        });

    let reply_to_text = msg["reply_to_message"]["text"]
        .as_str()
        .map(|t| t.to_owned());

    let inbound = InboundMessage {
        chat_id: chat_id.to_string(),
        thread_id,
        message_id: msg["message_id"].as_i64().unwrap_or(0).to_string(),
        user_id: user_id.to_string(),
        username: if username.is_empty() { user_id.to_string() } else { username },
        text,
        timestamp: msg["date"].as_i64().unwrap_or(0),
        reply_to_text,
        target_instance: target,
        attachment_file_id,
    };

    let _ = inbound_tx.try_send(inbound);
}

/// Extract the best file_id from a Telegram message (photo, document, audio, etc.)
fn extract_file_id(msg: &Value) -> Option<String> {
    // Photo: array of sizes, take the largest (last)
    if let Some(photos) = msg["photo"].as_array() {
        if let Some(last) = photos.last() {
            return last["file_id"].as_str().map(|s| s.to_owned());
        }
    }
    // Document
    if let Some(fid) = msg["document"]["file_id"].as_str() {
        return Some(fid.to_owned());
    }
    // Audio / Voice / Video
    for key in &["audio", "voice", "video", "video_note", "sticker"] {
        if let Some(fid) = msg[key]["file_id"].as_str() {
            return Some(fid.to_owned());
        }
    }
    None
}

fn handle_outbound(bot: &BotApi, action: OutboundAction) {
    match action {
        OutboundAction::SendText { chat_id, text, thread_id, reply_to, format } => {
            let parse_mode = match format.as_deref() {
                Some("markdown") => Some("MarkdownV2"),
                Some("html") => Some("HTML"),
                _ => None,
            };
            for chunk in chunk_text(&text, MAX_MESSAGE_LENGTH) {
                if let Err(e) = bot.send_message(&chat_id, chunk, thread_id.as_deref(), reply_to.as_deref(), parse_mode) {
                    log::error!("agend telegram: send_message: {e}");
                }
            }
        },
        OutboundAction::SendFile { chat_id, file_path, thread_id: _ } => {
            // TODO: implement file sending via multipart upload
            log::warn!("agend telegram: send_file not yet implemented for {file_path} to {chat_id}");
        },
        OutboundAction::EditMessage { chat_id, message_id, text } => {
            if let Err(e) = bot.edit_message_text(&chat_id, &message_id, &text) {
                log::error!("agend telegram: edit_message: {e}");
            }
        },
        OutboundAction::React { chat_id, message_id, emoji } => {
            if let Err(e) = bot.set_message_reaction(&chat_id, &message_id, &emoji) {
                log::error!("agend telegram: react: {e}");
            }
        },
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn chunk_text(text: &str, max_len: usize) -> Vec<&str> {
    if text.len() <= max_len {
        return vec![text];
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let end = (start + max_len).min(text.len());
        let actual_end = if end < text.len() {
            text[start..end]
                .rfind('\n')
                .filter(|&pos| pos > end - start - 200)
                .map(|pos| start + pos + 1)
                .unwrap_or(end)
        } else {
            end
        };
        chunks.push(&text[start..actual_end]);
        start = actual_end;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_short_text() {
        let chunks = chunk_text("hello", 4096);
        assert_eq!(chunks, vec!["hello"]);
    }

    #[test]
    fn chunk_long_text() {
        let text = "a".repeat(5000);
        let chunks = chunk_text(&text, 4096);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].len() <= 4096);
    }

    #[test]
    fn chunk_at_newline() {
        let text = format!("{}\n{}", "a".repeat(4000), "b".repeat(200));
        let chunks = chunk_text(&text, 4096);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 4001);
    }
}
