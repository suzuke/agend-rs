//! Telegram adapter — bot polling, topic routing, bidirectional messaging.
//!
//! Uses `teloxide` for Telegram Bot API interaction. Runs its own tokio
//! runtime and communicates with the rest of agend via crossbeam channels.

use crossbeam::channel::{self, Receiver, Sender};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

use teloxide::prelude::*;
use teloxide::types::{ChatId, InputFile, MessageId, ParseMode, ReactionType, ThreadId};

use super::config::{ChannelConfig, FleetConfig};
use super::routing::RoutingEngine;

/// Maximum message length for Telegram.
const MAX_MESSAGE_LENGTH: usize = 4096;

// ── Message types ───────────────────────────────────────────────────────

/// Inbound message from Telegram.
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
    /// Target instance name (resolved by routing engine).
    pub target_instance: Option<String>,
}

/// Outbound action to Telegram.
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

/// Result of an outbound action.
#[derive(Debug, Clone, Serialize)]
pub struct OutboundResult {
    pub message_id: Option<String>,
    pub error: Option<String>,
}

// ── Channel pair ────────────────────────────────────────────────────────

/// Create a pair of channels for bidirectional communication.
pub fn create_channels() -> (TelegramSender, TelegramReceiver) {
    let (inbound_tx, inbound_rx) = channel::bounded(256);
    let (outbound_tx, outbound_rx) = channel::bounded(256);
    (
        TelegramSender {
            inbound_rx,
            outbound_tx,
        },
        TelegramReceiver {
            inbound_tx,
            outbound_rx,
        },
    )
}

/// The fleet manager side — receives inbound messages, sends outbound actions.
pub struct TelegramSender {
    pub inbound_rx: Receiver<InboundMessage>,
    pub outbound_tx: Sender<OutboundAction>,
}

/// The Telegram adapter side — sends inbound messages, receives outbound actions.
pub struct TelegramReceiver {
    pub inbound_tx: Sender<InboundMessage>,
    pub outbound_rx: Receiver<OutboundAction>,
}

// ── Adapter ─────────────────────────────────────────────────────────────

/// The Telegram adapter. Call `run()` to start polling in a background thread.
pub struct TelegramAdapter {
    bot_token: String,
    group_id: i64,
    allowed_users: HashSet<i64>,
    routing: Arc<std::sync::RwLock<RoutingEngine>>,
}

impl TelegramAdapter {
    pub fn from_config(config: &FleetConfig) -> Option<Self> {
        let channel = config.channel.as_ref()?;
        if channel.channel_type != "telegram" {
            return None;
        }

        let bot_token_env = channel
            .bot_token_env
            .as_deref()
            .unwrap_or("AGEND_BOT_TOKEN");
        let bot_token = std::env::var(bot_token_env).ok()?;
        let group_id = channel.group_id?;

        let allowed_users: HashSet<i64> = channel
            .access
            .as_ref()
            .map(|a| a.allowed_users.iter().copied().collect())
            .unwrap_or_default();

        let mut routing = RoutingEngine::new();
        routing.rebuild(config);

        Some(Self {
            bot_token,
            group_id,
            allowed_users,
            routing: Arc::new(std::sync::RwLock::new(routing)),
        })
    }

    /// Start the Telegram bot in a background thread.
    /// Returns the channel pair for communication.
    pub fn run(self) -> TelegramSender {
        let (sender, receiver) = create_channels();

        std::thread::Builder::new()
            .name("agend_telegram".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Only enable io+time, NOT signals — tokio signal handlers
                    // conflict with Zellij's daemonized server signal handling
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_io()
                        .enable_time()
                        .build()
                        .expect("failed to build tokio runtime for telegram");
                    rt.block_on(async {
                        run_bot(self, receiver).await;
                    });
                }));
                if let Err(e) = result {
                    log::error!("agend telegram: thread panicked: {:?}", e);
                }
            })
            .expect("failed to spawn telegram thread");

        log::info!("agend telegram: adapter started");
        sender
    }
}

// ── Bot event loop ──────────────────────────────────────────────────────

async fn run_bot(adapter: TelegramAdapter, channels: TelegramReceiver) {
    let bot = Bot::new(&adapter.bot_token);

    // Spawn outbound handler
    let bot_clone = bot.clone();
    let outbound_rx = channels.outbound_rx;
    tokio::spawn(async move {
        handle_outbound(bot_clone, outbound_rx).await;
    });

    // Set up inbound message handler
    let handler = Update::filter_message().endpoint({
        let inbound_tx = channels.inbound_tx;
        let group_id = adapter.group_id;
        let allowed_users = adapter.allowed_users.clone();
        let routing = adapter.routing.clone();

        move |msg: Message, _bot: Bot| {
            let inbound_tx = inbound_tx.clone();
            let allowed_users = allowed_users.clone();
            let routing = routing.clone();
            async move {
                // Only process messages from the configured group
                if msg.chat.id != ChatId(group_id) {
                    return Ok::<(), teloxide::RequestError>(());
                }

                // Access control
                let user_id = msg
                    .from
                    .as_ref()
                    .map(|u| u.id.0 as i64)
                    .unwrap_or(0);

                if !allowed_users.is_empty() && !allowed_users.contains(&user_id) {
                    return Ok::<(), teloxide::RequestError>(());
                }

                let text = msg.text().unwrap_or("").to_owned();
                if text.is_empty() {
                    return Ok::<(), teloxide::RequestError>(());
                }

                let thread_id = msg.thread_id.map(|t| t.0.to_string());
                let username = msg
                    .from
                    .as_ref()
                    .and_then(|u| u.username.clone())
                    .unwrap_or_else(|| user_id.to_string());

                // Resolve target instance via routing
                let target = thread_id.as_deref().and_then(|tid| {
                    routing
                        .read()
                        .ok()
                        .and_then(|r| r.instance_for_thread(tid).map(|s| s.to_owned()))
                });

                // Get reply-to text if replying to a message
                let reply_to_text = msg
                    .reply_to_message()
                    .and_then(|r| r.text())
                    .map(|t| t.to_owned());

                let inbound = InboundMessage {
                    chat_id: msg.chat.id.0.to_string(),
                    thread_id,
                    message_id: msg.id.0.to_string(),
                    user_id: user_id.to_string(),
                    username,
                    text,
                    timestamp: msg.date.timestamp(),
                    reply_to_text,
                    target_instance: target,
                };

                let _ = inbound_tx.try_send(inbound);
                Ok::<(), teloxide::RequestError>(())
            }
        }
    });

    let mut dispatcher = Dispatcher::builder(bot, handler).build();

    log::info!("agend telegram: starting long polling");
    dispatcher.dispatch().await;
}

// ── Outbound handler ────────────────────────────────────────────────────

async fn handle_outbound(bot: Bot, outbound_rx: Receiver<OutboundAction>) {
    loop {
        let action = match outbound_rx.recv() {
            Ok(a) => a,
            Err(_) => break,
        };

        match action {
            OutboundAction::SendText {
                chat_id,
                text,
                thread_id,
                reply_to,
                format,
            } => {
                let chat = ChatId(chat_id.parse().unwrap_or(0));
                let parse_mode = match format.as_deref() {
                    Some("markdown") => Some(ParseMode::MarkdownV2),
                    Some("html") => Some(ParseMode::Html),
                    _ => None,
                };

                // Chunk long messages
                for chunk in chunk_text(&text, MAX_MESSAGE_LENGTH) {
                    let mut req = bot.send_message(chat, chunk);
                    if let Some(tid) = thread_id.as_deref() {
                        if let Ok(t) = tid.parse::<i32>() {
                            req = req.message_thread_id(ThreadId(MessageId(t)));
                        }
                    }
                    if let Some(ref rid) = reply_to {
                        if let Ok(r) = rid.parse::<i32>() {
                            let params = teloxide::types::ReplyParameters::new(MessageId(r));
                            req = req.reply_parameters(params);
                        }
                    }
                    if let Some(pm) = parse_mode {
                        req = req.parse_mode(pm);
                    }
                    if let Err(e) = req.await {
                        log::error!("agend telegram: send_message failed: {e}");
                    }
                }
            },

            OutboundAction::SendFile {
                chat_id,
                file_path,
                thread_id,
            } => {
                let chat = ChatId(chat_id.parse().unwrap_or(0));
                let file = InputFile::file(&file_path);
                let is_image = file_path.ends_with(".png")
                    || file_path.ends_with(".jpg")
                    || file_path.ends_with(".jpeg")
                    || file_path.ends_with(".gif")
                    || file_path.ends_with(".webp");

                let thread = thread_id
                    .as_deref()
                    .and_then(|t| t.parse::<i32>().ok())
                    .map(|t| ThreadId(MessageId(t)));

                let result = if is_image {
                    let mut req = bot.send_photo(chat, file);
                    if let Some(t) = thread {
                        req = req.message_thread_id(t);
                    }
                    req.await.map(|_| ())
                } else {
                    let mut req = bot.send_document(chat, file);
                    if let Some(t) = thread {
                        req = req.message_thread_id(t);
                    }
                    req.await.map(|_| ())
                };

                if let Err(e) = result {
                    log::error!("agend telegram: send_file failed: {e}");
                }
            },

            OutboundAction::EditMessage {
                chat_id,
                message_id,
                text,
            } => {
                let chat = ChatId(chat_id.parse().unwrap_or(0));
                let mid = MessageId(message_id.parse().unwrap_or(0));
                if let Err(e) = bot.edit_message_text(chat, mid, &text).await {
                    log::error!("agend telegram: edit_message failed: {e}");
                }
            },

            OutboundAction::React {
                chat_id,
                message_id,
                emoji,
            } => {
                let chat = ChatId(chat_id.parse().unwrap_or(0));
                let mid = MessageId(message_id.parse().unwrap_or(0));
                let reaction = ReactionType::Emoji { emoji };
                if let Err(e) = bot.set_message_reaction(chat, mid).reaction(vec![reaction]).await {
                    log::error!("agend telegram: react failed: {e}");
                }
            },
        }
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
        // Try to break at a newline within the last 200 chars
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
        assert_eq!(chunks[0].len(), 4001); // 4000 + newline
    }
}
