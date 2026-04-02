//! PTY output monitor — detects ready state and trust dialogs.
//!
//! Receives raw PTY bytes from the screen thread via a global crossbeam channel,
//! accumulates them per terminal, and pattern-matches for backend-specific events.

use crossbeam::channel::{self, Receiver, Sender};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;

/// Events sent from the screen thread to the agend monitor.
#[derive(Debug)]
pub enum PtyEvent {
    /// Raw bytes from a terminal pane.
    Bytes(u32, Vec<u8>),
    /// A new terminal pane was created. Register it for monitoring.
    /// (terminal_id, pane_name — matches the instance name from fleet.yaml)
    Register(u32, String),
    /// A terminal pane was closed.
    Closed(u32),
}

/// Actions the monitor wants to perform on a pane.
#[derive(Debug)]
pub enum MonitorAction {
    /// Write bytes to the terminal (e.g., Enter key to dismiss dialog).
    Write(u32, Vec<u8>),
}

/// Global channel for PTY events (screen → agend monitor).
static PTY_CHANNEL: Lazy<(Sender<PtyEvent>, Receiver<PtyEvent>)> =
    Lazy::new(|| channel::bounded(4096));

/// Global channel for monitor actions (agend monitor → pty writer).
static ACTION_CHANNEL: Lazy<(Sender<MonitorAction>, Receiver<MonitorAction>)> =
    Lazy::new(|| channel::bounded(256));

/// Send a PTY event to the monitor. Called from screen thread hook.
pub fn send_pty_event(event: PtyEvent) {
    let _ = PTY_CHANNEL.0.try_send(event);
}

/// Receive pending monitor actions. Called from the agend event loop.
pub fn recv_action() -> Option<MonitorAction> {
    ACTION_CHANNEL.1.try_recv().ok()
}

// ── Backend detection patterns ──────────────────────────────────────────

/// Ready patterns per backend (from Node.js AgEnD).
struct BackendPatterns {
    ready: Regex,
    name: &'static str,
}

fn backend_patterns() -> Vec<BackendPatterns> {
    vec![
        BackendPatterns {
            name: "claude-code",
            ready: Regex::new(r"❯|ok\s*$").unwrap(),
        },
        BackendPatterns {
            name: "gemini-cli",
            ready: Regex::new(r"Type your message|\? for shortcuts|YOLO Ctrl").unwrap(),
        },
        BackendPatterns {
            name: "codex",
            ready: Regex::new(r"% left|OpenAI Codex").unwrap(),
        },
        BackendPatterns {
            name: "opencode",
            ready: Regex::new(r"Ask anything|ctrl\+p commands").unwrap(),
        },
    ]
}

/// Dialog patterns (generic across all backends).
static DIALOG_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[Nn]o, exit|[Nn]o, quit|[Dd]on't trust|[Ii] accept|[Ii] trust|[Yy]es, continue|[Tt]rust folder")
        .unwrap()
});

/// "No" option is currently selected — need to navigate down.
static DIALOG_NO_SELECTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[❯›]\s*\d+\.\s*No").unwrap());

/// Gemini "Don't trust" is selected — need to navigate up.
static DIALOG_DONT_TRUST_SELECTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[❯›]\s*Don't trust").unwrap());

/// Resume session picker.
static RESUME_SESSION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[Rr]esume [Ss]ession").unwrap());

/// Fatal: command not found.
static NOT_FOUND_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"command not found|[Nn]ot found").unwrap());

// ── Per-terminal state ──────────────────────────────────────────────────

/// Rolling buffer of recent terminal output (last N bytes) for pattern matching.
const BUFFER_CAP: usize = 8192;

#[derive(Debug)]
pub struct TerminalState {
    /// Instance name (from fleet config).
    pub instance_name: String,
    /// Backend name for ready pattern selection.
    pub backend: String,
    /// Rolling output buffer.
    buf: Vec<u8>,
    /// Whether ready has been detected.
    pub ready: bool,
    /// Number of dialog dismiss attempts.
    pub dialog_attempts: u32,
}

impl TerminalState {
    pub fn new(instance_name: String, backend: String) -> Self {
        Self {
            instance_name,
            backend,
            buf: Vec::with_capacity(BUFFER_CAP),
            ready: false,
            dialog_attempts: 0,
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() > BUFFER_CAP {
            let drain = self.buf.len() - BUFFER_CAP;
            self.buf.drain(..drain);
        }
    }

    fn text(&self) -> String {
        strip_ansi(&String::from_utf8_lossy(&self.buf))
    }
}

/// Strip ANSI escape sequences from text so pattern matching works on clean text.
/// Raw PTY output contains color codes, cursor movements, etc. that break regex.
fn strip_ansi(s: &str) -> String {
    // Matches: ESC[ ... final_byte, ESC] ... ST, ESC(X, and other common sequences
    static ANSI_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[()][A-B012]|\x1b[>=]|\x1b\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]")
            .unwrap()
    });
    ANSI_RE.replace_all(s, "").into_owned()
}

// ── Monitor thread ──────────────────────────────────────────────────────

/// Registered terminals the monitor cares about.
/// Key = terminal_id (u32), set by FleetManager after spawning.
pub struct Monitor {
    pub(crate) terminals: HashMap<u32, TerminalState>,
    patterns: Vec<BackendPatterns>,
    /// Instance name → backend name mapping (from fleet config).
    instance_backends: HashMap<String, String>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            terminals: HashMap::new(),
            patterns: backend_patterns(),
            instance_backends: HashMap::new(),
        }
    }

    /// Create a monitor with fleet config for auto-registration.
    pub fn with_config(config: &super::config::FleetConfig) -> Self {
        let instance_backends = config
            .instances
            .iter()
            .map(|(name, ic)| {
                (name.clone(), ic.backend_or(&config.defaults).to_owned())
            })
            .collect();
        Self {
            terminals: HashMap::new(),
            patterns: backend_patterns(),
            instance_backends,
        }
    }

    /// Register a terminal for monitoring.
    pub fn register(&mut self, terminal_id: u32, instance_name: String, backend: String) {
        log::info!(
            "agend monitor: registered terminal {} for instance '{}' (backend: {})",
            terminal_id, instance_name, backend
        );
        // NOTE: Do NOT call super::register_terminal() here — it sends PtyEvent::Register
        // back to us, creating an infinite loop. The global registry is updated by the
        // caller (layout_applier or on_new_pane) before we get the event.
        self.terminals.insert(
            terminal_id,
            TerminalState::new(instance_name, backend),
        );
    }

    /// Process a PTY event. Returns any actions to take.
    pub fn process(&mut self, event: PtyEvent) -> Vec<MonitorAction> {
        let mut actions = Vec::new();
        match event {
            PtyEvent::Register(tid, pane_name) => {
                super::debug_log(&format!("monitor: PtyEvent::Register tid={} name={} backends={:?}", tid, pane_name, self.instance_backends.keys().collect::<Vec<_>>()));
                if let Some(backend) = self.instance_backends.get(&pane_name).cloned() {
                    self.register(tid, pane_name, backend);
                } else {
                    super::debug_log(&format!("monitor: pane '{}' NOT in fleet config", pane_name));
                }
                return actions;
            },
            PtyEvent::Bytes(tid, bytes) => {
                if self.terminals.is_empty() {
                    // Log once to show we're getting bytes but no terminals registered
                    static LOGGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
                    if !LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        super::debug_log(&format!("monitor: receiving Bytes(tid={}) but NO terminals registered!", tid));
                    }
                }
                if let Some(state) = self.terminals.get_mut(&tid) {
                    if state.ready {
                        return actions;
                    }
                    state.append(&bytes);
                    let text = state.text();

                    // Debug: log when text has real content (not just empty from ANSI stripping)
                    if !text.is_empty() && text.trim().len() > 5 {
                        static TEXT_LOG_COUNT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                        let n = TEXT_LOG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if n < 10 {
                            let preview = if text.len() > 500 { &text[text.len()-500..] } else { &text };
                            let dialog_match = DIALOG_PATTERN.is_match(&text);
                            let ready_match = self.patterns.iter().any(|bp| bp.name == state.backend && bp.ready.is_match(&text));
                            super::debug_log(&format!(
                                "monitor: tid={} inst={} len={} dialog={} ready={} text: {:?}",
                                tid, state.instance_name, text.len(), dialog_match, ready_match, preview
                            ));
                        }
                    }

                    // 1. Check for dialog BEFORE ready (dialog can look like ready)
                    if DIALOG_PATTERN.is_match(&text) && state.dialog_attempts < 5 {
                        state.dialog_attempts += 1;
                        super::debug_log(&format!("DIALOG DETECTED for '{}' (attempt {}), text: {}",
                            state.instance_name, state.dialog_attempts, &text[..text.len().min(200)]));

                        if DIALOG_NO_SELECTED.is_match(&text) {
                            // Navigate down to "Yes" option, then Enter
                            actions.push(MonitorAction::Write(tid, b"\x1b[B".to_vec())); // Down arrow
                            actions.push(MonitorAction::Write(tid, b"\r".to_vec())); // Enter
                        } else if DIALOG_DONT_TRUST_SELECTED.is_match(&text) {
                            // Navigate up twice to "Trust folder", then Enter
                            actions.push(MonitorAction::Write(tid, b"\x1b[A".to_vec())); // Up
                            actions.push(MonitorAction::Write(tid, b"\x1b[A".to_vec())); // Up
                            actions.push(MonitorAction::Write(tid, b"\r".to_vec())); // Enter
                        } else {
                            // Just press Enter (assuming accept option is focused)
                            actions.push(MonitorAction::Write(tid, b"\r".to_vec()));
                        }
                        // Clear buffer to re-evaluate after dialog is dismissed
                        state.buf.clear();
                        return actions;
                    }

                    // 2. Check resume session picker
                    if RESUME_SESSION_PATTERN.is_match(&text) {
                        log::info!(
                            "agend monitor: resume session picker for '{}', pressing Escape",
                            state.instance_name
                        );
                        actions.push(MonitorAction::Write(tid, b"\x1b".to_vec())); // Escape
                        state.buf.clear();
                        return actions;
                    }

                    // 3. Check for fatal errors
                    if NOT_FOUND_PATTERN.is_match(&text) {
                        log::error!(
                            "agend monitor: command not found for '{}'",
                            state.instance_name
                        );
                        // TODO: trigger restart logic
                        return actions;
                    }

                    // 4. Check backend-specific ready pattern
                    let backend = &state.backend;
                    for bp in &self.patterns {
                        if bp.name == backend && bp.ready.is_match(&text) {
                            log::info!(
                                "agend monitor: instance '{}' is READY (backend: {})",
                                state.instance_name, backend
                            );
                            state.ready = true;
                            return actions;
                        }
                    }
                }
            },
            PtyEvent::Closed(tid) => {
                if let Some(state) = self.terminals.remove(&tid) {
                    log::warn!(
                        "agend monitor: terminal {} closed for instance '{}'",
                        tid, state.instance_name
                    );
                    // TODO: trigger restart logic
                }
            },
        }
        actions
    }

    /// Run the monitor loop. Blocks the calling thread.
    pub fn run(mut self) {
        let rx = &PTY_CHANNEL.1;
        super::debug_log("monitor: run() started, waiting for events");
        let mut event_count: u64 = 0;
        loop {
            match rx.recv() {
                Ok(event) => {
                    event_count += 1;
                    if event_count <= 3 || (event_count <= 50 && event_count % 10 == 0) {
                        let desc = match &event {
                            PtyEvent::Bytes(tid, b) => format!("Bytes(tid={}, len={})", tid, b.len()),
                            PtyEvent::Register(tid, name) => format!("Register(tid={}, name={})", tid, name),
                            PtyEvent::Closed(tid) => format!("Closed(tid={})", tid),
                        };
                        super::debug_log(&format!("monitor: event #{}: {}", event_count, desc));
                    }
                    let actions = self.process(event);
                    if !actions.is_empty() {
                        super::debug_log(&format!("monitor: produced {} actions", actions.len()));
                    }
                    for action in actions {
                        let _ = ACTION_CHANNEL.0.try_send(action);
                    }
                },
                Err(_) => {
                    log::info!("agend monitor: channel closed, exiting");
                    break;
                },
            }
        }
    }
}
