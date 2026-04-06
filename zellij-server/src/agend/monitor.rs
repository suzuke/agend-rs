//! PTY output monitor — detects ready state and trust dialogs.
//!
//! Receives raw PTY bytes from the screen thread via a global crossbeam channel,
//! accumulates them per terminal, and pattern-matches for backend-specific events.

use crossbeam::channel::{self, Receiver, Sender};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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

/// Error severity detected from PTY output.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorKind {
    RateLimit,
    AuthError,
    NetworkError,
    Overloaded,
    Crash,
}

/// What the daemon should do when an error is detected.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorAction {
    Notify,
    Restart,
    Pause,
}

/// Actions the monitor wants to perform on a pane.
#[derive(Debug)]
pub enum MonitorAction {
    /// Write bytes to the terminal (e.g., Enter key to dismiss dialog).
    Write(u32, Vec<u8>),
    /// Error detected in PTY output. Daemon decides how to handle.
    Error(String, ErrorKind, ErrorAction),
    /// Instance process terminated unexpectedly.
    Restart(String),
}

/// Global channel for PTY events (screen → agend monitor).
static PTY_CHANNEL: Lazy<(Sender<PtyEvent>, Receiver<PtyEvent>)> =
    Lazy::new(|| channel::bounded(4096));

/// Global channel for monitor actions (agend monitor → pty writer).
static ACTION_CHANNEL: Lazy<(Sender<MonitorAction>, Receiver<MonitorAction>)> =
    Lazy::new(|| channel::bounded(256));

/// Per-instance last PTY activity timestamp (unix seconds).
/// Written by monitor thread, read by health checker thread.
static ACTIVITY_MAP: Lazy<RwLock<HashMap<String, Arc<AtomicU64>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get the last activity timestamp for an instance (unix seconds). Returns 0 if unknown.
pub fn last_activity_secs(instance_name: &str) -> u64 {
    ACTIVITY_MAP
        .read()
        .ok()
        .and_then(|m| m.get(instance_name).map(|a| a.load(Ordering::Relaxed)))
        .unwrap_or(0)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Send a PTY event to the monitor. Called from screen thread hook.
pub fn send_pty_event(event: PtyEvent) {
    let _ = PTY_CHANNEL.0.try_send(event);
}

/// Receive pending monitor actions. Called from the agend event loop.
pub fn recv_action() -> Option<MonitorAction> {
    ACTION_CHANNEL.1.try_recv().ok()
}

// ── Backend detection patterns ──────────────────────────────────────────

/// An error pattern to match against PTY output.
struct ErrorPattern {
    pattern: Regex,
    kind: ErrorKind,
    action: ErrorAction,
}

/// Ready patterns per backend (from Node.js AgEnD).
struct BackendPatterns {
    ready: Regex,
    name: &'static str,
}

/// Error patterns shared across all backends.
fn error_patterns() -> Vec<ErrorPattern> {
    vec![
        ErrorPattern {
            pattern: Regex::new(r"[Rr]ate.?[Ll]imit|[Tt]oo [Mm]any [Rr]equests|429|[Qq]uota [Ee]xceeded").unwrap(),
            kind: ErrorKind::RateLimit,
            action: ErrorAction::Notify,
        },
        ErrorPattern {
            pattern: Regex::new(r"[Aa]uth.*(?:[Ee]rror|[Ff]ail)|[Uu]nauthorized|401|403|[Ii]nvalid.*[Tt]oken").unwrap(),
            kind: ErrorKind::AuthError,
            action: ErrorAction::Pause,
        },
        ErrorPattern {
            pattern: Regex::new(r"[Nn]etwork.*[Ee]rror|[Cc]onnection.*(?:[Rr]efused|[Rr]eset|[Tt]imeout)|ECONNREFUSED|ETIMEDOUT").unwrap(),
            kind: ErrorKind::NetworkError,
            action: ErrorAction::Notify,
        },
        ErrorPattern {
            pattern: Regex::new(r"[Oo]verloaded|503|[Ss]ervice [Uu]navailable").unwrap(),
            kind: ErrorKind::Overloaded,
            action: ErrorAction::Notify,
        },
        ErrorPattern {
            pattern: Regex::new(r"[Ss]egmentation [Ff]ault|SIGSEGV|panic|[Ff]atal [Ee]rror|Aborted").unwrap(),
            kind: ErrorKind::Crash,
            action: ErrorAction::Restart,
        },
    ]
}

fn backend_patterns() -> Vec<BackendPatterns> {
    vec![
        BackendPatterns {
            name: "claude-code",
            ready: Regex::new(r"❯|ok\s*$").unwrap(),
        },
        BackendPatterns {
            name: "gemini-cli",
            ready: Regex::new(r"Type\s*your\s*message|\?\s*for\s*shortcuts|YOLO\s*Ctrl").unwrap(),
        },
        BackendPatterns {
            name: "codex",
            ready: Regex::new(r"%\s*left|OpenAI\s*Codex").unwrap(),
        },
        BackendPatterns {
            name: "opencode",
            // Shell prompt (opencode runs in a shell pane, not TUI mode)
            ready: Regex::new(r"Ask\s*anything|ctrl\+p\s*commands|[%$#>]\s*$").unwrap(),
        },
    ]
}

/// Dialog patterns (generic across all backends).
// NOTE: ANSI stripping removes cursor positioning, so TUI text loses all spaces.
// "I trust this folder" becomes "Itrustthisfolder". Patterns use \s* for optional spaces.
static DIALOG_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[Nn]o,\s*exit|[Nn]o,\s*quit|[Dd]on't\s*trust|[Ii]\s*accept|[Ii]\s*trust|[Yy]es,\s*continue|[Tt]rust\s*folder")
        .unwrap()
});

/// "No" option is currently selected — need to navigate down.
static DIALOG_NO_SELECTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[❯›]\s*\d+\.\s*No").unwrap());

/// Gemini "Don't trust" is selected — need to navigate up.
static DIALOG_DONT_TRUST_SELECTED: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[❯›]\s*[Dd]on't\s*trust").unwrap());

/// Resume session picker.
static RESUME_SESSION_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[Rr]esume\s*[Ss]ession").unwrap());

/// Fatal: shell reports command not found.
/// Matches "command not found" and "zsh: command not found: foo"
/// but NOT application-level messages like "Config not found" or "rg not found in $PATH".
static NOT_FOUND_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"command\s+not\s+found").unwrap()
});

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
    /// Last PTY activity timestamp (shared with health checker).
    pub last_activity: Arc<AtomicU64>,
}

impl TerminalState {
    pub fn new(instance_name: String, backend: String) -> Self {
        let last_activity = Arc::new(AtomicU64::new(now_secs()));
        // Register in global activity map for health checker access
        if let Ok(mut map) = ACTIVITY_MAP.write() {
            map.insert(instance_name.clone(), Arc::clone(&last_activity));
        }
        Self {
            instance_name,
            backend,
            buf: Vec::with_capacity(BUFFER_CAP),
            ready: false,
            dialog_attempts: 0,
            last_activity,
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
    error_patterns: Vec<ErrorPattern>,
    /// Instance name → backend name mapping (from fleet config).
    instance_backends: HashMap<String, String>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            terminals: HashMap::new(),
            patterns: backend_patterns(),
            error_patterns: error_patterns(),
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
            error_patterns: error_patterns(),
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
                if let Some(backend) = self.instance_backends.get(&pane_name).cloned() {
                    self.register(tid, pane_name, backend);
                }
                return actions;
            },
            PtyEvent::Bytes(tid, bytes) => {
                if let Some(state) = self.terminals.get_mut(&tid) {
                    state.last_activity.store(now_secs(), Ordering::Relaxed);
                    state.append(&bytes);
                    let text = state.text();

                    // ── Pre-ready: dialog dismissal + ready detection ──
                    if !state.ready {
                        // 1. Check for dialog BEFORE ready (dialog can look like ready)
                        if DIALOG_PATTERN.is_match(&text) && state.dialog_attempts < 5 {
                            state.dialog_attempts += 1;
                            log::info!(
                                "agend monitor: dialog detected for '{}' (attempt {})",
                                state.instance_name, state.dialog_attempts
                            );

                            if DIALOG_NO_SELECTED.is_match(&text) {
                                actions.push(MonitorAction::Write(tid, b"\x1b[B".to_vec()));
                                actions.push(MonitorAction::Write(tid, b"\r".to_vec()));
                            } else if DIALOG_DONT_TRUST_SELECTED.is_match(&text) {
                                actions.push(MonitorAction::Write(tid, b"\x1b[A".to_vec()));
                                actions.push(MonitorAction::Write(tid, b"\x1b[A".to_vec()));
                                actions.push(MonitorAction::Write(tid, b"\r".to_vec()));
                            } else {
                                actions.push(MonitorAction::Write(tid, b"\r".to_vec()));
                            }
                            state.buf.clear();
                            return actions;
                        }

                        // 2. Check resume session picker
                        if RESUME_SESSION_PATTERN.is_match(&text) {
                            log::info!(
                                "agend monitor: resume session picker for '{}', pressing Escape",
                                state.instance_name
                            );
                            actions.push(MonitorAction::Write(tid, b"\x1b".to_vec()));
                            state.buf.clear();
                            return actions;
                        }

                        // 3. Check for fatal errors
                        if NOT_FOUND_PATTERN.is_match(&text) {
                            log::error!(
                                "agend monitor: command not found for '{}'",
                                state.instance_name
                            );
                            actions.push(MonitorAction::Restart(state.instance_name.clone()));
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
                                break;
                            }
                        }
                    }

                    // ── Post-ready: error pattern detection (always runs) ──
                    for ep in &self.error_patterns {
                        if ep.pattern.is_match(&text) {
                            log::warn!(
                                "agend monitor: {:?} detected for '{}' → {:?}",
                                ep.kind, state.instance_name, ep.action
                            );
                            actions.push(MonitorAction::Error(
                                state.instance_name.clone(),
                                ep.kind.clone(),
                                ep.action.clone(),
                            ));
                            // Clear buffer to avoid re-triggering on same output
                            state.buf.clear();
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
        log::info!("agend monitor: started");
        loop {
            match rx.recv() {
                Ok(event) => {
                    let actions = self.process(event);
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
