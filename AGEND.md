# AgEnD-RS

AgEnD-RS is the Rust implementation of [AgEnD](https://github.com/suzuke/agend) (Agent Ensemble Daemon), built as a fork of [Zellij](https://github.com/zellij-org/zellij). It orchestrates multiple AI coding agents (Claude Code, Gemini CLI, Codex, OpenCode) as a fleet, with Telegram integration, cross-instance messaging, and fleet coordination.

## Why Rust? (vs TypeScript version)

| | AgEnD (TS) | AgEnD-RS |
|---|---|---|
| **Terminal** | tmux (paste hack) | Zellij PTY (native read/write) |
| **Observability** | Subprocess black box | `zellij attach` for full TUI |
| **Distribution** | Requires Node.js | Single binary |
| **Process model** | Fleet manager + tmux windows | Zellij tabs + daemon threads |

## Features

### Core (inherited from TS version)
- Multi-backend support: Claude Code, Gemini CLI, Codex, OpenCode
- Telegram bot integration with forum topic routing
- Cross-instance messaging (send_to_instance, broadcast, request_information, delegate_task, report_result)
- Shared decision log (post_decision, list_decisions, update_decision)
- Fleet task board (create, list, claim, done, update)
- Cron scheduler with message injection
- Instance lifecycle management (create, delete, start)

### Phase 1-4 additions
- **Event Log** — SQLite-backed audit trail with query API (list_events MCP tool)
- **PTY Error Detection** — 5 error patterns (rate limit, auth, network, overloaded, crash) with auto-notification
- **Hang Detection** — 15-minute inactivity threshold with Telegram alerts (excludes OpenCode subprocess mode)
- **Context Guardian** — Two-phase non-blocking rotation: warning injection (30s) then restart, agent saves context via post_decision
- **DAEMON_INBOX** — Unified event routing for health/error events to Telegram + event log
- **Topic Commands** — /status, /restart, /sysinfo via Telegram
- **Teams** — Group instances for targeted broadcast (create_team, list_teams, update_team, delete_team)
- **Tool Set Profiles** — full/standard/minimal per-instance tool filtering

### RS-only features (not in TS version)
- `--daemon` flag for headless background operation
- OpenCode subprocess mode (`opencode run --continue`)
- Automatic binary path resolution (`~/.opencode/bin`, `~/.cargo/bin`, `/opt/homebrew/bin`)

## Installation

### Build from source

```bash
# Clone
git clone https://github.com/suzuke/agend-rs.git
cd agend-rs
git checkout agend/main

# Build (uses agend-build.sh)
./agend-build.sh release    # release build → target/release/zellij
./agend-build.sh build      # debug build → target/debug/zellij
./agend-build.sh test       # run agend tests
./agend-build.sh run        # build + start fleet
```

### Requirements
- Rust 1.75+ (rustup recommended)
- At least one AI CLI backend installed:
  - [Claude Code](https://docs.anthropic.com/en/docs/claude-code) (`claude`)
  - [Gemini CLI](https://github.com/google-gemini/gemini-cli) (`gemini`)
  - [Codex](https://github.com/openai/codex) (`codex`)
  - [OpenCode](https://github.com/opencode-ai/opencode) (`opencode`)

## Configuration

Create `~/.agend/fleet.yaml`:

```yaml
channel:
  type: telegram
  mode: topic
  bot_token_env: TELEGRAM_BOT_TOKEN
  group_id: -1001234567890
  access:
    mode: locked
    allowed_users: [123456789]

defaults:
  backend: claude-code
  model: sonnet
  restart_policy:
    max_retries: 10
    backoff: exponential
  context_guardian:
    max_age_hours: 4
    grace_period_ms: 600000

instances:
  my-project:
    working_directory: /path/to/project
    description: "Main development instance"
    tags: [dev]
    skip_permissions: true
    topic_id: 12345

  reviewer:
    working_directory: /path/to/project
    backend: gemini-cli
    model: gemini-2.5-pro
    description: "Code reviewer"
    tags: [reviewer]
    tool_set: standard
    topic_id: 12346
```

### Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `defaults.backend` | string | `claude-code` | Default AI backend |
| `defaults.model` | string | — | Default model |
| `instances.<name>.working_directory` | path | required | Instance working dir |
| `instances.<name>.backend` | string | from defaults | AI backend |
| `instances.<name>.tags` | string[] | `[]` | Tags for broadcast filtering |
| `instances.<name>.tool_set` | string | `full` | Tool profile: full/standard/minimal |
| `instances.<name>.topic_id` | int | — | Telegram forum topic ID |
| `instances.<name>.skip_permissions` | bool | `false` | Skip CLI permission prompts |
| `instances.<name>.system_prompt` | string | — | Inline or `file:path.md` |
| `instances.<name>.display_name` | string | instance name | Display name |

### Tool set profiles

| Profile | Tools |
|---------|-------|
| `full` | All 30+ tools |
| `standard` | Excludes admin tools (create/delete instance, set_role, team/schedule CRUD) |
| `minimal` | reply, react, edit_message, send_to_instance, list_instances |

## Usage

### Start fleet (with TUI)

```bash
# Set bot token
export TELEGRAM_BOT_TOKEN=your_token_here

# Start via build script (debug build + run)
./agend-build.sh run

# Or directly with release binary
./target/release/zellij agend
```

### Start fleet (headless daemon)

```bash
./target/release/zellij agend --daemon
```

### Attach to running fleet

```bash
zellij attach agend
```

### Telegram commands

| Command | Description |
|---------|-------------|
| `/status` | Fleet status with per-instance activity icons |
| `/restart` | Graceful restart of all instances |
| `/sysinfo` | System info, IPC status, recent events |

## Architecture

```
Zellij Process
├── Screen Thread (main event loop)
│   └── drain_actions() → PTY writes + tab ops
├── Monitor Thread (PTY pattern matching)
│   ├── Ready detection (per-backend patterns)
│   ├── Dialog auto-dismissal
│   ├── Error detection (5 patterns)
│   └── Activity tracking (AtomicU64)
├── Daemon Thread (IPC + routing)
│   ├── IPC servers (Unix socket per instance)
│   ├── Telegram adapter (HTTP polling)
│   ├── DAEMON_INBOX (health/error events)
│   ├── SQLite DB (decisions, tasks, schedules, teams, events)
│   └── Topic command handler
├── Health Thread (lifecycle management)
│   ├── Hang detection (15min threshold)
│   ├── Context rotation (two-phase)
│   └── Crash recovery (exponential backoff)
└── Scheduler Thread (cron execution)
    └── Message injection to instances
```

## Differences from TS version

### Not implemented (by design)
- **tmux-manager** — Not needed; Zellij is the multiplexer
- **Transcript Monitor** — PTY monitor provides native byte-level access
- **Cost Guard** — No universal cost data source across backends
- **Statusline Watcher** — Claude Code specific, deferred
- **Web API/Dashboard** — Deferred
- **Discord channel** — Telegram only for now

### Different implementation
- **Hang Detection** — Uses PTY activity timestamps (AtomicU64) instead of transcript parsing
- **Context Guardian** — Uses decisions DB for context recovery instead of raw snapshot injection
- **Message Queue** — Telegram adapter's outbound channel acts as queue
- **Error Detection** — Real-time PTY pattern matching instead of 30s interval polling

## License

Same as Zellij — MIT License.
