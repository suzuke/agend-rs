pub mod config;
pub mod fleet;
pub mod mcp;
pub mod routing;

/// Initialize the AgEnD subsystem.
/// Called when `zellij agend` subcommand is invoked.
pub fn run() {
    log::info!("AgEnD subsystem starting...");
    // TODO: Phase 2 — load config, start fleet manager, MCP server
    println!("agend: not yet implemented");
}
