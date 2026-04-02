//! Tests for the agend module.

#[cfg(test)]
mod tests {
    use crate::agend::config::FleetConfig;
    use crate::agend::fleet::FleetManager;
    use crate::agend::monitor::{Monitor, PtyEvent};

    #[test]
    fn parse_fleet_yaml() {
        let yaml = r#"
defaults:
  backend: claude-code

instances:
  my-project:
    working_directory: /tmp/test
    skip_permissions: true
  gemini-proj:
    working_directory: /tmp/gemini
    backend: gemini-cli
    model: gemini-2.5-pro
"#;
        let config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.instances.len(), 2);
        assert_eq!(config.defaults.backend, "claude-code");

        let my = &config.instances["my-project"];
        assert!(my.skip_permissions);
        assert_eq!(my.backend_or(&config.defaults), "claude-code");

        let gem = &config.instances["gemini-proj"];
        assert_eq!(gem.backend_or(&config.defaults), "gemini-cli");
        assert_eq!(gem.model.as_deref(), Some("gemini-2.5-pro"));
    }

    #[test]
    fn generate_layout_kdl() {
        let yaml = r#"
defaults:
  backend: claude-code

instances:
  test-inst:
    working_directory: /tmp/test
    skip_permissions: true
"#;
        let config: FleetConfig = serde_yaml::from_str(yaml).unwrap();
        let layout = FleetManager::generate_layout(&config);

        assert!(layout.contains("tab name=\"test-inst\""));
        assert!(layout.contains("pane command=\"claude\""));
        assert!(layout.contains("cwd=\"/tmp/test\""));
        assert!(layout.contains("--dangerously-skip-permissions"));
    }

    #[test]
    fn monitor_detects_claude_ready() {
        let mut monitor = Monitor::new();
        monitor.register(1, "test".into(), "claude-code".into());

        // Not ready yet
        let actions = monitor.process(PtyEvent::Bytes(1, b"Loading...".to_vec()));
        assert!(actions.is_empty());

        // Ready!
        let actions = monitor.process(PtyEvent::Bytes(1, "❯ ".as_bytes().to_vec()));
        assert!(actions.is_empty()); // ready detected, no action needed

        // Verify state
        assert!(monitor.terminals.get(&1).unwrap().ready);
    }

    #[test]
    fn monitor_detects_dialog() {
        let mut monitor = Monitor::new();
        monitor.register(1, "test".into(), "claude-code".into());

        let actions = monitor.process(PtyEvent::Bytes(
            1,
            b"Do you trust this project?\n  1. Yes, I accept\n  2. No, exit".to_vec(),
        ));

        // Should produce a write action (Enter to dismiss)
        assert!(!actions.is_empty());
    }

    #[test]
    fn monitor_detects_gemini_ready() {
        let mut monitor = Monitor::new();
        monitor.register(2, "gemini-inst".into(), "gemini-cli".into());

        let actions = monitor.process(PtyEvent::Bytes(
            2,
            b"* Type your message or @path/to/file".to_vec(),
        ));
        assert!(actions.is_empty());
        assert!(monitor.terminals.get(&2).unwrap().ready);
    }

    #[test]
    fn monitor_ignores_unregistered_terminals() {
        let mut monitor = Monitor::new();
        // terminal 99 not registered
        let actions = monitor.process(PtyEvent::Bytes(99, b"hello".to_vec()));
        assert!(actions.is_empty());
    }
}
