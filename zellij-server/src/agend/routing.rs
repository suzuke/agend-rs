//! Routing engine — maps channel thread/topic IDs to fleet instances.

use super::config::FleetConfig;
use std::collections::HashMap;

/// The kind of route target.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteKind {
    /// A specific instance.
    Instance,
    /// The general/dispatcher topic.
    General,
}

/// A resolved route target.
#[derive(Debug, Clone)]
pub struct RouteTarget {
    pub kind: RouteKind,
    pub name: String,
}

/// Maps thread/topic IDs (as strings) to fleet instances.
pub struct RoutingEngine {
    table: HashMap<String, RouteTarget>,
}

impl RoutingEngine {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    /// Rebuild the routing table from fleet config.
    pub fn rebuild(&mut self, config: &FleetConfig) {
        self.table.clear();
        for (name, inst) in &config.instances {
            if let Some(topic_id) = inst.topic_id {
                let kind = if inst.general_topic {
                    RouteKind::General
                } else {
                    RouteKind::Instance
                };
                self.table.insert(
                    topic_id.to_string(),
                    RouteTarget {
                        kind,
                        name: name.clone(),
                    },
                );
            }
        }
        log::info!(
            "agend routing: rebuilt table with {} entries",
            self.table.len()
        );
    }

    /// Resolve a thread/topic ID to a route target.
    pub fn resolve(&self, thread_id: &str) -> Option<&RouteTarget> {
        self.table.get(thread_id)
    }

    /// Get instance name for a thread ID.
    pub fn instance_for_thread(&self, thread_id: &str) -> Option<&str> {
        self.table.get(thread_id).map(|t| t.name.as_str())
    }

    /// Register a single topic → instance mapping at runtime.
    pub fn register(&mut self, thread_id: i64, name: String) {
        self.table.insert(
            thread_id.to_string(),
            RouteTarget {
                kind: RouteKind::Instance,
                name,
            },
        );
    }

    /// Get the thread/topic ID for an instance (reverse lookup).
    pub fn thread_for_instance(&self, instance_name: &str) -> Option<&str> {
        self.table
            .iter()
            .find(|(_, t)| t.name == instance_name && t.kind == RouteKind::Instance)
            .map(|(k, _)| k.as_str())
    }

    /// Get the general/dispatcher instance (if any).
    pub fn general_instance(&self) -> Option<&str> {
        self.table
            .values()
            .find(|t| t.kind == RouteKind::General)
            .map(|t| t.name.as_str())
    }
}
