//! SQLite storage for decisions, tasks, schedules, and event log.

use rusqlite::{params, Connection, Result as SqlResult};
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

/// AgEnD database — wraps a single SQLite connection.
pub struct AgendDb {
    conn: Connection,
}

// ── Schema ──────────────────────────────────────────────────────────────

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS decisions (
    id            TEXT PRIMARY KEY,
    project_root  TEXT NOT NULL,
    scope         TEXT NOT NULL DEFAULT 'project',
    title         TEXT NOT NULL,
    content       TEXT NOT NULL,
    tags          TEXT,
    status        TEXT NOT NULL DEFAULT 'active',
    superseded_by TEXT,
    created_by    TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    expires_at    TEXT,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_project ON decisions(project_root);
CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions(status);

CREATE TABLE IF NOT EXISTS tasks (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    description TEXT,
    status      TEXT NOT NULL DEFAULT 'open',
    priority    TEXT NOT NULL DEFAULT 'normal',
    assignee    TEXT,
    created_by  TEXT NOT NULL,
    depends_on  TEXT,
    result      TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee);

CREATE TABLE IF NOT EXISTS schedules (
    id                TEXT PRIMARY KEY,
    cron              TEXT NOT NULL,
    message           TEXT NOT NULL,
    source            TEXT NOT NULL,
    target            TEXT NOT NULL,
    reply_chat_id     TEXT NOT NULL DEFAULT '',
    reply_thread_id   TEXT,
    label             TEXT,
    enabled           INTEGER DEFAULT 1,
    timezone          TEXT DEFAULT 'Asia/Taipei',
    created_at        TEXT NOT NULL,
    last_triggered_at TEXT,
    last_status       TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_name TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    sender        TEXT,
    receiver      TEXT,
    summary       TEXT,
    payload       TEXT,
    created_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_instance ON events(instance_name, created_at);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, created_at);
"#;

// ── Data types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub id: String,
    pub project_root: String,
    pub scope: String,
    pub title: String,
    pub content: String,
    pub tags: Vec<String>,
    pub status: String,
    pub superseded_by: Option<String>,
    pub created_by: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: String,
    pub assignee: Option<String>,
    pub created_by: String,
    pub depends_on: Vec<String>,
    pub result: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: i64,
    pub instance_name: String,
    pub event_type: String,
    pub sender: Option<String>,
    pub receiver: Option<String>,
    pub summary: Option<String>,
    pub payload: Option<serde_json::Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub id: String,
    pub cron: String,
    pub message: String,
    pub source: String,
    pub target: String,
    pub label: Option<String>,
    pub enabled: bool,
    pub timezone: String,
    pub created_at: String,
    pub last_triggered_at: Option<String>,
    pub last_status: Option<String>,
}

// ── Implementation ──────────────────────────────────────────────────────

impl AgendDb {
    pub fn open(path: &Path) -> SqlResult<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> SqlResult<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn })
    }

    // ── Decisions ───────────────────────────────────────────────────────

    pub fn create_decision(
        &self,
        project_root: &str,
        scope: &str,
        title: &str,
        content: &str,
        tags: &[String],
        created_by: &str,
        ttl_days: Option<u32>,
        supersedes: Option<&str>,
    ) -> SqlResult<Decision> {
        let id = Uuid::new_v4().to_string();
        let now = now_iso();
        let expires_at = ttl_days
            .filter(|&d| d > 0)
            .map(|d| expires_iso(d));
        let tags_json = if tags.is_empty() {
            None
        } else {
            Some(serde_json::to_string(tags).unwrap())
        };

        self.conn.execute(
            "INSERT INTO decisions (id, project_root, scope, title, content, tags, created_by, created_at, expires_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?8)",
            params![id, project_root, scope, title, content, tags_json, created_by, now, expires_at],
        )?;

        if let Some(old_id) = supersedes {
            self.conn.execute(
                "UPDATE decisions SET status = 'superseded', superseded_by = ?1, updated_at = ?2 WHERE id = ?3",
                params![id, now, old_id],
            )?;
        }

        Ok(self.get_decision(&id)?.unwrap())
    }

    pub fn get_decision(&self, id: &str) -> SqlResult<Option<Decision>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, project_root, scope, title, content, tags, status, superseded_by, created_by, created_at, expires_at, updated_at FROM decisions WHERE id = ?1"
        )?;
        let mut rows = stmt.query_map(params![id], row_to_decision)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_decisions(
        &self,
        project_root: &str,
        include_archived: bool,
        tags: &[String],
    ) -> SqlResult<Vec<Decision>> {
        self.prune_expired_decisions()?;

        let mut sql = String::from(
            "SELECT id, project_root, scope, title, content, tags, status, superseded_by, created_by, created_at, expires_at, updated_at FROM decisions WHERE ((project_root = ?1 AND scope = 'project') OR scope = 'fleet')"
        );
        if !include_archived {
            sql.push_str(" AND status = 'active'");
        }
        sql.push_str(" ORDER BY CASE scope WHEN 'fleet' THEN 0 ELSE 1 END, created_at DESC");

        let mut stmt = self.conn.prepare(&sql)?;
        let rows: Vec<Decision> = stmt
            .query_map(params![project_root], row_to_decision)?
            .filter_map(|r| r.ok())
            .collect();

        if tags.is_empty() {
            Ok(rows)
        } else {
            Ok(rows
                .into_iter()
                .filter(|d| d.tags.iter().any(|t| tags.contains(t)))
                .collect())
        }
    }

    pub fn update_decision(
        &self,
        id: &str,
        content: Option<&str>,
        tags: Option<&[String]>,
        ttl_days: Option<u32>,
        archive: bool,
    ) -> SqlResult<Decision> {
        let now = now_iso();
        if archive {
            self.conn.execute(
                "UPDATE decisions SET status = 'archived', updated_at = ?1 WHERE id = ?2",
                params![now, id],
            )?;
        } else {
            let mut sets = vec!["updated_at = ?1"];
            let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(now.clone())];

            if let Some(c) = content {
                sets.push("content = ?");
                values.push(Box::new(c.to_owned()));
            }
            if let Some(t) = tags {
                sets.push("tags = ?");
                values.push(Box::new(serde_json::to_string(t).unwrap()));
            }
            if let Some(d) = ttl_days {
                sets.push("expires_at = ?");
                values.push(Box::new(if d > 0 { Some(expires_iso(d)) } else { None }));
            }
            values.push(Box::new(id.to_owned()));

            // Build parameterized query
            let placeholders: Vec<String> = (1..=values.len())
                .map(|i| format!("?{i}"))
                .collect();
            let set_clause = sets
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    if i == 0 {
                        format!("updated_at = {}", placeholders[0])
                    } else {
                        s.replace('?', &placeholders[i])
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "UPDATE decisions SET {} WHERE id = {}",
                set_clause,
                placeholders.last().unwrap()
            );
            self.conn.execute(&sql, rusqlite::params_from_iter(values.iter().map(|v| v.as_ref())))?;
        }
        Ok(self.get_decision(id)?.unwrap())
    }

    fn prune_expired_decisions(&self) -> SqlResult<usize> {
        let now = now_iso();
        let n = self.conn.execute(
            "UPDATE decisions SET status = 'archived', updated_at = ?1 WHERE status = 'active' AND expires_at IS NOT NULL AND expires_at < ?1",
            params![now],
        )?;
        Ok(n)
    }

    // ── Tasks ───────────────────────────────────────────────────────────

    pub fn create_task(
        &self,
        title: &str,
        description: Option<&str>,
        priority: &str,
        assignee: Option<&str>,
        created_by: &str,
        depends_on: &[String],
    ) -> SqlResult<Task> {
        let id = Uuid::new_v4().to_string();
        let now = now_iso();
        let deps = if depends_on.is_empty() {
            None
        } else {
            Some(serde_json::to_string(depends_on).unwrap())
        };

        self.conn.execute(
            "INSERT INTO tasks (id, title, description, priority, assignee, created_by, depends_on, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![id, title, description, priority, assignee, created_by, deps, now],
        )?;

        Ok(self.get_task(&id)?.unwrap())
    }

    pub fn get_task(&self, id: &str) -> SqlResult<Option<Task>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, title, description, status, priority, assignee, created_by, depends_on, result, created_at, updated_at FROM tasks WHERE id = ?1"
        )?;
        let mut rows = stmt.query_map(params![id], row_to_task)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_tasks(
        &self,
        filter_assignee: Option<&str>,
        filter_status: Option<&str>,
    ) -> SqlResult<Vec<Task>> {
        let mut sql = String::from(
            "SELECT id, title, description, status, priority, assignee, created_by, depends_on, result, created_at, updated_at FROM tasks WHERE 1=1"
        );
        let mut values: Vec<String> = Vec::new();
        if let Some(a) = filter_assignee {
            sql.push_str(&format!(" AND assignee = ?{}", values.len() + 1));
            values.push(a.to_owned());
        }
        if let Some(s) = filter_status {
            sql.push_str(&format!(" AND status = ?{}", values.len() + 1));
            values.push(s.to_owned());
        }
        sql.push_str(" ORDER BY CASE priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END, created_at");

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(values.iter()), row_to_task)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn claim_task(&self, id: &str, assignee: &str) -> SqlResult<Task> {
        let task = self.get_task(id)?.ok_or_else(|| {
            rusqlite::Error::QueryReturnedNoRows
        })?;
        if task.status != "open" {
            return Err(rusqlite::Error::QueryReturnedNoRows); // TODO: custom error
        }
        // Check deps
        for dep_id in &task.depends_on {
            if let Some(dep) = self.get_task(dep_id)? {
                if dep.status != "done" {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
            }
        }
        let now = now_iso();
        self.conn.execute(
            "UPDATE tasks SET status = 'claimed', assignee = ?1, updated_at = ?2 WHERE id = ?3",
            params![assignee, now, id],
        )?;
        Ok(self.get_task(id)?.unwrap())
    }

    pub fn complete_task(&self, id: &str, result: Option<&str>) -> SqlResult<Task> {
        let now = now_iso();
        self.conn.execute(
            "UPDATE tasks SET status = 'done', result = ?1, updated_at = ?2 WHERE id = ?3",
            params![result, now, id],
        )?;
        Ok(self.get_task(id)?.unwrap())
    }

    pub fn update_task(
        &self,
        id: &str,
        status: Option<&str>,
        priority: Option<&str>,
        assignee: Option<&str>,
    ) -> SqlResult<Task> {
        let now = now_iso();
        if let Some(s) = status {
            self.conn.execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
                params![s, now, id],
            )?;
        }
        if let Some(p) = priority {
            self.conn.execute(
                "UPDATE tasks SET priority = ?1, updated_at = ?2 WHERE id = ?3",
                params![p, now, id],
            )?;
        }
        if let Some(a) = assignee {
            self.conn.execute(
                "UPDATE tasks SET assignee = ?1, updated_at = ?2 WHERE id = ?3",
                params![a, now, id],
            )?;
        }
        Ok(self.get_task(id)?.unwrap())
    }

    // ── Schedules ───────────────────────────────────────────────────────

    pub fn create_schedule(
        &self,
        cron: &str,
        message: &str,
        source: &str,
        target: &str,
        label: Option<&str>,
        timezone: Option<&str>,
    ) -> SqlResult<Schedule> {
        let id = Uuid::new_v4().to_string();
        let now = now_iso();
        let tz = timezone.unwrap_or("Asia/Taipei");
        self.conn.execute(
            "INSERT INTO schedules (id, cron, message, source, target, label, timezone, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![id, cron, message, source, target, label, tz, now],
        )?;
        Ok(self.get_schedule(&id)?.unwrap())
    }

    pub fn get_schedule(&self, id: &str) -> SqlResult<Option<Schedule>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, cron, message, source, target, label, enabled, timezone, created_at, last_triggered_at, last_status FROM schedules WHERE id = ?1"
        )?;
        let mut rows = stmt.query_map(params![id], row_to_schedule)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list_schedules(&self, target: Option<&str>) -> SqlResult<Vec<Schedule>> {
        let (sql, val): (&str, Vec<String>) = match target {
            Some(t) => (
                "SELECT id, cron, message, source, target, label, enabled, timezone, created_at, last_triggered_at, last_status FROM schedules WHERE target = ?1 ORDER BY created_at",
                vec![t.to_owned()],
            ),
            None => (
                "SELECT id, cron, message, source, target, label, enabled, timezone, created_at, last_triggered_at, last_status FROM schedules ORDER BY created_at",
                vec![],
            ),
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(val.iter()), row_to_schedule)?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn update_schedule(
        &self,
        id: &str,
        cron: Option<&str>,
        message: Option<&str>,
        target: Option<&str>,
        label: Option<&str>,
        timezone: Option<&str>,
        enabled: Option<bool>,
    ) -> SqlResult<Schedule> {
        let now = now_iso();
        if let Some(v) = cron {
            self.conn.execute("UPDATE schedules SET cron = ?1 WHERE id = ?2", params![v, id])?;
        }
        if let Some(v) = message {
            self.conn.execute("UPDATE schedules SET message = ?1 WHERE id = ?2", params![v, id])?;
        }
        if let Some(v) = target {
            self.conn.execute("UPDATE schedules SET target = ?1 WHERE id = ?2", params![v, id])?;
        }
        if let Some(v) = label {
            self.conn.execute("UPDATE schedules SET label = ?1 WHERE id = ?2", params![v, id])?;
        }
        if let Some(v) = timezone {
            self.conn.execute("UPDATE schedules SET timezone = ?1 WHERE id = ?2", params![v, id])?;
        }
        if let Some(v) = enabled {
            self.conn.execute("UPDATE schedules SET enabled = ?1 WHERE id = ?2", params![v as i32, id])?;
        }
        let _ = now; // used conceptually for audit
        Ok(self.get_schedule(id)?.unwrap())
    }

    pub fn delete_schedule(&self, id: &str) -> SqlResult<()> {
        self.conn.execute("DELETE FROM schedules WHERE id = ?1", params![id])?;
        Ok(())
    }

    // ── Event Log ────────────────────────────────────────────────────────

    pub fn insert_event(
        &self,
        instance_name: &str,
        event_type: &str,
        sender: Option<&str>,
        receiver: Option<&str>,
        summary: Option<&str>,
        payload: Option<&serde_json::Value>,
    ) -> SqlResult<i64> {
        let payload_str = payload.map(|v| v.to_string());
        self.conn.execute(
            "INSERT INTO events (instance_name, event_type, sender, receiver, summary, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![instance_name, event_type, sender, receiver, summary, payload_str],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn query_events(
        &self,
        instance: Option<&str>,
        event_type: Option<&str>,
        since: Option<&str>,
        limit: Option<u32>,
    ) -> SqlResult<Vec<Event>> {
        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(inst) = instance {
            values.push(Box::new(inst.to_owned()));
            conditions.push(format!("instance_name = ?{}", values.len()));
        }
        if let Some(et) = event_type {
            values.push(Box::new(et.to_owned()));
            conditions.push(format!("event_type = ?{}", values.len()));
        }
        if let Some(s) = since {
            values.push(Box::new(s.to_owned()));
            conditions.push(format!("created_at >= ?{}", values.len()));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let lim = limit.unwrap_or(50);
        values.push(Box::new(lim));
        let sql = format!(
            "SELECT id, instance_name, event_type, sender, receiver, summary, payload, created_at \
             FROM events {} ORDER BY created_at DESC LIMIT ?{}",
            where_clause,
            values.len()
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt
            .query_map(
                rusqlite::params_from_iter(values.iter().map(|v| v.as_ref())),
                row_to_event,
            )?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn prune_events(&self, days: u32) -> SqlResult<usize> {
        let n = self.conn.execute(
            "DELETE FROM events WHERE created_at < datetime('now', ?1)",
            params![format!("-{days} days")],
        )?;
        Ok(n)
    }
}

// ── Row mappers ─────────────────────────────────────────────────────────

fn row_to_decision(row: &rusqlite::Row) -> SqlResult<Decision> {
    let tags_json: Option<String> = row.get(5)?;
    let tags: Vec<String> = tags_json
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    Ok(Decision {
        id: row.get(0)?,
        project_root: row.get(1)?,
        scope: row.get(2)?,
        title: row.get(3)?,
        content: row.get(4)?,
        tags,
        status: row.get(6)?,
        superseded_by: row.get(7)?,
        created_by: row.get(8)?,
        created_at: row.get(9)?,
        expires_at: row.get(10)?,
        updated_at: row.get(11)?,
    })
}

fn row_to_task(row: &rusqlite::Row) -> SqlResult<Task> {
    let deps_json: Option<String> = row.get(7)?;
    let depends_on: Vec<String> = deps_json
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    Ok(Task {
        id: row.get(0)?,
        title: row.get(1)?,
        description: row.get(2)?,
        status: row.get(3)?,
        priority: row.get(4)?,
        assignee: row.get(5)?,
        created_by: row.get(6)?,
        depends_on,
        result: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

fn row_to_schedule(row: &rusqlite::Row) -> SqlResult<Schedule> {
    let enabled: i32 = row.get(6)?;
    Ok(Schedule {
        id: row.get(0)?,
        cron: row.get(1)?,
        message: row.get(2)?,
        source: row.get(3)?,
        target: row.get(4)?,
        label: row.get(5)?,
        enabled: enabled != 0,
        timezone: row.get(7)?,
        created_at: row.get(8)?,
        last_triggered_at: row.get(9)?,
        last_status: row.get(10)?,
    })
}

fn row_to_event(row: &rusqlite::Row) -> SqlResult<Event> {
    let payload_str: Option<String> = row.get(6)?;
    let payload = payload_str.and_then(|s| serde_json::from_str(&s).ok());
    Ok(Event {
        id: row.get(0)?,
        instance_name: row.get(1)?,
        event_type: row.get(2)?,
        sender: row.get(3)?,
        receiver: row.get(4)?,
        summary: row.get(5)?,
        payload,
        created_at: row.get(7)?,
    })
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn expires_iso(days: u32) -> String {
    (chrono::Utc::now() + chrono::Duration::days(days as i64)).to_rfc3339()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_crud() {
        let db = AgendDb::open_in_memory().unwrap();

        let d = db
            .create_decision("/tmp/proj", "project", "Use Rust", "We chose Rust", &[], "inst-a", None, None)
            .unwrap();
        assert_eq!(d.title, "Use Rust");
        assert_eq!(d.status, "active");

        let list = db.list_decisions("/tmp/proj", false, &[]).unwrap();
        assert_eq!(list.len(), 1);

        let d2 = db
            .update_decision(&d.id, Some("Updated content"), None, None, false)
            .unwrap();
        assert_eq!(d2.content, "Updated content");

        let d3 = db.update_decision(&d.id, None, None, None, true).unwrap();
        assert_eq!(d3.status, "archived");
    }

    #[test]
    fn task_lifecycle() {
        let db = AgendDb::open_in_memory().unwrap();

        let t = db
            .create_task("Fix bug", Some("Segfault on login"), "high", None, "inst-a", &[])
            .unwrap();
        assert_eq!(t.status, "open");

        let t = db.claim_task(&t.id, "inst-b").unwrap();
        assert_eq!(t.status, "claimed");
        assert_eq!(t.assignee.as_deref(), Some("inst-b"));

        let t = db.complete_task(&t.id, Some("Fixed in commit abc")).unwrap();
        assert_eq!(t.status, "done");
        assert_eq!(t.result.as_deref(), Some("Fixed in commit abc"));
    }

    #[test]
    fn schedule_crud() {
        let db = AgendDb::open_in_memory().unwrap();

        let s = db
            .create_schedule("0 7 * * *", "Good morning", "inst-a", "inst-a", Some("Daily"), None)
            .unwrap();
        assert!(s.enabled);
        assert_eq!(s.cron, "0 7 * * *");

        let list = db.list_schedules(None).unwrap();
        assert_eq!(list.len(), 1);

        let s = db.update_schedule(&s.id, None, None, None, None, None, Some(false)).unwrap();
        assert!(!s.enabled);

        db.delete_schedule(&s.id).unwrap();
        let list = db.list_schedules(None).unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn event_log() {
        let db = AgendDb::open_in_memory().unwrap();

        // Insert events
        let id1 = db
            .insert_event("inst-a", "telegram_message", Some("user:123"), Some("inst-a"), Some("Hello"), None)
            .unwrap();
        assert!(id1 > 0);

        let payload = serde_json::json!({"reason": "max_age_hours exceeded"});
        db.insert_event("inst-a", "context_rotation", None, None, Some("Rotating context"), Some(&payload))
            .unwrap();
        db.insert_event("inst-b", "crash_respawn", None, None, Some("Instance crashed"), None)
            .unwrap();

        // Query all
        let all = db.query_events(None, None, None, None).unwrap();
        assert_eq!(all.len(), 3);

        // Query by instance
        let inst_a = db.query_events(Some("inst-a"), None, None, None).unwrap();
        assert_eq!(inst_a.len(), 2);

        // Query by type
        let crashes = db.query_events(None, Some("crash_respawn"), None, None).unwrap();
        assert_eq!(crashes.len(), 1);
        assert_eq!(crashes[0].instance_name, "inst-b");

        // Query with limit
        let limited = db.query_events(None, None, None, Some(1)).unwrap();
        assert_eq!(limited.len(), 1);

        // Verify payload deserialization
        let rotations = db.query_events(None, Some("context_rotation"), None, None).unwrap();
        assert_eq!(rotations.len(), 1);
        let p = rotations[0].payload.as_ref().unwrap();
        assert_eq!(p["reason"], "max_age_hours exceeded");

        // Prune (all events have just been created, so pruning old ones removes nothing)
        let pruned = db.prune_events(1).unwrap();
        assert_eq!(pruned, 0);
        let still_all = db.query_events(None, None, None, None).unwrap();
        assert_eq!(still_all.len(), 3);
    }

    #[test]
    fn decision_scoping() {
        let db = AgendDb::open_in_memory().unwrap();

        db.create_decision("/proj-a", "project", "A-local", "content", &[], "a", None, None).unwrap();
        db.create_decision("/proj-b", "project", "B-local", "content", &[], "b", None, None).unwrap();
        db.create_decision("/proj-a", "fleet", "Fleet-wide", "content", &[], "a", None, None).unwrap();

        // proj-a sees its own + fleet
        let list_a = db.list_decisions("/proj-a", false, &[]).unwrap();
        assert_eq!(list_a.len(), 2);

        // proj-b sees its own + fleet
        let list_b = db.list_decisions("/proj-b", false, &[]).unwrap();
        assert_eq!(list_b.len(), 2);
    }
}
