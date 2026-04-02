//! Cron scheduler engine — triggers schedules and delivers messages.
//!
//! Polls the SQLite schedules table every 60 seconds, checks which
//! schedules are due based on their cron expression, and injects
//! messages into target instances.

use super::db::{AgendDb, Schedule};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Simple cron field matcher (supports *, N, and */N).
struct CronExpr {
    minute: CronField,
    hour: CronField,
    day: CronField,
    month: CronField,
    weekday: CronField,
}

enum CronField {
    Any,
    Exact(u32),
    Step(u32),
}

impl CronExpr {
    fn parse(expr: &str) -> Option<Self> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return None;
        }
        Some(Self {
            minute: CronField::parse(parts[0])?,
            hour: CronField::parse(parts[1])?,
            day: CronField::parse(parts[2])?,
            month: CronField::parse(parts[3])?,
            weekday: CronField::parse(parts[4])?,
        })
    }

    fn matches(&self, minute: u32, hour: u32, day: u32, month: u32, weekday: u32) -> bool {
        self.minute.matches(minute)
            && self.hour.matches(hour)
            && self.day.matches(day)
            && self.month.matches(month)
            && self.weekday.matches(weekday)
    }
}

impl CronField {
    fn parse(s: &str) -> Option<Self> {
        if s == "*" {
            Some(Self::Any)
        } else if let Some(step) = s.strip_prefix("*/") {
            step.parse().ok().map(Self::Step)
        } else {
            s.parse().ok().map(Self::Exact)
        }
    }

    fn matches(&self, value: u32) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(n) => value == *n,
            Self::Step(n) if *n == 0 => false,
            Self::Step(n) => value % n == 0,
        }
    }
}

/// Run the scheduler loop. Checks every 60s for due schedules.
pub fn run_scheduler() {
    log::info!("agend scheduler: started");
    let mut last_check: HashMap<String, Instant> = HashMap::new();

    loop {
        std::thread::sleep(Duration::from_secs(60));

        let db = match AgendDb::open(&super::paths::db_path()) {
            Ok(db) => db,
            Err(e) => {
                log::warn!("agend scheduler: db open failed: {e}");
                continue;
            },
        };

        let schedules = match db.list_schedules(None) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("agend scheduler: list failed: {e}");
                continue;
            },
        };

        let now = chrono::Local::now();
        let minute = now.format("%M").to_string().parse::<u32>().unwrap_or(0);
        let hour = now.format("%H").to_string().parse::<u32>().unwrap_or(0);
        let day = now.format("%d").to_string().parse::<u32>().unwrap_or(1);
        let month = now.format("%m").to_string().parse::<u32>().unwrap_or(1);
        let weekday = now.format("%u").to_string().parse::<u32>().unwrap_or(1) % 7; // 0=Sun

        for schedule in &schedules {
            if !schedule.enabled {
                continue;
            }

            // Don't fire same schedule within 60s
            if let Some(last) = last_check.get(&schedule.id) {
                if last.elapsed() < Duration::from_secs(60) {
                    continue;
                }
            }

            let cron = match CronExpr::parse(&schedule.cron) {
                Some(c) => c,
                None => {
                    log::warn!("agend scheduler: invalid cron '{}' for schedule {}", schedule.cron, schedule.id);
                    continue;
                },
            };

            if cron.matches(minute, hour, day, month, weekday) {
                log::info!(
                    "agend scheduler: triggering '{}' → {} (cron: {})",
                    schedule.label.as_deref().unwrap_or(&schedule.id),
                    schedule.target,
                    schedule.cron
                );

                // Inject message into target instance
                let formatted = format!(
                    "[schedule:{}] {}\n",
                    schedule.label.as_deref().unwrap_or("cron"),
                    schedule.message
                );

                if let Some(tid) = super::terminal_for_instance(&schedule.target) {
                    let mut bytes = Vec::with_capacity(formatted.len() + 1);
                    bytes.extend_from_slice(formatted.as_bytes());
                    bytes.push(b'\r');
                    super::send_daemon_action(super::DaemonAction::Write(tid, bytes));
                } else {
                    log::warn!("agend scheduler: no terminal for target '{}'", schedule.target);
                }

                last_check.insert(schedule.id.clone(), Instant::now());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_parsing() {
        let expr = CronExpr::parse("0 9 * * *").unwrap();
        assert!(expr.matches(0, 9, 15, 6, 1));
        assert!(!expr.matches(30, 9, 15, 6, 1));
        assert!(!expr.matches(0, 10, 15, 6, 1));
    }

    #[test]
    fn cron_step() {
        let expr = CronExpr::parse("*/15 * * * *").unwrap();
        assert!(expr.matches(0, 9, 1, 1, 0));
        assert!(expr.matches(15, 9, 1, 1, 0));
        assert!(expr.matches(30, 9, 1, 1, 0));
        assert!(!expr.matches(7, 9, 1, 1, 0));
    }

    #[test]
    fn cron_any() {
        let expr = CronExpr::parse("* * * * *").unwrap();
        assert!(expr.matches(0, 0, 1, 1, 0));
        assert!(expr.matches(59, 23, 31, 12, 6));
    }
}
