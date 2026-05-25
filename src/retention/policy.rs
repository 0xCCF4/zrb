use crate::snapshot::naming;
use chrono::{DateTime, Datelike, Duration, IsoWeek, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetentionConfig {
    pub recent: usize,
    pub weekly_for_days: i64,
    pub monthly_for_days: i64,
}

/// Partition `snapshots` into `(keep, delete)` according to the tiered policy.
///
/// Snapshots not managed by zrb (no valid timestamp) are kept unconditionally.
#[must_use]
pub fn apply(
    snapshots: &[String],
    now: DateTime<Utc>,
    config: &RetentionConfig,
) -> (Vec<String>, Vec<String>) {
    let mut parsed: Vec<(String, Option<DateTime<Utc>>)> = snapshots
        .iter()
        .map(|s| (s.clone(), naming::parse(s)))
        .collect();

    // Sort chronologically so "last N" is unambiguous.
    parsed.sort_by_key(|(_, ts)| *ts);

    let total = parsed.len();
    let recent_start = total.saturating_sub(config.recent);

    // The most-recent `config.recent` snapshots are always kept.
    let recent_set: std::collections::HashSet<usize> = (recent_start..total).collect();

    let weekly_cutoff = now - Duration::days(config.weekly_for_days);
    let monthly_cutoff = now - Duration::days(config.monthly_for_days);

    // For weekly/monthly/yearly we keep the *oldest* snapshot in each bucket
    // (first encountered when iterating oldest-first).
    let mut seen_weeks: std::collections::HashSet<(i32, IsoWeek)> =
        std::collections::HashSet::new();
    let mut seen_months: std::collections::HashSet<(i32, u32)> = std::collections::HashSet::new();
    let mut seen_years: std::collections::HashSet<i32> = std::collections::HashSet::new();

    let mut keep = Vec::new();
    let mut delete = Vec::new();

    for (idx, (name, ts_opt)) in parsed.iter().enumerate() {
        if recent_set.contains(&idx) {
            keep.push(name.clone());
            continue;
        }
        let Some(ts) = ts_opt else {
            // Non-zrb snapshot: keep unconditionally.
            keep.push(name.clone());
            continue;
        };

        if *ts > weekly_cutoff {
            // Weekly window.
            let bucket = (ts.year(), ts.iso_week());
            if seen_weeks.insert(bucket) {
                keep.push(name.clone());
            } else {
                delete.push(name.clone());
            }
        } else if *ts > monthly_cutoff {
            // Monthly window.
            let bucket = (ts.year(), ts.month());
            if seen_months.insert(bucket) {
                keep.push(name.clone());
            } else {
                delete.push(name.clone());
            }
        } else {
            // Yearly window.
            if seen_years.insert(ts.year()) {
                keep.push(name.clone());
            } else {
                delete.push(name.clone());
            }
        }
    }

    (keep, delete)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn cfg(recent: usize, weekly: i64, monthly: i64) -> RetentionConfig {
        RetentionConfig {
            recent,
            weekly_for_days: weekly,
            monthly_for_days: monthly,
        }
    }

    fn snap(dataset: &str, days_ago: i64, now: DateTime<Utc>) -> String {
        let ts = now - Duration::days(days_ago);
        naming::new_name(dataset, ts)
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 22, 12, 0, 0).unwrap()
    }

    #[test]
    fn empty_list_returns_empty_vecs() {
        let (keep, delete) = apply(&[], now(), &cfg(7, 30, 365));
        assert!(keep.is_empty());
        assert!(delete.is_empty());
    }

    #[test]
    fn fewer_than_recent_all_kept() {
        let now = now();
        let snaps: Vec<String> = (1..=3).map(|d| snap("pool/data", d, now)).collect();
        let (keep, delete) = apply(&snaps, now, &cfg(7, 30, 365));
        assert_eq!(keep.len(), 3);
        assert!(delete.is_empty());
    }

    #[test]
    fn exactly_recent_all_kept() {
        let now = now();
        let snaps: Vec<String> = (1..=7).map(|d| snap("pool/data", d, now)).collect();
        let (keep, delete) = apply(&snaps, now, &cfg(7, 30, 365));
        assert_eq!(keep.len(), 7);
        assert!(delete.is_empty());
    }

    #[test]
    fn beyond_recent_weekly_window_one_per_week() {
        let now = now();
        // 7 snapshots in the Recent bucket (days 1-7)
        // 2 extra snapshots from 8 and 9 days ago — same ISO week → only 1 survives
        let mut snaps: Vec<String> = (1..=7).map(|d| snap("pool/data", d, now)).collect();
        snaps.push(snap("pool/data", 8, now));
        snaps.push(snap("pool/data", 9, now));
        let (keep, delete) = apply(&snaps, now, &cfg(7, 30, 365));
        // 7 recent + 1 weekly survivor = 8 kept; 1 deleted
        assert_eq!(delete.len(), 1);
        assert_eq!(keep.len(), 8);
    }

    #[test]
    fn monthly_window_one_per_month() {
        let now = now();
        // Recent = 1 (just today), weekly window = 7 days, monthly = 60 days
        // Put 2 snapshots in the same month but outside the weekly window
        let snaps = vec![
            snap("pool/data", 1, now),  // recent
            snap("pool/data", 35, now), // monthly window, first in April
            snap("pool/data", 40, now), // monthly window, same month as 35d ago → deleted
        ];
        let (keep, delete) = apply(&snaps, now, &cfg(1, 7, 60));
        assert_eq!(delete.len(), 1);
        assert_eq!(keep.len(), 2);
    }

    #[test]
    fn yearly_window_one_per_year() {
        let now = now();
        // monthly window = 60 days, so 366+ days ago is in yearly window
        let snaps = vec![
            snap("pool/data", 1, now),   // recent
            snap("pool/data", 366, now), // yearly — first in that year
            snap("pool/data", 370, now), // yearly — same year → deleted
        ];
        let (keep, delete) = apply(&snaps, now, &cfg(1, 7, 60));
        assert_eq!(delete.len(), 1);
        assert_eq!(keep.len(), 2);
    }

    #[test]
    fn snapshot_on_weekly_boundary_is_kept() {
        let now = now();
        // Snapshot exactly at the weekly cutoff boundary should fall into weekly bucket.
        let snaps = vec![
            snap("pool/data", 1, now),
            snap("pool/data", 30, now), // exactly at weekly_for_days=30 boundary
        ];
        let (keep, delete) = apply(&snaps, now, &cfg(1, 30, 365));
        // The 30d-ago snapshot is on the boundary — Duration subtraction means
        // ts == weekly_cutoff so ts > weekly_cutoff is false; it falls into monthly.
        // Either way it must not be deleted (it's the only one in its bucket).
        assert!(delete.is_empty(), "boundary snapshot should not be deleted");
        assert_eq!(keep.len(), 2);
    }
}
