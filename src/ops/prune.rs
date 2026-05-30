use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use crate::ops::list as ops_list;
use crate::retention::policy::{KeepReason, RetentionConfig, apply};
use crate::zfs::client;


pub struct PruneResult {
    pub kept: Vec<(String, KeepReason)>,
    pub deleted: Vec<String>,
    /// True when pruning was skipped because a resume transfer is in progress.
    pub resume_skipped: bool,
    /// Snapshots skipped because they carry a Transfer Hold (`zrb:*`), as
    /// `(snapshot_name, hold_tag)` pairs.
    pub hold_skipped: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ResumeDecision {
    Idle,   // no token; clear any stale since property
    Wait,   // token present, hold period not yet elapsed; skip snapshot pruning
    Expire, // token present, hold period elapsed; abort
}

pub(crate) fn resume_decision(
    has_token: bool,
    since: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    hold_days: Option<u32>,
) -> ResumeDecision {
    if !has_token {
        return ResumeDecision::Idle;
    }
    let Some(since) = since else {
        // Server has not yet annotated; treat as in-progress and wait.
        return ResumeDecision::Wait;
    };
    match hold_days {
        None => ResumeDecision::Expire,
        Some(days) => {
            if now - since >= Duration::days(i64::from(days)) {
                ResumeDecision::Expire
            } else {
                ResumeDecision::Wait
            }
        }
    }
}


/// Return all `zrb:*`-prefixed tags from `holds` (the full tag string, not stripped).
///
/// Used to identify Transfer Holds before attempting to destroy a snapshot.
pub(crate) fn zrb_holds_from_tags(holds: &[String]) -> Vec<String> {
    holds
        .iter()
        .filter(|h| h.starts_with("zrb:"))
        .cloned()
        .collect()
}

/// Classify `candidates` into snapshots to delete and snapshots to skip due to
/// Transfer Holds, using a pre-fetched `holds_map` (snapshot → hold tags).
///
/// Returns `(to_delete, hold_skipped)` where `hold_skipped` is a vec of
/// `(snapshot_name, hold_tag)` pairs — one entry per `zrb:*` tag on each
/// protected snapshot.
pub(crate) fn classify_candidates(
    candidates: Vec<String>,
    holds_map: &HashMap<String, Vec<String>>,
) -> (Vec<String>, Vec<(String, String)>) {
    let empty = Vec::new();
    let mut to_delete = Vec::new();
    let mut hold_skipped = Vec::new();
    for snap in candidates {
        let zrb_holds = zrb_holds_from_tags(holds_map.get(&snap).unwrap_or(&empty));
        if zrb_holds.is_empty() {
            to_delete.push(snap);
        } else {
            for tag in zrb_holds {
                hold_skipped.push((snap.clone(), tag));
            }
        }
    }
    (to_delete, hold_skipped)
}

/// Apply the Retention Policy to `dataset` and destroy out-of-policy snapshots.
///
/// If the dataset has an unexpired resume token (`Wait`), snapshot deletion is
/// skipped to avoid invalidating the in-progress receive — unless `abort_resume`
/// is true, in which case the token is aborted and pruning proceeds regardless.
/// With `dry_run = true`, no ZFS mutations are performed; the function returns
/// what *would* happen.  A dry-run `Wait` (without `abort_resume`) sets
/// `resume_skipped = true` on the returned result.
///
/// Only zrb-managed snapshots are affected; others are ignored by `ops::list`.
///
/// # Errors
/// Propagates errors from `ops::list`, ZFS resume operations, or any `zfs destroy`.
pub fn prune(
    dataset: &str,
    config: &RetentionConfig,
    hold_days: Option<u32>,
    dry_run: bool,
    abort_resume: bool,
) -> anyhow::Result<PruneResult> {
    let has_token = client::get_resume_token(dataset)?.is_some();
    let since = client::get_resume_since(dataset)?;
    let now = Utc::now();
    let had_since = since.is_some();

    let mut decision = resume_decision(has_token, since, now, hold_days);
    if abort_resume && decision == ResumeDecision::Wait {
        decision = ResumeDecision::Expire;
    }
    log::debug!("{dataset}: resume decision: {decision:?}");
    match decision {
        ResumeDecision::Idle => {
            if had_since && !dry_run {
                client::clear_resume_since(dataset)?;
            }
        }
        ResumeDecision::Wait => {
            return Ok(PruneResult {
                kept: vec![],
                deleted: vec![],
                resume_skipped: true,
                hold_skipped: vec![],
            });
        }
        ResumeDecision::Expire => {
            if !dry_run {
                client::abort_resume(dataset)?;
                client::clear_resume_since(dataset)?;
            }
        }
    }

    let snapshots = ops_list::list(dataset)?;
    let (kept, candidates) = apply(&snapshots, Utc::now(), config);

    // One batched `zfs holds -H` call covers all candidates instead of N individual calls.
    let holds_map = client::batch_snapshot_holds(&candidates)?;
    let (to_delete, hold_skipped) = classify_candidates(candidates, &holds_map);

    for (snap, tag) in &hold_skipped {
        log::info!("skipped {snap} (Transfer Hold: {tag})");
    }

    let mut deleted = Vec::new();
    for snap in to_delete {
        if !dry_run {
            client::destroy_snapshot(&snap)?;
        }
        deleted.push(snap);
    }

    Ok(PruneResult {
        kept,
        deleted,
        resume_skipped: false,
        hold_skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap()
    }

    // ── resume_decision ───────────────────────────────────────────────────

    #[test]
    fn no_token_is_idle() {
        assert_eq!(
            resume_decision(false, None, now(), Some(3)),
            ResumeDecision::Idle
        );
    }

    #[test]
    fn no_token_with_stale_since_is_idle() {
        let stale = now() - Duration::days(5);
        assert_eq!(
            resume_decision(false, Some(stale), now(), Some(3)),
            ResumeDecision::Idle
        );
    }

    #[test]
    fn token_no_since_is_wait() {
        // Server has not yet annotated; prune waits rather than starting the timer.
        assert_eq!(
            resume_decision(true, None, now(), Some(3)),
            ResumeDecision::Wait
        );
    }

    #[test]
    fn token_no_since_no_hold_days_is_wait() {
        assert_eq!(
            resume_decision(true, None, now(), None),
            ResumeDecision::Wait
        );
    }

    #[test]
    fn token_within_hold_period_is_wait() {
        let since = now() - Duration::days(2);
        assert_eq!(
            resume_decision(true, Some(since), now(), Some(3)),
            ResumeDecision::Wait
        );
    }

    #[test]
    fn token_exactly_at_hold_boundary_is_expire() {
        let since = now() - Duration::days(3);
        assert_eq!(
            resume_decision(true, Some(since), now(), Some(3)),
            ResumeDecision::Expire
        );
    }

    #[test]
    fn token_past_hold_period_is_expire() {
        let since = now() - Duration::days(5);
        assert_eq!(
            resume_decision(true, Some(since), now(), Some(3)),
            ResumeDecision::Expire
        );
    }

    #[test]
    fn token_with_no_hold_days_always_expires() {
        let ancient = now() + Duration::days(9999);
        assert_eq!(
            resume_decision(true, Some(ancient), now(), None),
            ResumeDecision::Expire
        );
    }

    // ── zrb_holds_from_tags ───────────────────────────────────────────────

    #[test]
    fn empty_holds_gives_empty_vec() {
        assert_eq!(zrb_holds_from_tags(&[]), Vec::<String>::new());
    }

    #[test]
    fn non_zrb_holds_excluded() {
        let holds = vec!["manual".to_owned(), "other-tool:data".to_owned()];
        assert_eq!(zrb_holds_from_tags(&holds), Vec::<String>::new());
    }

    #[test]
    fn single_zrb_hold_returned_as_is() {
        let holds = vec!["zrb:primary".to_owned()];
        assert_eq!(zrb_holds_from_tags(&holds), vec!["zrb:primary"]);
    }

    #[test]
    fn multiple_zrb_holds_all_returned() {
        let holds = vec![
            "zrb:primary".to_owned(),
            "zrb:offsite".to_owned(),
            "manual".to_owned(),
        ];
        let got = zrb_holds_from_tags(&holds);
        assert_eq!(got, vec!["zrb:primary", "zrb:offsite"]);
    }

    #[test]
    fn server_side_received_tag_is_a_zrb_hold() {
        let holds = vec!["zrb:received".to_owned()];
        assert_eq!(zrb_holds_from_tags(&holds), vec!["zrb:received"]);
    }

    // ── classify_candidates ───────────────────────────────────────────────

    fn holds(snap: &str, tags: &[&str]) -> (String, Vec<String>) {
        (snap.to_owned(), tags.iter().map(|t| (*t).to_owned()).collect())
    }

    #[test]
    fn no_holds_all_candidates_go_to_delete() {
        let candidates = vec!["tank/data@zrb-A".to_owned(), "tank/data@zrb-B".to_owned()];
        let map = std::collections::HashMap::new();
        let (to_delete, hold_skipped) = classify_candidates(candidates.clone(), &map);
        assert_eq!(to_delete, candidates);
        assert!(hold_skipped.is_empty());
    }

    #[test]
    fn zrb_held_snapshot_goes_to_hold_skipped_not_deleted() {
        let candidates = vec!["tank/data@zrb-A".to_owned()];
        let map = [holds("tank/data@zrb-A", &["zrb:backup"])].into_iter().collect();
        let (to_delete, hold_skipped) = classify_candidates(candidates, &map);
        assert!(to_delete.is_empty());
        assert_eq!(hold_skipped, vec![("tank/data@zrb-A".to_owned(), "zrb:backup".to_owned())]);
    }

    #[test]
    fn non_zrb_hold_does_not_protect_snapshot() {
        let candidates = vec!["tank/data@zrb-A".to_owned()];
        let map = [holds("tank/data@zrb-A", &["manual"])].into_iter().collect();
        let (to_delete, hold_skipped) = classify_candidates(candidates.clone(), &map);
        assert_eq!(to_delete, candidates);
        assert!(hold_skipped.is_empty());
    }

    #[test]
    fn multiple_zrb_tags_on_one_snapshot_produces_multiple_hold_skipped_entries() {
        let candidates = vec!["tank/data@zrb-A".to_owned()];
        let map = [holds("tank/data@zrb-A", &["zrb:primary", "zrb:offsite"])].into_iter().collect();
        let (to_delete, hold_skipped) = classify_candidates(candidates, &map);
        assert!(to_delete.is_empty());
        assert_eq!(hold_skipped.len(), 2);
        assert!(hold_skipped.contains(&("tank/data@zrb-A".to_owned(), "zrb:primary".to_owned())));
        assert!(hold_skipped.contains(&("tank/data@zrb-A".to_owned(), "zrb:offsite".to_owned())));
    }

    #[test]
    fn mix_of_held_and_unheld_classified_correctly() {
        let candidates = vec![
            "tank/data@zrb-A".to_owned(),
            "tank/data@zrb-B".to_owned(),
            "tank/data@zrb-C".to_owned(),
        ];
        let map = [holds("tank/data@zrb-B", &["zrb:backup"])].into_iter().collect();
        let (to_delete, hold_skipped) = classify_candidates(candidates, &map);
        assert_eq!(to_delete, vec!["tank/data@zrb-A", "tank/data@zrb-C"]);
        assert_eq!(hold_skipped, vec![("tank/data@zrb-B".to_owned(), "zrb:backup".to_owned())]);
    }
}
