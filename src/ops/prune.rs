use chrono::{DateTime, Duration, Utc};

use crate::ops::list as ops_list;
use crate::retention::policy::{KeepReason, RetentionConfig, apply};
use crate::zfs::client;


pub struct PruneResult {
    pub kept: Vec<(String, KeepReason)>,
    pub deleted: Vec<String>,
    /// True when pruning was skipped because a resume transfer is in progress.
    pub resume_skipped: bool,
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
    let (kept, deleted) = apply(&snapshots, Utc::now(), config);
    if !dry_run {
        for snap in &deleted {
            client::destroy_snapshot(snap)?;
        }
    }
    Ok(PruneResult {
        kept,
        deleted,
        resume_skipped: false,
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
}
