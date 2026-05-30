use std::collections::HashMap;
use std::process::{Command, Stdio};

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::process::{
    Child as TokioChild, ChildStdin as TokioChildStdin, ChildStdout as TokioChildStdout,
    Command as TokioCommand,
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to spawn zfs process: {0}")]
    Spawn(std::io::Error),
    #[error("zfs command failed: {stderr}")]
    CommandFailed { stderr: String },
    #[error("zfs output could not be parsed as JSON: {0}")]
    JsonParse(#[from] serde_json::Error),
}

fn is_dataset_not_found(err: &ClientError) -> bool {
    matches!(err, ClientError::CommandFailed { stderr } if stderr.contains("dataset does not exist"))
}

fn run(mut cmd: Command) -> Result<String, ClientError> {
    let output = cmd.output().map_err(ClientError::Spawn)?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(ClientError::CommandFailed {
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// Create a snapshot `<dataset>@<snapshot_name>`.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs` process cannot be spawned or exits non-zero.
pub fn create_snapshot(dataset: &str, snapshot_name: &str) -> Result<(), ClientError> {
    log::trace!("zfs snapshot {dataset}@{snapshot_name}");
    let mut cmd = Command::new("zfs");
    cmd.arg("snapshot")
        .arg(format!("{dataset}@{snapshot_name}"));
    run(cmd).map(|_| ())
}

/// List all snapshots for `dataset`, returning raw `<dataset>@<name>` strings.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs` process cannot be spawned or exits non-zero.
pub fn list_snapshots(dataset: &str) -> Result<Vec<String>, ClientError> {
    log::trace!("zfs list snapshots for {dataset}");
    let mut cmd = Command::new("zfs");
    cmd.args(["list", "-t", "snapshot", "-H", "-o", "name", dataset]);
    match run(cmd) {
        Ok(stdout) => Ok(parse_list_output(&stdout)),
        Err(e) if is_dataset_not_found(&e) => Ok(vec![]),
        Err(e) => Err(e),
    }
}

fn parse_list_output(output: &str) -> Vec<String> {
    output
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Destroy a snapshot by its full name (`<dataset>@<snapshot>`).
///
/// # Errors
/// Returns [`ClientError`] if the `zfs` process cannot be spawned or exits non-zero.
///
/// # Panics
/// Panics if `snapshot` is not of the form `<dataset>@zrb-<suffix>` — guardrail against
/// accidentally destroying a dataset instead of a zrb-managed snapshot.
pub fn destroy_snapshot(snapshot: &str) -> Result<(), ClientError> {
    assert!(
        snapshot
            .split_once('@')
            .is_some_and(|(_, name)| name.starts_with("zrb-"))
            && snapshot.split('@').count() == 2,
        "Guardrail tripped: not a zrb snapshot: {snapshot}"
    );
    log::trace!("zfs destroy {snapshot}");
    let mut cmd = Command::new("zfs");
    cmd.arg("destroy").arg(snapshot);
    run(cmd).map(|_| ())
}

/// Spawn `zfs send [-i <base>] <snapshot>` and return the async stdout pipe.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned.
///
/// # Panics
/// Never panics — stdout is always present because `Stdio::piped()` is set unconditionally.
pub fn send_incremental(
    base: Option<&str>,
    snapshot: &str,
    opts: &[String],
) -> Result<TokioChildStdout, ClientError> {
    match base {
        Some(b) => log::trace!("zfs send -i {b} {snapshot}"),
        None => log::trace!("zfs send {snapshot} (full)"),
    }
    let mut cmd = TokioCommand::new("zfs");
    cmd.arg("send");
    if let Some(b) = base {
        cmd.args(["-i", b]);
    }
    cmd.arg(snapshot);
    cmd.args(opts);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = cmd.spawn().map_err(ClientError::Spawn)?;
    Ok(child.stdout.expect("stdout piped"))
}

/// Spawn `zfs send -t <token>` and return the async stdout pipe.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned.
///
/// # Panics
/// Never panics — stdout is always present because `Stdio::piped()` is set unconditionally.
pub fn send_resume(token: &str, opts: &[String]) -> Result<TokioChildStdout, ClientError> {
    log::trace!("zfs send -t {}… (resume)", &token[..token.len().min(16)]);
    let mut cmd = TokioCommand::new("zfs");
    cmd.args(["send", "-t", token]);
    cmd.args(opts);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = cmd.spawn().map_err(ClientError::Spawn)?;
    Ok(child.stdout.expect("stdout piped"))
}

/// Handle for an in-progress `zfs receive` process.
pub struct ZfsReceive {
    pub stdin: TokioChildStdin,
    child: TokioChild,
}

impl ZfsReceive {
    /// Close stdin (signals EOF to `zfs receive`), wait for the process, and
    /// return its exit status as a `Result`.
    ///
    /// # Errors
    /// Returns [`ClientError`] if `wait` fails or if `zfs receive` exits non-zero.
    pub async fn finish(self) -> Result<(), ClientError> {
        let Self { stdin, child } = self;
        drop(stdin);
        let output = child.wait_with_output().await.map_err(ClientError::Spawn)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(ClientError::CommandFailed {
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            })
        }
    }
}

/// Spawn `zfs receive -s <dataset>` and return a [`ZfsReceive`] handle.
///
/// Call [`ZfsReceive::finish`] after writing all data to `stdin` to wait for
/// the process to exit and check its result.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned.
///
/// # Panics
/// Never panics — stdin is always present because `Stdio::piped()` is set unconditionally.
pub fn receive(dataset: &str, opts: &[String]) -> Result<ZfsReceive, ClientError> {
    log::trace!("zfs receive -s {dataset}");
    let mut cmd = TokioCommand::new("zfs");
    cmd.args(["receive", "-s", dataset]);
    cmd.args(opts);
    cmd.stdin(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(ClientError::Spawn)?;
    let stdin = child.stdin.take().expect("stdin piped");
    Ok(ZfsReceive { stdin, child })
}

/// Run `zfs receive -A <dataset>` to abort a partial receive.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn abort_resume(dataset: &str) -> Result<(), ClientError> {
    log::trace!("zfs receive -A {dataset}");
    let mut cmd = Command::new("zfs");
    cmd.args(["receive", "-A", dataset]);
    run(cmd).map(|_| ())
}

/// Return the ZFS resume token for `dataset`, or `None` if no token is pending.
///
/// ZFS reports `-` or `none` when no resume is in progress.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn get_resume_token(dataset: &str) -> Result<Option<String>, ClientError> {
    log::trace!("zfs get receive_resume_token {dataset}");
    let mut cmd = Command::new("zfs");
    cmd.args(["get", "-H", "-o", "value", "receive_resume_token", dataset]);
    match run(cmd) {
        Ok(output) => Ok(parse_resume_token(output.trim())),
        Err(e) if is_dataset_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

fn parse_resume_token(value: &str) -> Option<String> {
    if value == "-" || value == "none" {
        None
    } else {
        Some(value.to_owned())
    }
}

/// Return all local ZFS datasets that have at least one `zrb-`-prefixed snapshot.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs` process cannot be spawned or exits non-zero.
pub fn discover_datasets() -> Result<Vec<String>, ClientError> {
    log::trace!("zfs list (discovering zrb-managed datasets)");
    let mut cmd = Command::new("zfs");
    cmd.args(["list", "-t", "snapshot", "-H", "-o", "name"]);
    let output = run(cmd)?;
    Ok(parse_discovered_datasets(&output))
}

/// Return the `zrb:resume-since` user property for `dataset`, or `None` if unset.
///
/// This timestamp records when a pending resume token was first observed by
/// `prune`, so subsequent prune runs can enforce the hold-day expiry.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn get_resume_since(dataset: &str) -> Result<Option<DateTime<Utc>>, ClientError> {
    log::trace!("zfs get zrb:resume-since {dataset}");
    let mut cmd = Command::new("zfs");
    cmd.args(["get", "-H", "-o", "value", "zrb:resume-since", dataset]);
    match run(cmd) {
        Ok(output) => Ok(parse_resume_since(output.trim())),
        Err(e) if is_dataset_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

fn parse_resume_since(value: &str) -> Option<DateTime<Utc>> {
    if value == "-" {
        None
    } else {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|dt| dt.to_utc())
    }
}

/// Record `ts` as the first time a resume token was observed on `dataset`.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn set_resume_since(dataset: &str, ts: DateTime<Utc>) -> Result<(), ClientError> {
    log::trace!("zfs set zrb:resume-since={ts} {dataset}");
    let value = ts.to_rfc3339();
    let mut cmd = Command::new("zfs");
    cmd.args(["set", &format!("zrb:resume-since={value}"), dataset]);
    run(cmd).map(|_| ())
}

/// Place a hold tagged `tag` on `snapshot`.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn hold_snapshot(snapshot: &str, tag: &str) -> Result<(), ClientError> {
    log::trace!("zfs hold {tag} {snapshot}");
    let mut cmd = Command::new("zfs");
    cmd.args(["hold", tag, snapshot]);
    run(cmd).map(|_| ())
}

/// Release the hold tagged `tag` from `snapshot`.
///
/// Idempotent: returns `Ok(())` if the hold does not exist.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero
/// for a reason other than the hold already being absent.
pub fn release_hold(snapshot: &str, tag: &str) -> Result<(), ClientError> {
    log::trace!("zfs release {tag} {snapshot}");
    let mut cmd = Command::new("zfs");
    cmd.args(["release", tag, snapshot]);
    match run(cmd) {
        Ok(_) => Ok(()),
        Err(ClientError::CommandFailed { ref stderr }) if stderr.contains("no such tag") => Ok(()),
        Err(e) => Err(e),
    }
}

/// Return the list of hold tags currently on `snapshot`.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn snapshot_holds(snapshot: &str) -> Result<Vec<String>, ClientError> {
    log::trace!("zfs holds -H {snapshot}");
    let mut cmd = Command::new("zfs");
    cmd.args(["holds", "-H", snapshot]);
    let output = run(cmd)?;
    Ok(parse_holds_output(&output))
}

fn parse_holds_output(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            fields.next(); // snapshot name
            fields.next().map(str::to_owned) // hold tag
        })
        .collect()
}

/// Scan all zrb-managed snapshots for `dataset` and return the full snapshot
/// name carrying `tag`, or `None` if none does.
///
/// # Errors
/// Returns [`ClientError`] if any ZFS process cannot be spawned or exits non-zero.
pub fn find_held_snapshot(dataset: &str, tag: &str) -> Result<Option<String>, ClientError> {
    find_held_snapshots(dataset, tag).map(|v| v.into_iter().next())
}

/// Like [`find_held_snapshot`] but takes an already-fetched snapshot list,
/// avoiding a redundant `zfs list` call when the caller already has the list.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs holds` process cannot be spawned or exits non-zero.
pub fn find_held_snapshot_in(
    snapshots: &[String],
    tag: &str,
) -> Result<Option<String>, ClientError> {
    find_held_snapshots_in(snapshots, tag).map(|v| v.into_iter().next())
}

/// Scan all zrb-managed snapshots for `dataset` and return **all** snapshot
/// names carrying `tag`. Returns an empty vec if none do. Multiple entries
/// indicate accumulated holds from interrupted runs.
///
/// # Errors
/// Returns [`ClientError`] if any ZFS process cannot be spawned or exits non-zero.
pub fn find_held_snapshots(dataset: &str, tag: &str) -> Result<Vec<String>, ClientError> {
    let snapshots = list_snapshots(dataset)?;
    find_held_snapshots_in(&snapshots, tag)
}

/// Like [`find_held_snapshots`] but takes an already-fetched snapshot list.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs holds` process cannot be spawned or exits non-zero.
pub fn find_held_snapshots_in(
    snapshots: &[String],
    tag: &str,
) -> Result<Vec<String>, ClientError> {
    if snapshots.is_empty() {
        return Ok(vec![]);
    }
    let mut cmd = Command::new("zfs");
    cmd.args(["holds", "-H"]);
    cmd.args(snapshots);
    let output = run(cmd)?;
    Ok(parse_held_snapshots_for_tag(&output, tag))
}

fn parse_held_snapshots_for_tag(output: &str, tag: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let snap_name = fields.next()?;
            let hold_tag = fields.next()?;
            (hold_tag == tag).then(|| snap_name.to_owned())
        })
        .collect()
}

/// Check holds on multiple snapshots in a single `zfs holds -H` invocation.
///
/// Returns a map of snapshot name → hold tags. Snapshots with no holds are absent
/// from the map. If `snapshots` is empty, returns an empty map without spawning
/// any process.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn batch_snapshot_holds(
    snapshots: &[String],
) -> Result<HashMap<String, Vec<String>>, ClientError> {
    if snapshots.is_empty() {
        return Ok(HashMap::new());
    }
    let mut cmd = Command::new("zfs");
    cmd.args(["holds", "-H"]);
    cmd.args(snapshots);
    let output = run(cmd)?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for line in output.lines() {
        let mut fields = line.splitn(3, '\t');
        let Some(snap_name) = fields.next() else {
            continue;
        };
        let Some(hold_tag) = fields.next() else {
            continue;
        };
        map.entry(snap_name.to_owned())
            .or_default()
            .push(hold_tag.to_owned());
    }
    Ok(map)
}

/// Clear the `zrb:resume-since` user property on `dataset`.
///
/// Safe to call when the property is not set (inherits from parent, which is
/// effectively unset for this user property).
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn clear_resume_since(dataset: &str) -> Result<(), ClientError> {
    log::trace!("zfs inherit zrb:resume-since {dataset}");
    let mut cmd = Command::new("zfs");
    cmd.args(["inherit", "zrb:resume-since", dataset]);
    run(cmd).map(|_| ())
}

fn parse_discovered_datasets(output: &str) -> Vec<String> {
    let mut datasets: Vec<String> = output
        .lines()
        .filter_map(|line| {
            let (dataset, snapshot) = line.trim().split_once('@')?;
            if snapshot.starts_with("zrb-") {
                Some(dataset.to_owned())
            } else {
                None
            }
        })
        .collect();
    datasets.dedup();
    datasets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "Guardrail tripped")]
    fn destroy_snapshot_panics_on_bare_dataset_with_zrb_prefix() {
        // A dataset named "zrb-tank/data" would pass the old contains("zrb-") check
        // but must be rejected because it has no '@'.
        let _ = super::destroy_snapshot("zrb-tank/data");
    }

    #[test]
    #[should_panic(expected = "Guardrail tripped")]
    fn destroy_snapshot_panics_on_non_zrb_snapshot() {
        let _ = super::destroy_snapshot("tank/data@manual-backup");
    }

    #[test]
    fn discover_empty_output_gives_empty_vec() {
        assert_eq!(parse_discovered_datasets(""), Vec::<String>::new());
    }

    #[test]
    fn discover_non_zrb_snapshots_excluded() {
        let out = "tank/home@manual-backup\ntank/data@nightly\n";
        assert_eq!(parse_discovered_datasets(out), Vec::<String>::new());
    }

    #[test]
    fn discover_zrb_snapshots_returns_dataset() {
        let out = "tank/home@zrb-2026-01-01T00:00:00Z\n";
        assert_eq!(parse_discovered_datasets(out), vec!["tank/home"]);
    }

    #[test]
    fn discover_multiple_snapshots_same_dataset_deduplicated() {
        let out = "tank/home@zrb-2026-01-01T00:00:00Z\ntank/home@zrb-2026-01-02T00:00:00Z\n";
        assert_eq!(parse_discovered_datasets(out), vec!["tank/home"]);
    }

    #[test]
    fn discover_mix_of_zrb_and_non_zrb_includes_dataset() {
        let out = "tank/home@manual\ntank/home@zrb-2026-01-01T00:00:00Z\n";
        assert_eq!(parse_discovered_datasets(out), vec!["tank/home"]);
    }

    #[test]
    fn list_output_empty_string_gives_empty_vec() {
        assert_eq!(parse_list_output(""), Vec::<String>::new());
    }

    #[test]
    fn list_output_parses_names() {
        let out = "tank/home@zrb-2026-01-01T00:00:00Z\ntank/home@zrb-2026-01-02T00:00:00Z\n";
        let got = parse_list_output(out);
        assert_eq!(
            got,
            [
                "tank/home@zrb-2026-01-01T00:00:00Z",
                "tank/home@zrb-2026-01-02T00:00:00Z"
            ]
        );
    }

    #[test]
    fn parse_resume_since_dash_is_none() {
        assert_eq!(parse_resume_since("-"), None);
    }

    #[test]
    fn parse_resume_since_valid_rfc3339_parses() {
        use chrono::Datelike;
        let ts = parse_resume_since("2026-05-23T12:00:00Z").unwrap();
        assert_eq!(ts.year(), 2026);
        assert_eq!(ts.month(), 5);
        assert_eq!(ts.day(), 23);
    }

    #[test]
    fn parse_resume_since_malformed_is_none() {
        assert_eq!(parse_resume_since("not-a-timestamp"), None);
    }

    #[test]
    fn resume_token_dash_is_none() {
        assert_eq!(parse_resume_token("-"), None);
    }

    #[test]
    fn resume_token_none_string_is_none() {
        assert_eq!(parse_resume_token("none"), None);
    }

    #[test]
    fn resume_token_real_value_is_some() {
        let tok = "1-abcdef0123456789abcdef0123456789";
        assert_eq!(parse_resume_token(tok), Some(tok.to_owned()));
    }

    #[test]
    fn parse_holds_empty_output_gives_empty_vec() {
        assert_eq!(parse_holds_output(""), Vec::<String>::new());
    }

    #[test]
    fn parse_holds_extracts_tag_column() {
        let out = "tank/data@snap1\tzrb:primary\tFri May 29 12:00 2026\n";
        assert_eq!(parse_holds_output(out), vec!["zrb:primary"]);
    }

    #[test]
    fn parse_holds_multiple_snapshots_multiple_tags() {
        let out = "tank/data@snap1\tzrb:primary\tdate\ntank/data@snap2\tzrb:offsite\tdate\n";
        let got = parse_holds_output(out);
        assert_eq!(got, vec!["zrb:primary", "zrb:offsite"]);
    }

    #[test]
    fn parse_holds_multiple_tags_on_same_snapshot() {
        let out = "tank/data@snap1\tzrb:primary\tdate\ntank/data@snap1\tzrb:offsite\tdate\n";
        let got = parse_holds_output(out);
        assert_eq!(got, vec!["zrb:primary", "zrb:offsite"]);
    }

    #[test]
    fn parse_held_snapshots_for_tag_empty_output_gives_empty_vec() {
        assert_eq!(parse_held_snapshots_for_tag("", "zrb:primary"), Vec::<String>::new());
    }

    #[test]
    fn parse_held_snapshots_for_tag_returns_matching_snapshot() {
        let out = "tank/data@snap-A\tzrb:primary\t2026-01-01\n";
        assert_eq!(
            parse_held_snapshots_for_tag(out, "zrb:primary"),
            vec!["tank/data@snap-A"]
        );
    }

    #[test]
    fn parse_held_snapshots_for_tag_returns_all_matching() {
        let out = "tank/data@snap-A\tzrb:primary\t2026-01-01\n\
                   tank/data@snap-B\tzrb:primary\t2026-01-02\n";
        assert_eq!(
            parse_held_snapshots_for_tag(out, "zrb:primary"),
            vec!["tank/data@snap-A", "tank/data@snap-B"]
        );
    }

    #[test]
    fn parse_held_snapshots_for_tag_excludes_other_tags() {
        let out = "tank/data@snap-A\tzrb:primary\t2026-01-01\n\
                   tank/data@snap-B\tzrb:offsite\t2026-01-02\n";
        assert_eq!(
            parse_held_snapshots_for_tag(out, "zrb:primary"),
            vec!["tank/data@snap-A"]
        );
    }

    #[test]
    fn json_parse_error_display_is_meaningful() {
        let err: ClientError = serde_json::from_str::<crate::zfs::schema::ZfsOutput>("not json")
            .unwrap_err()
            .into();
        let msg = err.to_string();
        assert!(
            msg.contains("JSON"),
            "display should mention JSON, got: {msg}"
        );
    }
}
