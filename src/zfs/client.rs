use std::collections::HashMap;
use std::process::{Command, Stdio};

use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::process::{
    Child as TokioChild, ChildStdin as TokioChildStdin, ChildStdout as TokioChildStdout,
    Command as TokioCommand,
};

use crate::zfs::schema::ZfsOutput;

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
    cmd.args(["list", "-t", "snapshot", "--json", dataset]);
    match run(cmd) {
        Ok(stdout) => decode_list_json(&stdout),
        Err(e) if is_dataset_not_found(&e) => Ok(vec![]),
        Err(e) => Err(e),
    }
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
    cmd.args(["get", "--json", "receive_resume_token", dataset]);
    match run(cmd) {
        Ok(stdout) => Ok(map_token_value(decode_property_json(
            &stdout,
            "receive_resume_token",
            dataset,
        )?)),
        Err(e) if is_dataset_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Return all local ZFS datasets that have at least one `zrb-`-prefixed snapshot.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs` process cannot be spawned or exits non-zero.
pub fn discover_datasets() -> Result<Vec<String>, ClientError> {
    log::trace!("zfs list (discovering zrb-managed datasets)");
    let mut cmd = Command::new("zfs");
    cmd.args(["list", "-t", "snapshot", "--json"]);
    let output = run(cmd)?;
    decode_discover_json(&output)
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
    cmd.args(["get", "--json", "zrb:resume-since", dataset]);
    match run(cmd) {
        Ok(stdout) => Ok(map_since_value(decode_property_json(
            &stdout,
            "zrb:resume-since",
            dataset,
        )?)),
        Err(e) if is_dataset_not_found(&e) => Ok(None),
        Err(e) => Err(e),
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

fn decode_list_json(stdout: &str) -> Result<Vec<String>, ClientError> {
    let out: ZfsOutput = serde_json::from_str(stdout)?;
    Ok(out.datasets.into_keys().collect())
}

fn decode_property_json(
    stdout: &str,
    property: &str,
    dataset: &str,
) -> Result<Option<String>, ClientError> {
    let out: ZfsOutput = serde_json::from_str(stdout)?;
    Ok(out
        .datasets
        .get(dataset)
        .and_then(|d| d.properties.get(property))
        .map(|p| p.value.clone()))
}

fn decode_discover_json(stdout: &str) -> Result<Vec<String>, ClientError> {
    let out: ZfsOutput = serde_json::from_str(stdout)?;
    let mut datasets: Vec<String> = out
        .datasets
        .into_keys()
        .filter_map(|key| {
            let (dataset, snap) = key.split_once('@')?;
            snap.starts_with("zrb-").then(|| dataset.to_owned())
        })
        .collect();
    datasets.sort_unstable();
    datasets.dedup();
    Ok(datasets)
}

fn map_token_value(raw: Option<String>) -> Option<String> {
    raw.filter(|v| v != "-" && v != "none")
}

fn map_since_value(raw: Option<String>) -> Option<DateTime<Utc>> {
    raw.filter(|v| v != "-")
        .and_then(|v| DateTime::parse_from_rfc3339(&v).ok())
        .map(|dt| dt.to_utc())
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
    fn decode_list_json_empty_datasets_gives_empty_vec() {
        let json = r#"{"datasets":{}}"#;
        assert_eq!(decode_list_json(json).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn decode_list_json_single_snapshot_returned() {
        let json = r#"{"datasets":{"tank/home@zrb-2026-01-01T00:00:00Z":{"properties":{}}}}"#;
        assert_eq!(
            decode_list_json(json).unwrap(),
            vec!["tank/home@zrb-2026-01-01T00:00:00Z"]
        );
    }

    #[test]
    fn decode_list_json_multiple_snapshots_all_returned() {
        let json = r#"{"datasets":{
            "tank/home@zrb-2026-01-02T00:00:00Z":{"properties":{}},
            "tank/home@zrb-2026-01-01T00:00:00Z":{"properties":{}}
        }}"#;
        let mut got = decode_list_json(json).unwrap();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![
                "tank/home@zrb-2026-01-01T00:00:00Z",
                "tank/home@zrb-2026-01-02T00:00:00Z"
            ]
        );
    }

    #[test]
    fn decode_discover_json_non_zrb_snapshot_excluded() {
        let json =
            r#"{"datasets":{"tank/home@manual-backup":{"properties":{}},"tank/data@nightly":{"properties":{}}}}"#;
        assert_eq!(decode_discover_json(json).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn decode_discover_json_zrb_snapshot_returns_dataset() {
        let json =
            r#"{"datasets":{"tank/home@zrb-2026-01-01T00:00:00Z":{"properties":{}}}}"#;
        assert_eq!(decode_discover_json(json).unwrap(), vec!["tank/home"]);
    }

    #[test]
    fn decode_discover_json_multiple_snapshots_same_dataset_deduplicated() {
        let json = r#"{"datasets":{
            "tank/home@zrb-2026-01-01T00:00:00Z":{"properties":{}},
            "tank/home@zrb-2026-01-02T00:00:00Z":{"properties":{}}
        }}"#;
        assert_eq!(decode_discover_json(json).unwrap(), vec!["tank/home"]);
    }

    #[test]
    fn decode_discover_json_mix_returns_only_zrb_datasets() {
        let json = r#"{"datasets":{
            "tank/home@manual":{"properties":{}},
            "tank/home@zrb-2026-01-01T00:00:00Z":{"properties":{}}
        }}"#;
        assert_eq!(decode_discover_json(json).unwrap(), vec!["tank/home"]);
    }

    #[test]
    fn decode_property_json_present_returns_some() {
        let json = r#"{"datasets":{"tank/data":{"properties":{"receive_resume_token":{"value":"1-abc"}}}}}"#;
        assert_eq!(
            decode_property_json(json, "receive_resume_token", "tank/data").unwrap(),
            Some("1-abc".to_owned())
        );
    }

    #[test]
    fn decode_property_json_empty_datasets_returns_none() {
        let json = r#"{"datasets":{}}"#;
        assert_eq!(
            decode_property_json(json, "receive_resume_token", "tank/data").unwrap(),
            None
        );
    }

    #[test]
    fn decode_property_json_wrong_dataset_returns_none() {
        let json = r#"{"datasets":{"tank/data":{"properties":{"receive_resume_token":{"value":"1-abc"}}}}}"#;
        assert_eq!(
            decode_property_json(json, "receive_resume_token", "tank/other").unwrap(),
            None
        );
    }

    #[test]
    fn get_resume_token_none_string_maps_to_none() {
        let json = r#"{"datasets":{"tank/data":{"properties":{"receive_resume_token":{"value":"none"}}}}}"#;
        let raw = decode_property_json(json, "receive_resume_token", "tank/data").unwrap();
        assert_eq!(map_token_value(raw), None);
    }

    #[test]
    fn get_resume_token_dash_maps_to_none() {
        let json = r#"{"datasets":{"tank/data":{"properties":{"receive_resume_token":{"value":"-"}}}}}"#;
        let raw = decode_property_json(json, "receive_resume_token", "tank/data").unwrap();
        assert_eq!(map_token_value(raw), None);
    }

    #[test]
    fn get_resume_token_real_value_maps_to_some() {
        let tok = "1-abcdef0123456789abcdef0123456789";
        let json = format!(
            r#"{{"datasets":{{"tank/data":{{"properties":{{"receive_resume_token":{{"value":"{tok}"}}}}}}}}}}"#
        );
        let raw = decode_property_json(&json, "receive_resume_token", "tank/data").unwrap();
        assert_eq!(map_token_value(raw), Some(tok.to_owned()));
    }

    #[test]
    fn get_resume_since_dash_maps_to_none() {
        let json = r#"{"datasets":{"tank/data":{"properties":{"zrb:resume-since":{"value":"-"}}}}}"#;
        let raw = decode_property_json(json, "zrb:resume-since", "tank/data").unwrap();
        assert_eq!(map_since_value(raw), None);
    }

    #[test]
    fn get_resume_since_valid_rfc3339_maps_to_datetime() {
        use chrono::Datelike;
        let json = r#"{"datasets":{"tank/data":{"properties":{"zrb:resume-since":{"value":"2026-05-23T12:00:00Z"}}}}}"#;
        let raw = decode_property_json(json, "zrb:resume-since", "tank/data").unwrap();
        let ts = map_since_value(raw).unwrap();
        assert_eq!(ts.year(), 2026);
        assert_eq!(ts.month(), 5);
        assert_eq!(ts.day(), 23);
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
