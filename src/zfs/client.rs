use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use chrono::{DateTime, Utc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to spawn zfs process: {0}")]
    Spawn(std::io::Error),
    #[error("zfs command failed: {stderr}")]
    CommandFailed { stderr: String },
}

fn is_dataset_not_found(err: &ClientError) -> bool {
    matches!(err, ClientError::CommandFailed { stderr } if stderr.contains("dataset does not exist"))
}

fn run(mut cmd: Command) -> Result<String, ClientError> {
    let output = cmd
        .output()
        .map_err(ClientError::Spawn)?;
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
    let mut cmd = Command::new("zfs");
    cmd.arg("snapshot").arg(format!("{dataset}@{snapshot_name}"));
    run(cmd).map(|_| ())
}

/// List all snapshots for `dataset`, returning raw `<dataset>@<name>` strings.
///
/// # Errors
/// Returns [`ClientError`] if the `zfs` process cannot be spawned or exits non-zero.
pub fn list_snapshots(dataset: &str) -> Result<Vec<String>, ClientError> {
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
        snapshot.split_once('@').is_some_and(|(_, name)| name.starts_with("zrb-")),
        "Guardrail tripped: not a zrb snapshot: {snapshot}"
    );
    let mut cmd = Command::new("zfs");
    cmd.arg("destroy").arg(snapshot);
    run(cmd).map(|_| ())
}

/// Spawn `zfs send [-i <base>] <snapshot>` and return the stdout pipe.
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
) -> Result<ChildStdout, ClientError> {
    let mut cmd = Command::new("zfs");
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

/// Spawn `zfs send -t <token>` and return the stdout pipe.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned.
///
/// # Panics
/// Never panics — stdout is always present because `Stdio::piped()` is set unconditionally.
pub fn send_resume(token: &str, opts: &[String]) -> Result<ChildStdout, ClientError> {
    let mut cmd = Command::new("zfs");
    cmd.args(["send", "-t", token]);
    cmd.args(opts);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = cmd.spawn().map_err(ClientError::Spawn)?;
    Ok(child.stdout.expect("stdout piped"))
}

/// Handle for an in-progress `zfs receive` process.
pub struct ZfsReceive {
    pub stdin: ChildStdin,
    child: Child,
}

impl ZfsReceive {
    /// Close stdin (signals EOF to `zfs receive`), wait for the process, and
    /// return its exit status as a `Result`.
    ///
    /// # Errors
    /// Returns [`ClientError`] if `wait` fails or if `zfs receive` exits non-zero.
    pub fn finish(self) -> Result<(), ClientError> {
        let Self { stdin, child } = self;
        drop(stdin);
        let output = child.wait_with_output().map_err(ClientError::Spawn)?;
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
    let mut cmd = Command::new("zfs");
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
        DateTime::parse_from_rfc3339(value).ok().map(|dt| dt.to_utc())
    }
}

/// Record `ts` as the first time a resume token was observed on `dataset`.
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn set_resume_since(dataset: &str, ts: DateTime<Utc>) -> Result<(), ClientError> {
    let value = ts.to_rfc3339();
    let mut cmd = Command::new("zfs");
    cmd.args(["set", &format!("zrb:resume-since={value}"), dataset]);
    run(cmd).map(|_| ())
}

/// Clear the `zrb:resume-since` user property on `dataset`.
///
/// Safe to call when the property is not set (inherits from parent, which is
/// effectively unset for this user property).
///
/// # Errors
/// Returns [`ClientError`] if the process cannot be spawned or exits non-zero.
pub fn clear_resume_since(dataset: &str) -> Result<(), ClientError> {
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
        assert_eq!(got, ["tank/home@zrb-2026-01-01T00:00:00Z", "tank/home@zrb-2026-01-02T00:00:00Z"]);
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
}
