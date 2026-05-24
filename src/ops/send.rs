use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::process::{ChildStdin, ChildStdout, Command};

use sd_notify::NotifyState;

use anyhow::Context;

use crate::config::{RemoteConfig, SourceConfig};
use crate::ops::{list as ops_list, snapshot as ops_snapshot};
use crate::protocol::codec::{self, ClientHello};
use crate::ssh::transport;
use crate::zfs::{client as zfs, estimator};

/// Back up `datasets` to all configured Remotes (or a named subset).
///
/// A failure for one Remote is logged as a warning but does not abort sends
/// to other Remotes (user story 8). The local Snapshot created at the start
/// of each dataset's send is retained regardless of send outcome (user story 7).
///
/// # Errors
/// Returns `Err` only on pre-send failures (snapshot creation or config lookup).
/// Per-remote send errors are logged, not propagated.
pub fn send(
    datasets: &[&str],
    remote_filter: Option<&[&str]>,
    config: &SourceConfig,
) -> anyhow::Result<()> {
    for &dataset in datasets {
        let latest = ops_snapshot::snapshot(dataset, config)
            .with_context(|| format!("creating snapshot for {dataset}"))?;

        let local_snaps = ops_list::list(dataset)
            .with_context(|| format!("listing local snapshots for {dataset}"))?;

        let dataset_remotes = config
            .datasets
            .get(dataset)
            .ok_or_else(|| anyhow::anyhow!("dataset '{dataset}' not found in config"))?;

        for (remote_name, target) in dataset_remotes {
            if let Some(filter) = remote_filter
                && !filter.contains(&remote_name.as_str())
            {
                continue;
            }
            let remote_cfg = config
                .remotes
                .get(remote_name)
                .ok_or_else(|| anyhow::anyhow!("remote '{remote_name}' not in config"))?;

            if let Err(e) = send_to_remote(
                &latest,
                &local_snaps,
                remote_cfg,
                target,
                config.name(),
            ) {
                log::warn!("send {dataset} -> {remote_name}: {e:#}");
            }
        }
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
fn run_with_progress<F>(
    latest: &str,
    remote_cfg: &RemoteConfig,
    log_verb: &str,
    inner: F,
) -> anyhow::Result<()>
where
    F: FnOnce(
        &mut BufReader<ChildStdout>,
        &mut ChildStdin,
        Option<&mut dyn FnMut(u64, u64)>,
    ) -> anyhow::Result<()>,
{
    let tty = std::io::stderr().is_terminal();
    let start = std::time::Instant::now();
    let dataset = latest.split_once('@').map_or(latest, |(d, _)| d);

    let mut conn = transport::connect(remote_cfg, &[])?;
    let mut reader = BufReader::new(conn.stdout);

    let mut final_bytes = 0u64;
    let result = {
        let fb = &mut final_bytes;
        let mut cb = |bytes: u64, total: u64| {
            *fb = bytes;
            let _ = sd_notify::notify(&[NotifyState::Watchdog]);
            if tty {
                let elapsed_s = start.elapsed().as_secs_f64();
                let speed = if elapsed_s > 0.0 { bytes as f64 / elapsed_s } else { 0.0 };
                let mib = bytes as f64 / (1024.0 * 1024.0);
                let speed_mbs = speed / (1024.0 * 1024.0);
                if total > 0 {
                    let total_mib = total as f64 / (1024.0 * 1024.0);
                    let pct = 100.0 * bytes as f64 / total as f64;
                    let remaining = total.saturating_sub(bytes) as f64;
                    let eta_s = if speed > 0.0 { remaining / speed } else { 0.0 };
                    eprint!(
                        "\rsent {mib:.1} MiB / ~{total_mib:.0} MiB ({pct:.0}%)  \
                         {speed_mbs:.1} MB/s  ETA {eta_s:.0}s  "
                    );
                } else {
                    eprint!("\rsent {mib:.1} MiB  {speed_mbs:.1} MB/s  ");
                }
            }
        };
        let r = inner(&mut reader, &mut conn.stdin, Some(&mut cb));
        if tty {
            eprintln!();
        }
        r
    };

    let _ = conn.child.wait();

    if result.is_ok() {
        let elapsed_s = start.elapsed().as_secs_f64();
        let rate_mbs = if elapsed_s > 0.0 {
            final_bytes as f64 / (1024.0 * 1024.0) / elapsed_s
        } else {
            0.0
        };
        log::info!("{log_verb} {dataset}: {final_bytes} bytes in {elapsed_s:.1}s ({rate_mbs:.2} MB/s)");
    }

    result
}

fn send_to_remote(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
) -> anyhow::Result<()> {
    run_with_progress(latest, remote_cfg, "sent", |reader, writer, progress| {
        send_on(latest, local_snaps, remote_cfg, target, client_name, reader, writer, progress)
    })
}

/// Run the send protocol over arbitrary `Read`/`Write` streams.
///
/// Encodes `ClientHello`, reads `ServerHello`, streams the ZFS send output,
/// then reads `ServerStatus` and returns `Ok` or an error.
///
/// `progress`, if `Some`, is called after each protocol chunk with
/// `(bytes_so_far, total_bytes)`. `total_bytes = 0` means unknown (resume path).
///
/// # Errors
/// Returns `Err` on I/O, codec, or remote protocol failure.
#[allow(clippy::too_many_arguments)]
pub fn send_on<R: BufRead, W: Write>(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    reader: &mut R,
    writer: &mut W,
    progress: Option<&mut dyn FnMut(u64, u64)>,
) -> anyhow::Result<()> {
    codec::encode_client_hello(
        &ClientHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            client_name: client_name.to_owned(),
            target: target.to_owned(),
        },
        writer,
    )
    .context("writing ClientHello")?;

    let version_status = codec::decode_server_status(reader).context("reading version ServerStatus")?;
    if !version_status.ok {
        anyhow::bail!("server rejected connection: {}", version_status.message);
    }

    let hello = codec::decode_server_hello(reader).context("reading ServerHello")?;

    let (mut zfs_out, total_bytes) = if let Some(ref token) = hello.resume_token {
        let out = zfs::send_resume(token, &remote_cfg.zfs_send_opts).context("zfs send -t")?;
        (out, 0u64)
    } else {
        // Server snapshots use the destination dataset prefix; local snapshots use
        // the source dataset prefix.  Compare only the @name suffix (timestamp) so
        // that snapshots which exist on both sides are recognised as common.
        let server_names: std::collections::HashSet<&str> = hello
            .snapshots
            .iter()
            .filter_map(|s| s.split_once('@').map(|(_, n)| n))
            .collect();
        let common: Vec<String> = local_snaps
            .iter()
            .filter(|s| s.split_once('@').is_some_and(|(_, n)| server_names.contains(n)))
            .cloned()
            .collect();
        let best = best_base(&common, |cand| estimate_size(cand, latest, &remote_cfg.zfs_send_opts))
            .context("selecting incremental base")?;
        let (base_snap, estimate) = best.map_or((None, 0u64), |(s, e)| (Some(s), e));
        let out = zfs::send_incremental(base_snap.as_deref(), latest, &remote_cfg.zfs_send_opts)
            .context("zfs send")?;
        (out, estimate)
    };

    codec::write_stream(&mut zfs_out, writer, remote_cfg.bandwidth_limit, total_bytes, progress)
        .context("transferring stream")?;

    let status = codec::decode_server_status(reader).context("reading ServerStatus")?;

    if status.ok {
        Ok(())
    } else {
        Err(remote_receive_error(&status.message))
    }
}

/// Back up `datasets` to all configured Remotes without creating a new snapshot.
///
/// Uses the newest existing local snapshot for each dataset. If a Resume Token is
/// pending on the Remote it is consumed; otherwise the newest snapshot is sent
/// incrementally. Errors if the newest snapshot is already present on the Remote
/// (use `zrb send` instead to create a fresh snapshot).
///
/// # Errors
/// Returns `Err` if there are no local snapshots for a dataset, or on pre-send
/// config failures. Per-remote errors are logged as warnings.
pub fn send_resume(
    datasets: &[&str],
    remote_filter: Option<&[&str]>,
    config: &SourceConfig,
) -> anyhow::Result<()> {
    for &dataset in datasets {
        let local_snaps = ops_list::list(dataset)
            .with_context(|| format!("listing local snapshots for {dataset}"))?;

        let newest = local_snaps
            .last()
            .ok_or_else(|| {
                anyhow::anyhow!("no local snapshots for '{dataset}'; run `zrb send` first")
            })?
            .clone();

        let dataset_remotes = config
            .datasets
            .get(dataset)
            .ok_or_else(|| anyhow::anyhow!("dataset '{dataset}' not found in config"))?;

        for (remote_name, target) in dataset_remotes {
            if let Some(filter) = remote_filter
                && !filter.contains(&remote_name.as_str())
            {
                continue;
            }
            let remote_cfg = config
                .remotes
                .get(remote_name)
                .ok_or_else(|| anyhow::anyhow!("remote '{remote_name}' not in config"))?;

            if let Err(e) =
                resume_to_remote(&newest, &local_snaps, remote_cfg, target, config.name())
            {
                log::warn!("resume {dataset} -> {remote_name}: {e:#}");
            }
        }
    }
    Ok(())
}

fn resume_to_remote(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
) -> anyhow::Result<()> {
    run_with_progress(latest, remote_cfg, "resumed", |reader, writer, progress| {
        resume_on(latest, local_snaps, remote_cfg, target, client_name, reader, writer, progress)
    })
}

/// Run the resume protocol over arbitrary `Read`/`Write` streams.
///
/// Behaves like [`send_on`] but skips snapshot creation. Decision tree after
/// `ServerHello`:
/// - Resume Token present → `zfs send -t <token>`
/// - No token, newest snapshot not on server → incremental send to `latest`
/// - No token, newest snapshot already on server → `Err`
///
/// # Errors
/// Returns `Err` on I/O, codec, remote protocol failure, or if the newest
/// snapshot is already present on the Remote.
#[allow(clippy::too_many_arguments)]
pub fn resume_on<R: BufRead, W: Write>(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    reader: &mut R,
    writer: &mut W,
    progress: Option<&mut dyn FnMut(u64, u64)>,
) -> anyhow::Result<()> {
    codec::encode_client_hello(
        &ClientHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            client_name: client_name.to_owned(),
            target: target.to_owned(),
        },
        writer,
    )
    .context("writing ClientHello")?;

    let version_status = codec::decode_server_status(reader).context("reading version ServerStatus")?;
    if !version_status.ok {
        anyhow::bail!("server rejected connection: {}", version_status.message);
    }

    let hello = codec::decode_server_hello(reader).context("reading ServerHello")?;

    let (mut zfs_out, total_bytes) = if let Some(ref token) = hello.resume_token {
        let out = zfs::send_resume(token, &remote_cfg.zfs_send_opts).context("zfs send -t")?;
        (out, 0u64)
    } else {
        let latest_suffix = latest.split_once('@').map_or(latest, |(_, n)| n);
        let on_server = hello
            .snapshots
            .iter()
            .any(|s| s.split_once('@').is_some_and(|(_, n)| n == latest_suffix));
        if on_server {
            return Err(anyhow::anyhow!(
                "newest snapshot already on server; run `zrb send` to create a fresh backup"
            ));
        }
        let server_names: std::collections::HashSet<&str> = hello
            .snapshots
            .iter()
            .filter_map(|s| s.split_once('@').map(|(_, n)| n))
            .collect();
        let common: Vec<String> = local_snaps
            .iter()
            .filter(|s| s.split_once('@').is_some_and(|(_, n)| server_names.contains(n)))
            .cloned()
            .collect();
        let best =
            best_base(&common, |cand| estimate_size(cand, latest, &remote_cfg.zfs_send_opts))
                .context("selecting incremental base")?;
        let (base_snap, estimate) = best.map_or((None, 0u64), |(s, e)| (Some(s), e));
        let out = zfs::send_incremental(base_snap.as_deref(), latest, &remote_cfg.zfs_send_opts)
            .context("zfs send")?;
        (out, estimate)
    };

    codec::write_stream(&mut zfs_out, writer, remote_cfg.bandwidth_limit, total_bytes, progress)
        .context("transferring stream")?;

    let status = codec::decode_server_status(reader).context("reading ServerStatus")?;

    if status.ok {
        Ok(())
    } else {
        Err(remote_receive_error(&status.message))
    }
}

fn remote_receive_error(msg: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "remote error: {msg}\nhint: the target dataset may need `zfs rollback` or the \
         snapshots may conflict with the send stream; check `zfs allow` delegation on the server"
    )
}

fn estimate_size(candidate: &str, latest: &str, opts: &[String]) -> anyhow::Result<u64> {
    let mut cmd = Command::new("zfs");
    cmd.args(["send", "-n", "-v", "-i", candidate, latest]);
    cmd.args(opts);
    let output = cmd.output().context("running zfs send -n -v")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    estimator::parse_estimated_size(&stdout).map_err(|e| anyhow::anyhow!("{e}"))
}

fn best_base<F>(common: &[String], estimate: F) -> anyhow::Result<Option<(String, u64)>>
where
    F: Fn(&str) -> anyhow::Result<u64>,
{
    if common.is_empty() {
        return Ok(None);
    }
    let mut best: Option<(String, u64)> = None;
    for snap in common {
        let size = estimate(snap)?;
        if best.as_ref().is_none_or(|(_, s)| size < *s) {
            best = Some((snap.clone(), size));
        }
    }
    Ok(best)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::config::RemoteConfig;
    use crate::protocol::codec::{self, ServerHello, ServerStatus};

    fn test_remote_cfg() -> RemoteConfig {
        RemoteConfig {
            host: "backup.example.com".to_owned(),
            port: Some(22),
            user: Some("zfsbackup".to_owned()),
            ssh_key: None,
            ssh_opts: vec![],
            zfs_send_opts: vec![],
            bandwidth_limit: None,
        }
    }

    fn version_ok_then_hello(hello: &ServerHello) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::encode_server_status(
            &crate::protocol::codec::ServerStatus { ok: true, message: "ok".to_owned() },
            &mut buf,
        )
        .unwrap();
        codec::encode_server_hello(hello, &mut buf).unwrap();
        buf
    }

    #[test]
    fn resume_on_errors_when_newest_snapshot_already_on_server() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let hello = ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots: vec!["backup/home@zrb-2026-01-20T00:00:00Z".to_owned()],
            resume_token: None,
        };
        let reader_bytes = version_ok_then_hello(&hello);

        let result = resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut Cursor::new(reader_bytes),
            &mut std::io::sink(),
            None,
        );

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("already on server"), "unexpected message: {msg}");
    }

    #[test]
    fn resume_on_errors_when_newest_is_among_multiple_server_snapshots() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![
            "tank/home@zrb-2026-01-10T00:00:00Z".to_owned(),
            latest.to_owned(),
        ];
        let hello = ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots: vec![
                "backup/home@zrb-2026-01-10T00:00:00Z".to_owned(),
                "backup/home@zrb-2026-01-20T00:00:00Z".to_owned(),
            ],
            resume_token: None,
        };
        let result = resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut Cursor::new(version_ok_then_hello(&hello)),
            &mut std::io::sink(),
            None,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already on server"));
    }

    fn version_rejection_bytes(message: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::encode_server_status(
            &ServerStatus { ok: false, message: message.to_owned() },
            &mut buf,
        )
        .unwrap();
        buf
    }

    use super::*;

    #[test]
    fn send_on_errors_on_version_rejection() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let server_bytes = version_rejection_bytes("version mismatch: client 0.1.0, server 0.2.0");

        let result = send_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut Cursor::new(server_bytes),
            &mut std::io::sink(),
            None,
        );

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("version mismatch"), "unexpected: {msg}");
    }

    #[test]
    fn resume_on_errors_on_version_rejection() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let server_bytes = version_rejection_bytes("version mismatch: client 0.1.0, server 0.2.0");

        let result = resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut Cursor::new(server_bytes),
            &mut std::io::sink(),
            None,
        );

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("version mismatch"), "unexpected: {msg}");
    }

    #[test]
    fn remote_receive_error_includes_original_message_and_hint() {
        let err = remote_receive_error("cannot receive: destination has snapshots (pool/data)");
        let msg = err.to_string();
        assert!(msg.contains("cannot receive:"), "missing original in: {msg}");
        assert!(msg.contains("hint:"), "missing hint in: {msg}");
        assert!(msg.contains("zfs rollback"), "missing rollback hint in: {msg}");
    }

    #[test]
    fn no_common_snapshots_is_full_send() {
        let result = best_base(&[], |_| Ok(0u64)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn single_common_snapshot_is_selected() {
        let common = vec!["tank/home@zrb-2026-01-01T00:00:00Z".to_owned()];
        let (snap, size) = best_base(&common, |_| Ok(1000u64)).unwrap().unwrap();
        assert_eq!(snap, "tank/home@zrb-2026-01-01T00:00:00Z");
        assert_eq!(size, 1000);
    }

    #[test]
    fn smallest_estimate_wins() {
        let snaps = [
            "tank/home@zrb-2026-01-01T00:00:00Z",
            "tank/home@zrb-2026-01-10T00:00:00Z",
            "tank/home@zrb-2026-01-20T00:00:00Z",
        ];
        let common: Vec<String> = snaps.iter().map(|s| (*s).to_owned()).collect();
        let (snap, size) = best_base(&common, |candidate| {
            if candidate.contains("01-10") {
                Ok(200_u64)
            } else if candidate.contains("01-01") {
                Ok(1_500_u64)
            } else {
                Ok(900_u64)
            }
        })
        .unwrap()
        .unwrap();
        assert_eq!(snap, "tank/home@zrb-2026-01-10T00:00:00Z");
        assert_eq!(size, 200);
    }

    #[test]
    fn best_base_returns_estimate_alongside_snapshot() {
        let common = vec!["tank/home@zrb-2026-01-01T00:00:00Z".to_owned()];
        let (_, size) = best_base(&common, |_| Ok(42_000u64)).unwrap().unwrap();
        assert_eq!(size, 42_000);
    }
}
