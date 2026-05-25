use std::collections::HashMap;
use std::future::Future;

use anyhow::Context;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sd_notify::NotifyState;
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::task::JoinSet;

use crate::config::{RemoteConfig, RemoteTargets, SourceConfig};
use crate::ops::{list as ops_list, snapshot as ops_snapshot};
use crate::protocol::codec::{self, ClientHello, ClientReady};
use crate::ssh::transport;
use crate::zfs::{client as zfs, estimator};

/// Back up `datasets` to all configured Remotes (or a named subset).
///
/// A failure for one Remote is logged as a warning but does not abort sends
/// to other Remotes. The local Snapshot created at the start of each dataset's
/// send is retained regardless of send outcome.
///
/// # Errors
/// Returns `Err` only on pre-send failures (snapshot creation or config lookup).
/// Per-remote send errors are logged, not propagated.
pub async fn send(
    datasets: &[&str],
    remote_filter: Option<&[&str]>,
    config: &SourceConfig,
    sequential: bool,
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

        let tasks = collect_tasks(dataset_remotes, remote_filter, &config.remotes)?;

        dispatch_tasks(
            &latest,
            &local_snaps,
            config.name(),
            dataset,
            tasks,
            sequential,
            false,
        )
        .await;
    }
    Ok(())
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
pub async fn send_resume(
    datasets: &[&str],
    remote_filter: Option<&[&str]>,
    config: &SourceConfig,
    sequential: bool,
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

        let tasks = collect_tasks(dataset_remotes, remote_filter, &config.remotes)?;

        dispatch_tasks(
            &newest,
            &local_snaps,
            config.name(),
            dataset,
            tasks,
            sequential,
            true,
        )
        .await;
    }
    Ok(())
}

/// Filter and clone remote config into owned `(remote_name, RemoteConfig, target)` triples.
///
/// Owned data is needed so each spawned task can take ownership without borrowing
/// from the caller's stack frame.
fn collect_tasks(
    dataset_remotes: &RemoteTargets,
    remote_filter: Option<&[&str]>,
    all_remotes: &HashMap<String, RemoteConfig>,
) -> anyhow::Result<Vec<(String, RemoteConfig, String)>> {
    let mut tasks = Vec::new();
    for (remote_name, target) in dataset_remotes {
        if let Some(filter) = remote_filter
            && !filter.contains(&remote_name.as_str())
        {
            continue;
        }
        let remote_cfg = all_remotes
            .get(remote_name)
            .ok_or_else(|| anyhow::anyhow!("remote '{remote_name}' not in config"))?
            .clone();
        tasks.push((remote_name.clone(), remote_cfg, target.clone()));
    }
    Ok(tasks)
}

async fn dispatch_tasks(
    latest: &str,
    local_snaps: &[String],
    client_name: &str,
    dataset: &str,
    tasks: Vec<(String, RemoteConfig, String)>,
    sequential: bool,
    is_resume: bool,
) {
    let mp = MultiProgress::new();
    let max_name_len = tasks.iter().map(|(n, _, _)| n.len()).max().unwrap_or(0);
    let bars: Vec<ProgressBar> = tasks
        .iter()
        .map(|(name, _, _)| {
            let bar = mp.add(ProgressBar::new(0));
            bar.set_style(style_waiting());
            bar.set_prefix(format!("{name:<max_name_len$}"));
            bar
        })
        .collect();

    if sequential {
        for ((remote_name, remote_cfg, target), bar) in tasks.iter().zip(bars) {
            let result = if is_resume {
                resume_to_remote(latest, local_snaps, remote_cfg, target, client_name, bar).await
            } else {
                send_to_remote(latest, local_snaps, remote_cfg, target, client_name, bar).await
            };
            if let Err(e) = result {
                let verb = if is_resume { "resume" } else { "send" };
                log::warn!("{verb} {dataset} -> {remote_name}: {e:#}");
            }
        }
    } else {
        let mut set: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();
        for ((remote_name, remote_cfg, target), bar) in tasks.into_iter().zip(bars) {
            let (latest, local_snaps, cname) = (
                latest.to_owned(),
                local_snaps.to_vec(),
                client_name.to_owned(),
            );
            set.spawn(async move {
                let result = if is_resume {
                    resume_to_remote(&latest, &local_snaps, &remote_cfg, &target, &cname, bar).await
                } else {
                    send_to_remote(&latest, &local_snaps, &remote_cfg, &target, &cname, bar).await
                };
                (remote_name, result)
            });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((rname, Err(e))) = joined {
                let verb = if is_resume { "resume" } else { "send" };
                log::warn!("{verb} {dataset} -> {rname}: {e:#}");
            }
        }
    }
}

fn style_waiting() -> ProgressStyle {
    ProgressStyle::with_template("  {prefix:.dim}  waiting")
        .expect("static template is valid")
}

fn style_active_bounded() -> ProgressStyle {
    ProgressStyle::with_template(
        "  {prefix:.cyan.bold}  [{bar:40.cyan/white}] \
         {decimal_bytes}/{decimal_total_bytes}  {decimal_bytes_per_sec}  ETA {eta}",
    )
    .expect("static template is valid")
    .progress_chars("=>-")
}

fn style_active_unbounded() -> ProgressStyle {
    ProgressStyle::with_template(
        "  {prefix:.cyan.bold}  {spinner}  {decimal_bytes}  {decimal_bytes_per_sec}",
    )
    .expect("static template is valid")
}

fn style_done() -> ProgressStyle {
    ProgressStyle::with_template("  {prefix:.green.bold}  {decimal_bytes} {msg}")
        .expect("static template is valid")
}

fn style_failed() -> ProgressStyle {
    ProgressStyle::with_template("  {prefix:.red.bold}  failed")
        .expect("static template is valid")
}

#[allow(clippy::cast_precision_loss)]
async fn send_to_remote(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    bar: ProgressBar,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let dataset = latest.split_once('@').map_or(latest, |(d, _)| d);

    let mut conn = transport::connect(remote_cfg, &[])?;
    let mut reader = tokio::io::BufReader::new(conn.stdout);

    let mut final_bytes = 0u64;
    let mut style_set = false;
    let result = {
        let fb = &mut final_bytes;
        let ss = &mut style_set;
        let bar_cb = bar.clone();
        let cb: &mut (dyn FnMut(u64, u64) + Send) = &mut move |bytes: u64, total: u64| {
            *fb = bytes;
            let _ = sd_notify::notify(&[NotifyState::Watchdog]);
            if !*ss {
                if total > 0 {
                    bar_cb.set_length(total);
                    bar_cb.set_style(style_active_bounded());
                } else {
                    bar_cb.set_style(style_active_unbounded());
                }
                *ss = true;
            }
            bar_cb.set_position(bytes);
        };
        send_on(
            latest,
            local_snaps,
            remote_cfg,
            target,
            client_name,
            &mut reader,
            &mut conn.stdin,
            Some(cb),
        )
        .await
    };

    drop(conn.stdin);
    let _ = conn.child.wait().await;

    let elapsed_s = start.elapsed().as_secs_f64();
    let rate_mbs = if elapsed_s > 0.0 {
        final_bytes as f64 / 1_000_000.0 / elapsed_s
    } else {
        0.0
    };
    if result.is_ok() {
        bar.set_style(style_done());
        bar.set_position(final_bytes);
        bar.finish_with_message(format!("({elapsed_s:.1}s)"));
        log::info!("sent {dataset}: {final_bytes} bytes in {elapsed_s:.1}s ({rate_mbs:.2} MB/s)");
    } else {
        bar.set_style(style_failed());
        bar.abandon();
    }

    result
}

#[allow(clippy::cast_precision_loss)]
async fn resume_to_remote(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    bar: ProgressBar,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let dataset = latest.split_once('@').map_or(latest, |(d, _)| d);

    let mut conn = transport::connect(remote_cfg, &[])?;
    let mut reader = tokio::io::BufReader::new(conn.stdout);

    let mut final_bytes = 0u64;
    let mut style_set = false;
    let result = {
        let fb = &mut final_bytes;
        let ss = &mut style_set;
        let bar_cb = bar.clone();
        let cb: &mut (dyn FnMut(u64, u64) + Send) = &mut move |bytes: u64, total: u64| {
            *fb = bytes;
            let _ = sd_notify::notify(&[NotifyState::Watchdog]);
            if !*ss {
                if total > 0 {
                    bar_cb.set_length(total);
                    bar_cb.set_style(style_active_bounded());
                } else {
                    bar_cb.set_style(style_active_unbounded());
                }
                *ss = true;
            }
            bar_cb.set_position(bytes);
        };
        resume_on(
            latest,
            local_snaps,
            remote_cfg,
            target,
            client_name,
            &mut reader,
            &mut conn.stdin,
            Some(cb),
        )
        .await
    };

    drop(conn.stdin);
    let _ = conn.child.wait().await;

    let elapsed_s = start.elapsed().as_secs_f64();
    let rate_mbs = if elapsed_s > 0.0 {
        final_bytes as f64 / 1_000_000.0 / elapsed_s
    } else {
        0.0
    };
    if result.is_ok() {
        bar.set_style(style_done());
        bar.set_position(final_bytes);
        bar.finish_with_message(format!("({elapsed_s:.1}s)"));
        log::info!(
            "resumed {dataset}: {final_bytes} bytes in {elapsed_s:.1}s ({rate_mbs:.2} MB/s)"
        );
    } else {
        bar.set_style(style_failed());
        bar.abandon();
    }

    result
}

/// Run the send protocol over arbitrary async `Read`/`Write` streams.
///
/// Encodes `ClientHello`, reads `ServerHello`, streams the ZFS send output,
/// then reads `ServerStatus` and returns `Ok` or an error.
///
/// `progress`, if `Some`, is called after each protocol chunk with
/// `(bytes_so_far, total_bytes)`. `total_bytes = 0` means unknown (resume path).
///
/// # Errors
/// Returns `Err` on I/O, codec, or remote protocol failure.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn send_on<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    reader: &mut R,
    writer: &mut W,
    progress: Option<&mut (dyn FnMut(u64, u64) + Send)>,
) -> anyhow::Result<()> {
    codec::encode_client_hello(
        &ClientHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            client_name: client_name.to_owned(),
            target: target.to_owned(),
        },
        writer,
    )
    .await
    .context("writing ClientHello")?;

    let version_status = codec::decode_server_status(reader)
        .await
        .context("reading version ServerStatus")?;
    if !version_status.ok {
        anyhow::bail!("server rejected connection: {}", version_status.message);
    }

    let hello = codec::decode_server_hello(reader)
        .await
        .context("reading ServerHello")?;

    log::debug!("server has {} snapshot(s)", hello.snapshots.len());

    // Server snapshots use the destination dataset prefix; local snapshots use
    // the source dataset prefix.  Compare only the @name suffix (timestamp) so
    // that snapshots which exist on both sides are recognised as common.
    let stream_res = if let Some(ref token) = hello.resume_token {
        log::debug!("resume token present; using zfs send -t");
        let size = estimate_resume_size(token, &remote_cfg.zfs_send_opts).await;
        zfs::send_resume(token, &remote_cfg.zfs_send_opts)
            .context("zfs send -t")
            .map(|out| (out, size))
    } else {
        let server_names: std::collections::HashSet<&str> = hello
            .snapshots
            .iter()
            .filter_map(|s| s.split_once('@').map(|(_, n)| n))
            .collect();
        let common: Vec<String> = local_snaps
            .iter()
            .filter(|s| {
                s.split_once('@')
                    .is_some_and(|(_, n)| server_names.contains(n))
            })
            .cloned()
            .collect();
        log::debug!(
            "{} local, {} server, {} common snapshot(s)",
            local_snaps.len(),
            hello.snapshots.len(),
            common.len()
        );
        best_base(&common, |cand| {
            let c = cand.to_owned();
            let opts = remote_cfg.zfs_send_opts.clone();
            let l = latest.to_owned();
            async move { estimate_size(&c, &l, &opts).await }
        })
        .await
        .context("selecting incremental base")
        .and_then(|best| {
            let (base_snap, estimate) = best.map_or((None, 0u64), |(s, e)| (Some(s), e));
            match &base_snap {
                Some(s) => log::debug!("incremental base: {s} (~{estimate} bytes estimated)"),
                None => log::debug!("no common snapshots; sending full stream"),
            }
            zfs::send_incremental(base_snap.as_deref(), latest, &remote_cfg.zfs_send_opts)
                .context("zfs send")
                .map(|out| (out, estimate))
        })
    };

    let (mut zfs_out, total_bytes) = match stream_res {
        Ok(pair) => {
            codec::encode_client_ready(
                &ClientReady {
                    ok: true,
                    message: "ok".to_owned(),
                },
                writer,
            )
            .await
            .context("writing ClientReady")?;
            pair
        }
        Err(e) => {
            let _ = codec::encode_client_ready(
                &ClientReady {
                    ok: false,
                    message: e.to_string(),
                },
                writer,
            )
            .await;
            return Err(e);
        }
    };

    codec::write_stream(
        &mut zfs_out,
        writer,
        remote_cfg.bandwidth_limit,
        total_bytes,
        progress,
    )
    .await
    .context("transferring stream")?;

    let status = codec::decode_server_status(reader)
        .await
        .context("reading ServerStatus")?;

    if status.ok {
        Ok(())
    } else {
        Err(remote_receive_error(&status.message))
    }
}

/// Run the resume protocol over arbitrary async `Read`/`Write` streams.
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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn resume_on<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    reader: &mut R,
    writer: &mut W,
    progress: Option<&mut (dyn FnMut(u64, u64) + Send)>,
) -> anyhow::Result<()> {
    codec::encode_client_hello(
        &ClientHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            client_name: client_name.to_owned(),
            target: target.to_owned(),
        },
        writer,
    )
    .await
    .context("writing ClientHello")?;

    let version_status = codec::decode_server_status(reader)
        .await
        .context("reading version ServerStatus")?;
    if !version_status.ok {
        anyhow::bail!("server rejected connection: {}", version_status.message);
    }

    let hello = codec::decode_server_hello(reader)
        .await
        .context("reading ServerHello")?;

    log::debug!("server has {} snapshot(s)", hello.snapshots.len());

    let stream_res = if let Some(ref token) = hello.resume_token {
        log::debug!("resume token present; using zfs send -t");
        let size = estimate_resume_size(token, &remote_cfg.zfs_send_opts).await;
        zfs::send_resume(token, &remote_cfg.zfs_send_opts)
            .context("zfs send -t")
            .map(|out| (out, size))
    } else {
        let latest_suffix = latest.split_once('@').map_or(latest, |(_, n)| n);
        let on_server = hello
            .snapshots
            .iter()
            .any(|s| s.split_once('@').is_some_and(|(_, n)| n == latest_suffix));
        if on_server {
            Err(anyhow::anyhow!(
                "newest snapshot already on server; run `zrb send` to create a fresh backup"
            ))
        } else {
            let server_names: std::collections::HashSet<&str> = hello
                .snapshots
                .iter()
                .filter_map(|s| s.split_once('@').map(|(_, n)| n))
                .collect();
            let common: Vec<String> = local_snaps
                .iter()
                .filter(|s| {
                    s.split_once('@')
                        .is_some_and(|(_, n)| server_names.contains(n))
                })
                .cloned()
                .collect();
            log::debug!(
                "{} local, {} server, {} common snapshot(s)",
                local_snaps.len(),
                hello.snapshots.len(),
                common.len()
            );
            best_base(&common, |cand| {
                let c = cand.to_owned();
                let opts = remote_cfg.zfs_send_opts.clone();
                let l = latest.to_owned();
                async move { estimate_size(&c, &l, &opts).await }
            })
            .await
            .context("selecting incremental base")
            .and_then(|best| {
                let (base_snap, estimate) = best.map_or((None, 0u64), |(s, e)| (Some(s), e));
                match &base_snap {
                    Some(s) => log::debug!("incremental base: {s} (~{estimate} bytes estimated)"),
                    None => log::debug!("no common snapshots; sending full stream"),
                }
                zfs::send_incremental(base_snap.as_deref(), latest, &remote_cfg.zfs_send_opts)
                    .context("zfs send")
                    .map(|out| (out, estimate))
            })
        }
    };

    let (mut zfs_out, total_bytes) = match stream_res {
        Ok(pair) => {
            codec::encode_client_ready(
                &ClientReady {
                    ok: true,
                    message: "ok".to_owned(),
                },
                writer,
            )
            .await
            .context("writing ClientReady")?;
            pair
        }
        Err(e) => {
            let _ = codec::encode_client_ready(
                &ClientReady {
                    ok: false,
                    message: e.to_string(),
                },
                writer,
            )
            .await;
            return Err(e);
        }
    };

    codec::write_stream(
        &mut zfs_out,
        writer,
        remote_cfg.bandwidth_limit,
        total_bytes,
        progress,
    )
    .await
    .context("transferring stream")?;

    let status = codec::decode_server_status(reader)
        .await
        .context("reading ServerStatus")?;

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

fn estimate_from_output(output: &str) -> u64 {
    estimator::parse_estimated_size(output).unwrap_or(0)
}

async fn estimate_resume_size(token: &str, opts: &[String]) -> u64 {
    let Ok(output) = tokio::process::Command::new("zfs")
        .args(["send", "-n", "-v", "-t", token])
        .args(opts)
        .output()
        .await
    else {
        return 0;
    };
    // ZFS writes verbose stats to stderr; combine with stdout for compatibility
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout),
    );
    estimate_from_output(&combined)
}

async fn estimate_size(candidate: &str, latest: &str, opts: &[String]) -> anyhow::Result<u64> {
    let output = tokio::process::Command::new("zfs")
        .args(["send", "-n", "-v", "-i", candidate, latest])
        .args(opts)
        .output()
        .await
        .context("running zfs send -n -v")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let size = estimator::parse_estimated_size(&stdout).map_err(|e| anyhow::anyhow!("{e}"))?;
    log::trace!("size estimate {candidate} → {latest}: {size} bytes");
    Ok(size)
}

async fn best_base<F, Fut>(common: &[String], estimate: F) -> anyhow::Result<Option<(String, u64)>>
where
    F: Fn(&str) -> Fut,
    Fut: Future<Output = anyhow::Result<u64>>,
{
    if common.is_empty() {
        return Ok(None);
    }
    let mut best: Option<(String, u64)> = None;
    for snap in common {
        let size = estimate(snap).await?;
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

    async fn version_ok_then_hello(hello: &ServerHello) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::encode_server_status(
            &crate::protocol::codec::ServerStatus {
                ok: true,
                message: "ok".to_owned(),
            },
            &mut buf,
        )
        .await
        .unwrap();
        codec::encode_server_hello(hello, &mut buf).await.unwrap();
        buf
    }

    #[tokio::test]
    async fn resume_on_errors_when_newest_snapshot_already_on_server() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let hello = ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots: vec!["backup/home@zrb-2026-01-20T00:00:00Z".to_owned()],
            resume_token: None,
        };
        let reader_bytes = version_ok_then_hello(&hello).await;

        let result = super::resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(reader_bytes)),
            &mut tokio::io::sink(),
            None,
        )
        .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("already on server"),
            "unexpected message: {msg}"
        );
    }

    #[tokio::test]
    async fn resume_on_errors_when_newest_is_among_multiple_server_snapshots() {
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
        let result = super::resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(version_ok_then_hello(&hello).await)),
            &mut tokio::io::sink(),
            None,
        )
        .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already on server")
        );
    }

    #[tokio::test]
    async fn resume_on_writes_client_not_ready_when_newest_snapshot_already_on_server() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let hello = ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots: vec!["backup/home@zrb-2026-01-20T00:00:00Z".to_owned()],
            resume_token: None,
        };
        let reader_bytes = version_ok_then_hello(&hello).await;
        let mut writer: Vec<u8> = Vec::new();

        let _ = super::resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(reader_bytes)),
            &mut writer,
            None,
        )
        .await;

        // writer has ClientHello then ClientReady — skip past ClientHello
        let mut r = tokio::io::BufReader::new(Cursor::new(&writer));
        let _: codec::ClientHello = codec::decode_client_hello(&mut r).await.unwrap();
        let ready = codec::decode_client_ready(&mut r).await.unwrap();
        assert!(!ready.ok, "expected ClientReady.ok=false, got ok=true");
        assert!(
            ready.message.contains("already on server"),
            "unexpected ClientReady message: {}",
            ready.message
        );
    }

    async fn version_rejection_bytes(message: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::encode_server_status(
            &ServerStatus {
                ok: false,
                message: message.to_owned(),
            },
            &mut buf,
        )
        .await
        .unwrap();
        buf
    }

    use super::*;

    #[tokio::test]
    async fn send_on_errors_on_version_rejection() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let server_bytes =
            version_rejection_bytes("version mismatch: client 0.1.0, server 0.2.0").await;

        let result = send_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(server_bytes)),
            &mut tokio::io::sink(),
            None,
        )
        .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("version mismatch"), "unexpected: {msg}");
    }

    #[tokio::test]
    async fn resume_on_errors_on_version_rejection() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let server_bytes =
            version_rejection_bytes("version mismatch: client 0.1.0, server 0.2.0").await;

        let result = resume_on(
            latest,
            &local_snaps,
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(server_bytes)),
            &mut tokio::io::sink(),
            None,
        )
        .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("version mismatch"), "unexpected: {msg}");
    }

    #[test]
    fn remote_receive_error_includes_original_message_and_hint() {
        let err = remote_receive_error("cannot receive: destination has snapshots (pool/data)");
        let msg = err.to_string();
        assert!(
            msg.contains("cannot receive:"),
            "missing original in: {msg}"
        );
        assert!(msg.contains("hint:"), "missing hint in: {msg}");
        assert!(
            msg.contains("zfs rollback"),
            "missing rollback hint in: {msg}"
        );
    }

    #[tokio::test]
    async fn no_common_snapshots_is_full_send() {
        let result = best_base::<_, _>(&[], |_: &str| async { Ok(0u64) })
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn single_common_snapshot_is_selected() {
        let common = vec!["tank/home@zrb-2026-01-01T00:00:00Z".to_owned()];
        let (snap, size) = best_base(&common, |_: &str| async { Ok(1000u64) })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snap, "tank/home@zrb-2026-01-01T00:00:00Z");
        assert_eq!(size, 1000);
    }

    #[tokio::test]
    async fn smallest_estimate_wins() {
        let snaps = [
            "tank/home@zrb-2026-01-01T00:00:00Z",
            "tank/home@zrb-2026-01-10T00:00:00Z",
            "tank/home@zrb-2026-01-20T00:00:00Z",
        ];
        let common: Vec<String> = snaps.iter().map(|s| (*s).to_owned()).collect();
        let (snap, size) = best_base(&common, |candidate: &str| {
            let c = candidate.to_owned();
            async move {
                if c.contains("01-10") {
                    Ok(200_u64)
                } else if c.contains("01-01") {
                    Ok(1_500_u64)
                } else {
                    Ok(900_u64)
                }
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(snap, "tank/home@zrb-2026-01-10T00:00:00Z");
        assert_eq!(size, 200);
    }

    #[tokio::test]
    async fn best_base_returns_estimate_alongside_snapshot() {
        let common = vec!["tank/home@zrb-2026-01-01T00:00:00Z".to_owned()];
        let (_, size) = best_base(&common, |_: &str| async { Ok(42_000u64) })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(size, 42_000);
    }

    #[test]
    fn collect_tasks_filters_by_remote_name() {
        let mut dataset_remotes = RemoteTargets::new();
        dataset_remotes.insert("primary".to_owned(), "backup/home".to_owned());
        dataset_remotes.insert("secondary".to_owned(), "offsite/home".to_owned());

        let mut all_remotes = HashMap::new();
        all_remotes.insert("primary".to_owned(), test_remote_cfg());
        all_remotes.insert("secondary".to_owned(), test_remote_cfg());

        let tasks = collect_tasks(&dataset_remotes, Some(&["primary"]), &all_remotes).unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0, "primary");
        assert_eq!(tasks[0].2, "backup/home");
    }

    #[test]
    fn collect_tasks_no_filter_returns_all() {
        let mut dataset_remotes = RemoteTargets::new();
        dataset_remotes.insert("primary".to_owned(), "backup/home".to_owned());
        dataset_remotes.insert("secondary".to_owned(), "offsite/home".to_owned());

        let mut all_remotes = HashMap::new();
        all_remotes.insert("primary".to_owned(), test_remote_cfg());
        all_remotes.insert("secondary".to_owned(), test_remote_cfg());

        let tasks = collect_tasks(&dataset_remotes, None, &all_remotes).unwrap();
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn collect_tasks_errors_on_unknown_remote() {
        let mut dataset_remotes = RemoteTargets::new();
        dataset_remotes.insert("ghost".to_owned(), "backup/home".to_owned());
        let all_remotes = HashMap::new();

        let err = collect_tasks(&dataset_remotes, None, &all_remotes).unwrap_err();
        assert!(
            err.to_string().contains("ghost"),
            "expected remote name in error: {err}"
        );
    }

    #[test]
    fn estimate_from_output_parses_gib() {
        let out = "send from @zrb-2026-05-01T00:00:00Z to tank/home@zrb-2026-05-22T14:30:00Z \
                   estimated size is 1.23G\n";
        assert_eq!(estimate_from_output(out), 1_320_702_443u64);
    }

    #[test]
    fn estimate_from_output_falls_back_to_zero_on_missing_line() {
        assert_eq!(estimate_from_output("no relevant output\n"), 0);
    }

    #[tokio::test]
    async fn resume_on_resume_token_errors_gracefully() {
        let hello = ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots: vec![],
            resume_token: Some("1-fake-resume-token".to_owned()),
        };
        let reader_bytes = version_ok_then_hello(&hello).await;

        let result = super::resume_on(
            "tank/home@zrb-2026-01-20T00:00:00Z",
            &[],
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(reader_bytes)),
            &mut tokio::io::sink(),
            None,
        )
        .await;

        // With a fake token, zfs::send_resume will either fail to spawn
        // (ZFS unavailable) or produce an error stream (invalid token).
        // Either way resume_on must return Err, never panic.
        assert!(
            result.is_err(),
            "expected Err with fake resume token, got Ok"
        );
    }

    #[tokio::test]
    async fn send_on_resume_token_errors_gracefully() {
        let hello = ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots: vec![],
            resume_token: Some("1-fake-resume-token".to_owned()),
        };
        let reader_bytes = version_ok_then_hello(&hello).await;

        let result = send_on(
            "tank/home@zrb-2026-01-20T00:00:00Z",
            &[],
            &test_remote_cfg(),
            "backup/home",
            "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(reader_bytes)),
            &mut tokio::io::sink(),
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "expected Err with fake resume token, got Ok"
        );
    }
}
