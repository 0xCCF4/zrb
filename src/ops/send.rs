use std::collections::HashMap;
use std::sync::Arc;

type CancelMap = HashMap<String, tokio_util::sync::CancellationToken>;

use anyhow::Context;
use sd_notify::NotifyState;
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::task::JoinSet;

use crate::config::{RemoteConfig, RemoteTargets, SourceConfig};
use crate::ops::list as ops_list;
use crate::progress::SendProgress;
use crate::protocol::codec::{self, ClientReady, CodecError, ServerHello, ServerStatus};
use crate::protocol::handshake;
use crate::ssh::transport;
use crate::zfs::{client as zfs, estimator};

/// The transfer action to take, determined from the Protocol handshake alone.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransferDecision {
    /// Latest local snapshot already present on the Remote — nothing to transfer.
    AlreadyUpToDate,
    /// No snapshots on the Remote; send the full snapshot stream.
    FullSend,
    /// Resume an interrupted transfer using the stored ZFS resume token.
    ResumeSend { token: String },
    /// Send an incremental stream from `base` to `latest`.
    IncrementalSend { base: String },
}

/// Decide what transfer action to take, purely from handshake information.
///
/// This function makes no I/O calls. Size estimation and ZFS subprocess spawning
/// happen after the decision is made.
///
/// # Errors
/// Returns `Err` when the Remote's head snapshot is absent from `local_snaps`
/// (history divergence); the error message names the missing snapshot.
pub(crate) fn decide_transfer(
    hello: &ServerHello,
    local_snaps: &[String],
    latest: &str,
) -> anyhow::Result<TransferDecision> {
    let latest_suffix = latest.split_once('@').map_or(latest, |(_, n)| n);
    let already_on_server = hello
        .head
        .as_deref()
        .and_then(|h| h.split_once('@'))
        .is_some_and(|(_, n)| n == latest_suffix);

    if already_on_server {
        return Ok(TransferDecision::AlreadyUpToDate);
    }

    if let Some(token) = hello.resume_token.clone() {
        return Ok(TransferDecision::ResumeSend { token });
    }

    let Some(remote_head) = hello.head.as_deref() else {
        return Ok(TransferDecision::FullSend);
    };

    let head_name = remote_head.split_once('@').map_or("", |(_, n)| n);
    match local_snaps
        .iter()
        .find(|s| s.split_once('@').is_some_and(|(_, n)| n == head_name))
    {
        Some(base) => Ok(TransferDecision::IncrementalSend { base: base.clone() }),
        None => Err(anyhow::anyhow!(
            "histories have diverged: the Remote's most recent snapshot `{head_name}` \
             does not exist locally.\n\
             \n\
             This usually means the snapshot was deleted on this machine while not \
             yet deleted on the Remote, or the Remote was restored from a different \
             backup.\n\
             \n\
             To recover: delete any snapshots on the Remote that are newer than the \
             most recent snapshot shared with this machine, then retry."
        )),
    }
}

/// Send the most recent local Snapshot for `dataset` to all configured Remotes.
///
/// Uses the newest existing local snapshot; fails clearly if no local snapshots exist.
/// A failure for one Remote is logged as a warning but does not abort sends to other Remotes.
///
/// # Errors
/// Returns `Err` on pre-send failures (no local snapshots, config lookup) or if
/// any remote transfer fails.
async fn send_one_dataset(
    dataset: String,
    config: SourceConfig,
    remote_filter: Option<Vec<String>>,
    sequential: bool,
    progress: Option<Arc<dyn SendProgress>>,
    cancel_map: Option<CancelMap>,
) -> (String, anyhow::Result<()>) {
    let result: anyhow::Result<()> = async {
        let local_snaps = ops_list::list(&dataset)
            .with_context(|| format!("listing local snapshots for {dataset}"))?;
        let latest = local_snaps
            .last()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no local snapshots for '{dataset}'; run `zrb snapshot` first"
                )
            })?
            .clone();
        let dataset_remotes = config
            .datasets
            .get(&dataset)
            .ok_or_else(|| anyhow::anyhow!("dataset '{dataset}' not found in config"))?;
        let filter_refs: Option<Vec<&str>> =
            remote_filter.as_deref().map(|v| v.iter().map(String::as_str).collect());
        let tasks = collect_tasks(dataset_remotes, filter_refs.as_deref(), &config.remotes)?;
        dispatch_tasks(
            &latest, &local_snaps, config.name(), tasks,
            sequential, progress, cancel_map.as_ref(),
        )
        .await?;
        Ok(())
    }
    .await;
    (dataset, result)
}

/// Send the most recent local Snapshot for each dataset in `datasets` to all configured Remotes.
///
/// Datasets are sent in parallel unless `sequential` is true. Per-dataset
/// failures are logged as warnings; all other datasets continue. Returns `Err`
/// after all datasets have been attempted if any failed.
///
/// # Errors
/// Returns `Err` if any dataset fails (no local snapshots, config lookup, or transfer).
pub async fn send(
    datasets: &[&str],
    remote_filter: Option<&[&str]>,
    config: &SourceConfig,
    sequential: bool,
    progress: Option<Arc<dyn SendProgress>>,
    cancel_map: Option<CancelMap>,
) -> anyhow::Result<()> {
    let remote_filter_owned: Option<Vec<String>> =
        remote_filter.map(|f| f.iter().map(|&s| s.to_owned()).collect());

    let mut failed = false;
    if sequential {
        for &ds in datasets {
            let (dataset, result) = send_one_dataset(
                ds.to_owned(), config.clone(), remote_filter_owned.clone(),
                sequential, progress.clone(), cancel_map.clone(),
            ).await;
            if let Err(e) = result {
                log::warn!("send {dataset}: {e:#}");
                failed = true;
            }
        }
    } else {
        let mut set: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();
        for &ds in datasets {
            set.spawn(send_one_dataset(
                ds.to_owned(), config.clone(), remote_filter_owned.clone(),
                sequential, progress.clone(), cancel_map.clone(),
            ));
        }
        while let Some(joined) = set.join_next().await {
            let (dataset, result) = joined?;
            if let Err(e) = result {
                log::warn!("send {dataset}: {e:#}");
                failed = true;
            }
        }
    }

    if let Some(ref p) = progress {
        p.all_done();
    }
    if failed {
        anyhow::bail!("one or more datasets failed to send");
    }
    Ok(())
}

/// All per-remote transfer parameters bundled for a single send operation.
struct SendContext {
    latest: String,
    local_snaps: Vec<String>,
    remote_name: String,
    remote_cfg: RemoteConfig,
    target: String,
    client_name: String,
    progress: Option<Arc<dyn SendProgress>>,
    cancel: Option<tokio_util::sync::CancellationToken>,
}

impl SendContext {
    fn row_key_for(latest: &str, remote_name: &str) -> String {
        let dataset = latest.split_once('@').map_or(latest, |(d, _)| d);
        format!("{dataset} \u{2192} {remote_name}")
    }

    fn row_key(&self) -> String {
        Self::row_key_for(&self.latest, &self.remote_name)
    }

    fn dataset(&self) -> &str {
        self.latest.split_once('@').map_or(&self.latest, |(d, _)| d)
    }
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
    tasks: Vec<(String, RemoteConfig, String)>,
    sequential: bool,
    progress: Option<Arc<dyn SendProgress>>,
    cancel_map: Option<&CancelMap>,
) -> anyhow::Result<()> {
    let mut any_failed = false;
    if sequential {
        for (remote_name, remote_cfg, target) in tasks {
            let row_key = SendContext::row_key_for(latest, &remote_name);
            let cancel = cancel_map.and_then(|m| m.get(&row_key)).cloned();
            let ctx = SendContext {
                latest: latest.to_owned(),
                local_snaps: local_snaps.to_vec(),
                client_name: client_name.to_owned(),
                remote_name,
                remote_cfg,
                target,
                progress: progress.clone(),
                cancel,
            };
            if let Err(e) = send_to_remote(ctx).await && !is_cancelled(&e) {
                log::warn!("send {row_key}: {e:#}");
                any_failed = true;
            }
        }
    } else {
        let mut set: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();
        for (remote_name, remote_cfg, target) in tasks {
            let row_key = SendContext::row_key_for(latest, &remote_name);
            let cancel = cancel_map.and_then(|m| m.get(&row_key)).cloned();
            let ctx = SendContext {
                latest: latest.to_owned(),
                local_snaps: local_snaps.to_vec(),
                client_name: client_name.to_owned(),
                remote_name,
                remote_cfg,
                target,
                progress: progress.clone(),
                cancel,
            };
            set.spawn(async move { (row_key, send_to_remote(ctx).await) });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((row_key, Err(e))) = joined && !is_cancelled(&e) {
                log::warn!("send {row_key}: {e:#}");
                any_failed = true;
            }
        }
    }
    if any_failed {
        anyhow::bail!("one or more remotes failed");
    }
    Ok(())
}

#[allow(clippy::cast_precision_loss)]
async fn send_to_remote(ctx: SendContext) -> anyhow::Result<()> {
    let row_key = ctx.row_key();
    let start = std::time::Instant::now();
    let dataset = ctx.dataset().to_owned();

    let mut conn = transport::connect(&ctx.remote_cfg, &[])?;
    let mut reader = tokio::io::BufReader::new(conn.stdout);

    let result = send_on(
        &ctx.latest, &ctx.local_snaps, &ctx.remote_cfg, &ctx.target, &ctx.client_name,
        &mut reader, &mut conn.stdin, &row_key, ctx.progress.clone(), ctx.cancel.as_ref(),
    )
    .await;

    drop(conn.stdin);
    let _ = conn.child.wait().await;

    let elapsed_s = start.elapsed().as_secs_f64();
    match result {
        Ok(0) => {
            if let Some(ref p) = ctx.progress {
                p.remote_up_to_date(&row_key);
            }
            Ok(())
        }
        Ok(bytes) => {
            place_transfer_hold(&dataset, &ctx.latest, &ctx.remote_name);
            let rate_mbs = if elapsed_s > 0.0 {
                bytes as f64 / 1_000_000.0 / elapsed_s
            } else {
                0.0
            };
            log::info!("sent {dataset}: {bytes} bytes in {elapsed_s:.1}s ({rate_mbs:.2} MB/s)");
            if let Some(ref p) = ctx.progress {
                p.remote_completed(&row_key, bytes, elapsed_s);
            }
            Ok(())
        }
        Err(e) => {
            if is_cancelled(&e) {
                log::debug!("send {dataset} -> {}: cancelled by user", ctx.remote_name);
                if let Some(ref p) = ctx.progress {
                    p.remote_skipped(&row_key);
                }
            } else if let Some(ref p) = ctx.progress {
                p.remote_failed(&row_key, &e.to_string());
            }
            Err(e)
        }
    }
}

fn is_cancelled(e: &anyhow::Error) -> bool {
    e.downcast_ref::<CodecError>()
        .is_some_and(|c| matches!(c, CodecError::Cancelled))
}

/// Run the send protocol over arbitrary async `Read`/`Write` streams.
///
/// Encodes `ClientHello`, reads `ServerHello`, streams the ZFS send output,
/// then reads `ServerStatus` and returns `Ok(bytes_sent)` or an error.
/// Returns `Ok(0)` when the latest snapshot is already present on the Remote.
///
/// `progress.remote_started` is called right before the stream starts (carrying
/// the estimated total bytes). `progress.remote_progress` is called after each
/// protocol chunk.
///
/// # Errors
/// Returns `Err` on I/O, codec, or remote protocol failure.
#[allow(clippy::too_many_arguments, clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn send_on<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    latest: &str,
    local_snaps: &[String],
    remote_cfg: &RemoteConfig,
    target: &str,
    client_name: &str,
    reader: &mut R,
    writer: &mut W,
    row_key: &str,
    progress: Option<Arc<dyn SendProgress>>,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> anyhow::Result<u64> {
    let handshake::ClientHandshakeResult { hello } =
        handshake::client_handshake(reader, writer, target, client_name).await?;

    log::debug!("server head: {:?}", hello.head);

    let decision = decide_transfer(&hello, local_snaps, latest)?;

    let stream_res: anyhow::Result<_> = match decision {
        TransferDecision::AlreadyUpToDate => {
            log::info!("{}: already up to date", latest.split_once('@').map_or(latest, |(d, _)| d));
            let _ = codec::encode_json(
                &ClientReady { ok: false, message: "already up to date".to_owned() },
                writer,
            )
            .await;
            return Ok(0);
        }
        TransferDecision::ResumeSend { ref token } => {
            log::debug!("resume token present; using zfs send -t");
            let size = estimate_resume_size(token, &remote_cfg.zfs_send_opts).await;
            zfs::send_resume(token, &remote_cfg.zfs_send_opts)
                .context("zfs send -t")
                .map(|out| (out, size))
        }
        TransferDecision::FullSend => {
            log::debug!("no server snapshots; sending full stream");
            let estimate = estimate_size(None, latest, &remote_cfg.zfs_send_opts)
                .await
                .unwrap_or(0);
            zfs::send_incremental(None, latest, &remote_cfg.zfs_send_opts)
                .context("zfs send")
                .map(|out| (out, estimate))
        }
        TransferDecision::IncrementalSend { ref base } => {
            log::debug!("incremental base: {base}");
            let estimate = estimate_size(Some(base.as_str()), latest, &remote_cfg.zfs_send_opts)
                .await
                .unwrap_or(0);
            zfs::send_incremental(Some(base.as_str()), latest, &remote_cfg.zfs_send_opts)
                .context("zfs send")
                .map(|out| (out, estimate))
        }
    };

    let (mut zfs_out, total_bytes) = match stream_res {
        Ok(pair) => {
            codec::encode_json(
                &ClientReady { ok: true, message: "ok".to_owned() },
                writer,
            )
            .await
            .context("writing ClientReady")?;
            pair
        }
        Err(e) => {
            let _ = codec::encode_json(
                &ClientReady { ok: false, message: e.to_string() },
                writer,
            )
            .await;
            return Err(e);
        }
    };

    if let Some(ref p) = progress {
        p.remote_started(row_key, total_bytes);
    }

    let mut bytes_sent: u64 = 0;
    {
        let bs = &mut bytes_sent;
        let rk = row_key.to_owned();
        let eprog = progress;
        let cb: &mut (dyn FnMut(u64, u64) + Send) = &mut move |b: u64, _total: u64| {
            *bs = b;
            let _ = sd_notify::notify(&[NotifyState::Watchdog]);
            if let Some(ref p) = eprog {
                p.remote_progress(&rk, b);
            }
        };
        if let Err(e) = codec::write_stream(
            &mut zfs_out,
            writer,
            remote_cfg.bandwidth_limit,
            total_bytes,
            Some(cb),
            cancel,
        )
        .await
        {
            if !matches!(e, codec::CodecError::Cancelled)
                && let Ok(status) = codec::decode_json::<ServerStatus, _>(reader).await
                && !status.ok
            {
                return Err(remote_receive_error(&status.message));
            }
            return Err(e).context("transferring stream");
        }
    }

    let status: ServerStatus = codec::decode_json(reader)
        .await
        .context("reading ServerStatus")?;

    if status.ok {
        Ok(bytes_sent)
    } else {
        Err(remote_receive_error(&status.message))
    }
}

/// Place or move the Transfer Hold for `remote_name` onto `snapshot`.
///
/// Ordering: the new hold is placed before the previous hold is released,
/// so there is never a window with no hold on this dataset for `remote_name`.
/// Hold/release failures are logged as warnings; the function never panics or
/// propagates errors.
pub fn place_transfer_hold(dataset: &str, snapshot: &str, remote_name: &str) {
    let tag = format!("zrb:{remote_name}");
    let old_snaps: Vec<String> = match zfs::find_held_snapshots(dataset, &tag) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("Transfer Hold: failed to find existing holds for {dataset} ({remote_name}): {e}");
            vec![]
        }
    }
    .into_iter()
    .filter(|s| s != snapshot)
    .collect();
    if let Err(e) = zfs::hold_snapshot(snapshot, &tag) {
        log::warn!("Transfer Hold: failed to hold {snapshot} for {remote_name}: {e}");
        return;
    }
    for old_snap in &old_snaps {
        if let Err(e) = zfs::release_hold(old_snap, &tag) {
            log::warn!("Transfer Hold: failed to release old hold on {old_snap} for {remote_name}: {e}");
        }
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
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout),
    );
    estimate_from_output(&combined)
}

async fn estimate_size(base: Option<&str>, latest: &str, opts: &[String]) -> anyhow::Result<u64> {
    let mut cmd = tokio::process::Command::new("zfs");
    cmd.args(["send", "-n", "-v"]);
    if let Some(b) = base {
        cmd.args(["-i", b]);
    }
    cmd.arg(latest).args(opts);
    let output = cmd.output().await.context("running zfs send -n -v")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}\n{stderr}");
    let size = estimator::parse_estimated_size(&combined).map_err(|e| anyhow::anyhow!("{e}"))?;
    log::trace!("size estimate {base:?} → {latest}: {size} bytes");
    Ok(size)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Mutex;

    use crate::config::RemoteConfig;
    use crate::protocol::codec::{self, ClientHello, ClientReady, ServerHello, ServerStatus};

    use super::*;

    // ── Test helpers ──────────────────────────────────────────────────────

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
        codec::encode_json(&ServerStatus { ok: true, message: "ok".to_owned() }, &mut buf)
            .await.unwrap();
        codec::encode_json(hello, &mut buf).await.unwrap();
        buf
    }

    async fn version_rejection_bytes(message: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::encode_json(
            &ServerStatus { ok: false, message: message.to_owned() },
            &mut buf,
        )
        .await.unwrap();
        buf
    }

    #[derive(Default)]
    struct SpySendProgress(Mutex<Vec<String>>);

    impl SpySendProgress {
        fn calls(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl SendProgress for SpySendProgress {
        fn remote_started(&self, remote: &str, total_bytes: u64) {
            self.0.lock().unwrap().push(format!("started:{remote}:{total_bytes}"));
        }
        fn remote_progress(&self, remote: &str, bytes_sent: u64) {
            self.0.lock().unwrap().push(format!("progress:{remote}:{bytes_sent}"));
        }
        fn remote_completed(&self, remote: &str, bytes: u64, _elapsed: f64) {
            self.0.lock().unwrap().push(format!("completed:{remote}:{bytes}"));
        }
        fn remote_skipped(&self, remote: &str) {
            self.0.lock().unwrap().push(format!("skipped:{remote}"));
        }
        fn remote_up_to_date(&self, remote: &str) {
            self.0.lock().unwrap().push(format!("up_to_date:{remote}"));
        }
        fn remote_failed(&self, remote: &str, error: &str) {
            self.0.lock().unwrap().push(format!("failed:{remote}:{error}"));
        }
        fn all_done(&self) {
            self.0.lock().unwrap().push("all_done".to_owned());
        }
    }

    // ── decide_transfer ───────────────────────────────────────────────────

    fn hello(head: Option<&str>, resume_token: Option<&str>) -> ServerHello {
        ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            head: head.map(str::to_owned),
            resume_token: resume_token.map(str::to_owned),
        }
    }

    #[test]
    fn decide_transfer_already_up_to_date_wins_over_resume_token() {
        // Stale resume token must not cause a re-transfer when the latest snapshot
        // is already on the remote — AlreadyUpToDate takes priority.
        let h = hello(Some("backup/data@zrb-2026-01-10T00:00:00Z"), Some("tok"));
        let local = vec!["tank/data@zrb-2026-01-10T00:00:00Z".to_owned()];
        let result = decide_transfer(&h, &local, "tank/data@zrb-2026-01-10T00:00:00Z").unwrap();
        assert_eq!(result, TransferDecision::AlreadyUpToDate);
    }

    #[test]
    fn decide_transfer_resume_token_used_when_latest_not_yet_on_remote() {
        // Resume token is honoured when the latest snapshot is not yet fully received.
        let h = hello(Some("backup/data@zrb-2026-01-01T00:00:00Z"), Some("tok"));
        let local = vec![
            "tank/data@zrb-2026-01-01T00:00:00Z".to_owned(),
            "tank/data@zrb-2026-01-10T00:00:00Z".to_owned(),
        ];
        let result = decide_transfer(&h, &local, "tank/data@zrb-2026-01-10T00:00:00Z").unwrap();
        assert_eq!(result, TransferDecision::ResumeSend { token: "tok".to_owned() });
    }

    #[test]
    fn decide_transfer_already_up_to_date_when_head_matches_latest() {
        let h = hello(Some("backup/data@zrb-2026-01-10T00:00:00Z"), None);
        let local = vec!["tank/data@zrb-2026-01-10T00:00:00Z".to_owned()];
        let result = decide_transfer(&h, &local, "tank/data@zrb-2026-01-10T00:00:00Z").unwrap();
        assert_eq!(result, TransferDecision::AlreadyUpToDate);
    }

    #[test]
    fn decide_transfer_full_send_when_no_server_head() {
        let h = hello(None, None);
        let local = vec!["tank/data@zrb-2026-01-01T00:00:00Z".to_owned()];
        let result = decide_transfer(&h, &local, "tank/data@zrb-2026-01-01T00:00:00Z").unwrap();
        assert_eq!(result, TransferDecision::FullSend);
    }

    #[test]
    fn decide_transfer_incremental_when_head_in_local() {
        let h = hello(Some("backup/data@zrb-2026-01-01T00:00:00Z"), None);
        let local = vec![
            "tank/data@zrb-2026-01-01T00:00:00Z".to_owned(),
            "tank/data@zrb-2026-01-10T00:00:00Z".to_owned(),
        ];
        let result = decide_transfer(&h, &local, "tank/data@zrb-2026-01-10T00:00:00Z").unwrap();
        assert_eq!(
            result,
            TransferDecision::IncrementalSend {
                base: "tank/data@zrb-2026-01-01T00:00:00Z".to_owned()
            }
        );
    }

    #[test]
    fn decide_transfer_error_when_head_absent_from_local() {
        let h = hello(Some("backup/data@zrb-2026-01-20T00:00:00Z"), None);
        let local = vec!["tank/data@zrb-2026-01-01T00:00:00Z".to_owned()];
        let err = decide_transfer(&h, &local, "tank/data@zrb-2026-01-25T00:00:00Z").unwrap_err();
        assert!(
            err.to_string().contains("zrb-2026-01-20"),
            "error should name the missing snapshot: {err}"
        );
    }

    // ── send_on with mock streams ─────────────────────────────────────────

    #[tokio::test]
    async fn send_on_errors_on_version_rejection() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let server_bytes =
            version_rejection_bytes("version mismatch: client 0.1.0, server 0.2.0").await;

        let result = send_on(
            latest, &local_snaps, &test_remote_cfg(), "backup/home", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(server_bytes)),
            &mut tokio::io::sink(), "primary", None, None,
        )
        .await;

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("version mismatch"), "unexpected: {msg}");
    }

    #[tokio::test]
    async fn send_on_emits_no_progress_on_version_rejection() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let server_bytes =
            version_rejection_bytes("version mismatch: client 0.1.0, server 0.2.0").await;
        let spy = Arc::new(SpySendProgress::default());

        let _ = send_on(
            latest, &local_snaps, &test_remote_cfg(), "backup/home", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(server_bytes)),
            &mut tokio::io::sink(), "primary", Some(spy.clone()), None,
        )
        .await;

        assert!(
            spy.calls().is_empty(),
            "version rejection should emit no progress calls — stream never starts"
        );
    }

    #[tokio::test]
    async fn send_on_emits_no_progress_when_already_up_to_date() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let h = hello(Some("backup/home@zrb-2026-01-20T00:00:00Z"), None);
        let spy = Arc::new(SpySendProgress::default());

        let _ = send_on(
            latest, &local_snaps, &test_remote_cfg(), "backup/home", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(version_ok_then_hello(&h).await)),
            &mut tokio::io::sink(), "primary", Some(spy.clone()), None,
        )
        .await;

        assert!(
            spy.calls().is_empty(),
            "already-up-to-date should emit no progress calls"
        );
    }

    #[tokio::test]
    async fn send_on_errors_when_remote_head_absent_from_local() {
        let latest = "tank/data@zrb-2026-01-25T00:00:00Z";
        let local_snaps = vec!["tank/data@zrb-2026-01-01T00:00:00Z".to_owned(), latest.to_owned()];
        let h = hello(Some("backup/data@zrb-2026-01-20T00:00:00Z"), None);

        let result = send_on(
            latest, &local_snaps, &test_remote_cfg(), "backup/data", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(version_ok_then_hello(&h).await)),
            &mut tokio::io::sink(), "primary", None, None,
        )
        .await;
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("zrb-2026-01-20"), "expected missing snapshot name in error: {msg}");
    }

    #[tokio::test]
    async fn send_on_resume_token_errors_gracefully() {
        let h = hello(None, Some("1-fake-resume-token"));
        let result = send_on(
            "tank/home@zrb-2026-01-20T00:00:00Z", &[],
            &test_remote_cfg(), "backup/home", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(version_ok_then_hello(&h).await)),
            &mut tokio::io::sink(), "primary", None, None,
        )
        .await;
        assert!(result.is_err(), "expected Err with fake resume token, got Ok");
    }

    #[tokio::test]
    async fn send_on_writes_client_not_ready_when_already_up_to_date() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let h = hello(Some("backup/home@zrb-2026-01-20T00:00:00Z"), None);
        let mut writer: Vec<u8> = Vec::new();

        let _ = send_on(
            latest, &local_snaps, &test_remote_cfg(), "backup/home", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(version_ok_then_hello(&h).await)),
            &mut writer, "primary", None, None,
        )
        .await;

        let mut r = tokio::io::BufReader::new(Cursor::new(&writer));
        let _: ClientHello = codec::decode_json(&mut r).await.unwrap();
        let ready: ClientReady = codec::decode_json(&mut r).await.unwrap();
        assert!(!ready.ok, "ClientReady.ok should be false when already up to date");
        assert!(
            ready.message.contains("already up to date"),
            "unexpected message: {}", ready.message
        );
    }

    #[tokio::test]
    async fn send_on_succeeds_silently_when_snapshot_already_on_server() {
        let latest = "tank/home@zrb-2026-01-20T00:00:00Z";
        let local_snaps = vec![latest.to_owned()];
        let h = hello(Some("backup/home@zrb-2026-01-20T00:00:00Z"), None);

        let result = send_on(
            latest, &local_snaps, &test_remote_cfg(), "backup/home", "my-laptop",
            &mut tokio::io::BufReader::new(Cursor::new(version_ok_then_hello(&h).await)),
            &mut tokio::io::sink(), "primary", None, None,
        )
        .await;

        assert!(result.is_ok(), "send_on should succeed silently when already on server, got: {:?}", result.err());
    }

    // ── collect_tasks ─────────────────────────────────────────────────────

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
        assert!(err.to_string().contains("ghost"), "expected remote name in error: {err}");
    }

    // ── estimate helpers ──────────────────────────────────────────────────

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

    // ── remote_receive_error ──────────────────────────────────────────────

    #[test]
    fn remote_receive_error_includes_original_message_and_hint() {
        let err = remote_receive_error("cannot receive: destination has snapshots (pool/data)");
        let msg = err.to_string();
        assert!(msg.contains("cannot receive:"), "missing original in: {msg}");
        assert!(msg.contains("hint:"), "missing hint in: {msg}");
        assert!(msg.contains("zfs rollback"), "missing rollback hint in: {msg}");
    }
}
