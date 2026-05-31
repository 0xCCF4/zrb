use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use chrono::Utc;
use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::config::ServerConfig;
use crate::protocol::codec::{self, ClientReady, ServerStatus};
use crate::protocol::handshake;
use crate::zfs::client as zfs;

static CANCEL: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sighup(_: libc::c_int) {
    CANCEL.store(true, Ordering::Relaxed);
}

/// Run `zrb server` — reads from stdin, writes to stdout.
///
/// # Errors
/// Returns `Err` on I/O or codec failure.
pub async fn server(config: &ServerConfig, permitted_clients: &[String]) -> anyhow::Result<()> {
    CANCEL.store(false, Ordering::Relaxed);
    // SAFETY: signal handlers that only set an AtomicBool are async-signal-safe.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
        libc::signal(
            libc::SIGHUP,
            handle_sighup as *const () as libc::sighandler_t,
        );
    }
    let permitted: Vec<&str> = permitted_clients.iter().map(String::as_str).collect();

    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    run_server_on(config, &permitted, &mut input, &mut output, &CANCEL).await
}

/// Run the server protocol over arbitrary async `Read`/`Write` streams.
///
/// `cancel` is checked between chunks; when set, the streaming loop stops
/// cleanly after the current chunk.
///
/// # Errors
/// Returns `Err` on I/O or codec failure. Validation rejections are sent as
/// `ServerStatus { ok: false }` and return `Ok(())`.
#[allow(clippy::too_many_lines)]
pub async fn run_server_on<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    config: &ServerConfig,
    permitted_clients: &[&str],
    input: &mut R,
    output: &mut W,
    cancel: &AtomicBool,
) -> anyhow::Result<()> {
    let Some(handshake::ServerHandshakeResult {
        target,
        zfs_receive_opts,
    }) = handshake::server_handshake(config, permitted_clients, input, output).await?
    else {
        return Ok(());
    };

    let ready: ClientReady = codec::decode_json(input)
        .await
        .context("reading ClientReady")?;
    if !ready.ok {
        log::info!("client declined to send: {}", ready.message);
        return Ok(());
    }

    let mut recv = zfs::receive(&target, &zfs_receive_opts)
        .context("spawning zfs receive")?;

    let stream_result = codec::read_stream_with_cancel(input, &mut recv.stdin, cancel).await;

    match stream_result {
        Ok(true) => {
            // SIGHUP: SSH session closed mid-transfer.
            let _ = recv.finish().await;
            annotate_resume_if_needed(&target)?;
            log::info!("client disconnected mid-transfer; cleaned up");
            return Ok(());
        }
        Ok(false) => match recv.finish().await {
            Ok(()) => {
                place_server_transfer_hold(&target);
                codec::encode_json(
                    &ServerStatus {
                        ok: true,
                        message: "ok".to_owned(),
                    },
                    output,
                )
                .await?;
            }
            Err(e) => {
                annotate_resume_if_needed(&target)?;
                codec::encode_json(
                    &ServerStatus {
                        ok: false,
                        message: e.to_string(),
                    },
                    output,
                )
                .await?;
            }
        },
        Err(_) => {
            let recv_err = recv.finish().await.err();
            annotate_resume_if_needed(&target)?;
            let message = recv_err.map_or_else(|| "stream error".to_owned(), |e| e.to_string());
            codec::encode_json(&ServerStatus { ok: false, message }, output)
                .await?;
        }
    }
    Ok(())
}

fn place_server_transfer_hold(dataset: &str) {
    const TAG: &str = "zrb:received";
    // Use the zrb-filtered, chronologically sorted snapshot list — one call serves
    // both finding the old hold and identifying the newest snapshot.
    let snaps = match crate::ops::list::list(dataset) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("Transfer Hold (server): failed to list snapshots for {dataset}: {e}");
            return;
        }
    };
    let Some(newest) = snaps.last() else {
        log::warn!("Transfer Hold (server): no snapshots found for {dataset}");
        return;
    };
    let old_snaps: Vec<String> = match zfs::find_held_snapshots_in(&snaps, TAG) {
        Ok(v) => v,
        Err(e) => {
            log::warn!("Transfer Hold (server): failed to find existing holds for {dataset}: {e}");
            vec![]
        }
    }
    .into_iter()
    .filter(|s| s != newest)
    .collect();
    if old_snaps.is_empty() && snaps.last().is_some_and(|s| s == newest) {
        // newest is already the only held snapshot — nothing to do
    }
    if let Err(e) = zfs::hold_snapshot(newest, TAG) {
        log::warn!("Transfer Hold (server): failed to hold {newest}: {e}");
        return;
    }
    for old_snap in &old_snaps {
        if let Err(e) = zfs::release_hold(old_snap, TAG) {
            log::warn!("Transfer Hold (server): failed to release old hold on {old_snap}: {e}");
        }
    }
}

fn annotate_resume_if_needed(dataset: &str) -> anyhow::Result<()> {
    if zfs::get_resume_token(dataset)
        .context("checking resume token for annotation")?
        .is_some()
        && zfs::get_resume_since(dataset)
            .context("checking resume-since for annotation")?
            .is_none()
    {
        zfs::set_resume_since(dataset, Utc::now()).context("setting resume-since")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::protocol::codec::ClientHello;

    fn no_cancel() -> AtomicBool {
        AtomicBool::new(false)
    }

    fn test_config() -> ServerConfig {
        toml::from_str(
            r#"
[server]
resume_hold_days = 3

[clients.my-laptop]
allow = ["backup/laptop/home"]
zfs_receive_opts = []

[retention]
recent = 14
weekly_for_days = 60
monthly_for_days = 730
"#,
        )
        .expect("test config")
    }

    async fn make_client_hello(client_name: &str, target: &str) -> Vec<u8> {
        make_client_hello_with_version(env!("CARGO_PKG_VERSION"), client_name, target).await
    }

    async fn make_client_hello_with_version(
        version: &str,
        client_name: &str,
        target: &str,
    ) -> Vec<u8> {
        let msg = ClientHello {
            version: version.to_owned(),
            client_name: client_name.to_owned(),
            target: target.to_owned(),
        };
        let mut buf = Vec::new();
        codec::encode_json(&msg, &mut buf).await.unwrap();
        buf
    }

    #[tokio::test]
    async fn version_major_mismatch_gets_rejection() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes =
            make_client_hello_with_version("1.0.0", "my-laptop", "backup/laptop/home").await;
        let mut output = Vec::new();
        run_server_on(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
            &no_cancel(),
        )
        .await
        .unwrap();
        let status: ServerStatus =
            codec::decode_json(&mut tokio::io::BufReader::new(Cursor::new(&output)))
                .await
                .unwrap();
        assert!(!status.ok);
        assert!(
            status.message.contains("version mismatch"),
            "unexpected: {}",
            status.message
        );
    }

    #[tokio::test]
    async fn version_minor_mismatch_gets_rejection() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes =
            make_client_hello_with_version("0.99.0", "my-laptop", "backup/laptop/home").await;
        let mut output = Vec::new();
        run_server_on(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
            &no_cancel(),
        )
        .await
        .unwrap();
        let status: ServerStatus =
            codec::decode_json(&mut tokio::io::BufReader::new(Cursor::new(&output)))
                .await
                .unwrap();
        assert!(!status.ok);
        assert!(
            status.message.contains("version mismatch"),
            "unexpected: {}",
            status.message
        );
    }

    #[tokio::test]
    async fn version_patch_difference_is_accepted() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        // Derive major.minor from the crate version and use a different patch — must be accepted.
        let major_minor = env!("CARGO_PKG_VERSION")
            .rsplitn(2, '.')
            .nth(1)
            .unwrap_or("0.1");
        let patched_version = format!("{major_minor}.99");
        let input_bytes =
            make_client_hello_with_version(&patched_version, "my-laptop", "backup/laptop/home")
                .await;
        let mut output = Vec::new();
        // Ignore the result: run_server_on may fail on the ZFS call that follows the
        // version gate (zfs binary absent in sandbox). We only care about the first
        // ServerStatus, which is written before any ZFS interaction.
        let _ = run_server_on(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
            &no_cancel(),
        )
        .await;
        // First ServerStatus is the version gate — must be ok.
        let status: ServerStatus =
            codec::decode_json(&mut tokio::io::BufReader::new(Cursor::new(&output)))
                .await
                .unwrap();
        assert!(
            status.ok,
            "patch-only version diff should be accepted: {}",
            status.message
        );
    }

    async fn read_two_statuses(output: &[u8]) -> (ServerStatus, ServerStatus) {
        let mut cur = tokio::io::BufReader::new(Cursor::new(output));
        let first: ServerStatus = codec::decode_json(&mut cur).await.unwrap();
        let second: ServerStatus = codec::decode_json(&mut cur).await.unwrap();
        (first, second)
    }

    #[tokio::test]
    async fn unknown_client_gets_rejection() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes = make_client_hello("rogue-host", "backup/laptop/home").await;
        let mut output = Vec::new();
        run_server_on(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
            &no_cancel(),
        )
        .await
        .unwrap();
        let (version_status, rejection) = read_two_statuses(&output).await;
        assert!(version_status.ok, "version gate should pass");
        assert!(!rejection.ok);
        assert!(rejection.message.contains("unknown client"));
    }

    #[tokio::test]
    async fn dataset_not_in_allow_list_gets_rejection() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes = make_client_hello("my-laptop", "backup/laptop/secret").await;
        let mut output = Vec::new();
        run_server_on(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
            &no_cancel(),
        )
        .await
        .unwrap();
        let (version_status, rejection) = read_two_statuses(&output).await;
        assert!(version_status.ok, "version gate should pass");
        assert!(!rejection.ok);
        assert!(rejection.message.contains("not allowed"));
    }
}
