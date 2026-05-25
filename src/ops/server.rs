use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use chrono::Utc;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};

use crate::config::ServerConfig;
use crate::protocol::codec::{self, ServerHello, ServerStatus};
use crate::snapshot::naming;
use crate::zfs::client as zfs;

static CANCEL: AtomicBool = AtomicBool::new(false);

fn version_compatible(client: &str, server: &str) -> bool {
    let parse = |v: &str| -> Option<(u64, u64)> {
        let mut parts = v.splitn(3, '.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        Some((major, minor))
    };
    match (parse(client), parse(server)) {
        (Some(c), Some(s)) => c == s,
        _ => false,
    }
}

extern "C" fn handle_sighup(_: libc::c_int) {
    CANCEL.store(true, Ordering::Relaxed);
}

/// Run `zrb server` — reads from stdin, writes to stdout.
///
/// # Errors
/// Returns `Err` on I/O or codec failure.
pub fn server(config: &ServerConfig, permitted_clients: &[String]) -> anyhow::Result<()> {
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
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut input = tokio::io::BufReader::new(tokio::io::stdin());
        let mut output = tokio::io::stdout();
        run_server_on(config, &permitted, &mut input, &mut output, &CANCEL).await
    })
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
    let request = codec::decode_client_hello(input)
        .await
        .context("reading ClientHello")?;

    if !version_compatible(&request.version, env!("CARGO_PKG_VERSION")) {
        codec::encode_server_status(
            &ServerStatus {
                ok: false,
                message: format!(
                    "version mismatch: client {}, server {}",
                    request.version,
                    env!("CARGO_PKG_VERSION")
                ),
            },
            output,
        )
        .await?;
        return Ok(());
    }
    codec::encode_server_status(
        &ServerStatus {
            ok: true,
            message: "ok".to_owned(),
        },
        output,
    )
    .await?;

    if !permitted_clients.contains(&request.client_name.as_str()) {
        codec::encode_server_status(
            &ServerStatus {
                ok: false,
                message: format!("unknown client: {}", request.client_name),
            },
            output,
        )
        .await?;
        return Ok(());
    }

    let client_cfg = config
        .clients
        .get(&request.client_name)
        .context("client in permitted_clients but missing from config")?;
    if !client_cfg.allow.contains(&request.target) {
        codec::encode_server_status(
            &ServerStatus {
                ok: false,
                message: format!("dataset not allowed: {}", request.target),
            },
            output,
        )
        .await?;
        return Ok(());
    }

    let resume_token = zfs::get_resume_token(&request.target).context("checking resume token")?;

    let raw_snaps = zfs::list_snapshots(&request.target).context("listing snapshots")?;
    let snapshots = naming::filter_zrb(&raw_snaps);

    codec::encode_server_hello(
        &ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            snapshots,
            resume_token,
        },
        output,
    )
    .await?;
    output.flush().await?;

    let ready = codec::decode_client_ready(input)
        .await
        .context("reading ClientReady")?;
    if !ready.ok {
        log::info!("client declined to send: {}", ready.message);
        return Ok(());
    }

    let mut recv = zfs::receive(&request.target, &client_cfg.zfs_receive_opts)
        .context("spawning zfs receive")?;

    let stream_result = codec::read_stream_with_cancel(input, &mut recv.stdin, cancel).await;

    match stream_result {
        Ok(true) => {
            // SIGHUP: SSH session closed mid-transfer.
            let _ = recv.finish().await;
            annotate_resume_if_needed(&request.target)?;
            log::info!("client disconnected mid-transfer; cleaned up");
            return Ok(());
        }
        Ok(false) => match recv.finish().await {
            Ok(()) => {
                codec::encode_server_status(
                    &ServerStatus {
                        ok: true,
                        message: "ok".to_owned(),
                    },
                    output,
                )
                .await?;
            }
            Err(e) => {
                annotate_resume_if_needed(&request.target)?;
                codec::encode_server_status(
                    &ServerStatus {
                        ok: false,
                        message: e.to_string(),
                    },
                    output,
                )
                .await?;
            }
        },
        Err(e) => {
            let _ = recv.finish().await;
            annotate_resume_if_needed(&request.target)?;
            codec::encode_server_status(
                &ServerStatus {
                    ok: false,
                    message: e.to_string(),
                },
                output,
            )
            .await?;
        }
    }
    Ok(())
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
        codec::encode_client_hello(&msg, &mut buf).await.unwrap();
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
        let status =
            codec::decode_server_status(&mut tokio::io::BufReader::new(Cursor::new(&output)))
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
        let status =
            codec::decode_server_status(&mut tokio::io::BufReader::new(Cursor::new(&output)))
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
        // Current version is 0.1.0; send 0.1.99 — patch diff only, must be accepted.
        let input_bytes =
            make_client_hello_with_version("0.1.99", "my-laptop", "backup/laptop/home").await;
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
        let status =
            codec::decode_server_status(&mut tokio::io::BufReader::new(Cursor::new(&output)))
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
        let first = codec::decode_server_status(&mut cur).await.unwrap();
        let second = codec::decode_server_status(&mut cur).await.unwrap();
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
