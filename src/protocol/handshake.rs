use anyhow::Context;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt as _};

use crate::config::ServerConfig;
use crate::protocol::codec::{self, ClientHello, ServerHello, ServerStatus};
use crate::snapshot::naming;
use crate::zfs::client as zfs;

/// Outcome of a successful client-side handshake.
#[derive(Debug)]
pub struct ClientHandshakeResult {
    pub hello: ServerHello,
}

/// Outcome of a successful server-side handshake.
pub struct ServerHandshakeResult {
    pub target: String,
    pub zfs_receive_opts: Vec<String>,
}

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

/// Run the client side of the Protocol handshake.
///
/// Writes `ClientHello`, reads `ServerStatus` (version gate), then reads
/// `ServerHello`. Returns the `ServerHello` on success, or `Err` if the server
/// rejected the version or the I/O failed.
///
/// # Errors
/// Returns `Err` on I/O, codec, or version rejection.
pub async fn client_handshake<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut R,
    writer: &mut W,
    target: &str,
    client_name: &str,
) -> anyhow::Result<ClientHandshakeResult> {
    codec::encode_json(
        &ClientHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            client_name: client_name.to_owned(),
            target: target.to_owned(),
        },
        writer,
    )
    .await
    .context("writing ClientHello")?;

    let version_status: ServerStatus = codec::decode_json(reader)
        .await
        .context("reading version ServerStatus")?;
    if !version_status.ok {
        anyhow::bail!("server rejected connection: {}", version_status.message);
    }

    let hello: ServerHello = codec::decode_json(reader)
        .await
        .context("reading ServerHello")?;

    Ok(ClientHandshakeResult { hello })
}

/// Run the server side of the Protocol handshake.
///
/// Reads `ClientHello`, validates the version (writing `ServerStatus`), validates
/// the client name and dataset allowlist (writing additional `ServerStatus` messages
/// on rejection), then lists snapshots and writes `ServerHello`.
///
/// Returns `Ok(Some(result))` when the handshake succeeds and the transfer should
/// proceed. Returns `Ok(None)` when the server has already sent a rejection to the
/// client and the connection should close cleanly. Returns `Err` only on I/O failure.
///
/// # Errors
/// Returns `Err` on I/O or codec failure.
pub async fn server_handshake<R: AsyncBufRead + Unpin, W: AsyncWrite + Unpin>(
    config: &ServerConfig,
    permitted_clients: &[&str],
    input: &mut R,
    output: &mut W,
) -> anyhow::Result<Option<ServerHandshakeResult>> {
    let request: ClientHello = codec::decode_json(input)
        .await
        .context("reading ClientHello")?;

    if !version_compatible(&request.version, env!("CARGO_PKG_VERSION")) {
        codec::encode_json(
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
        return Ok(None);
    }
    codec::encode_json(
        &ServerStatus {
            ok: true,
            message: "ok".to_owned(),
        },
        output,
    )
    .await?;

    if !permitted_clients.contains(&request.client_name.as_str()) {
        codec::encode_json(
            &ServerStatus {
                ok: false,
                message: format!("unknown client: {}", request.client_name),
            },
            output,
        )
        .await?;
        return Ok(None);
    }

    let client_cfg = config
        .clients
        .get(&request.client_name)
        .context("client in permitted_clients but missing from config")?;
    if !client_cfg.allow.contains(&request.target) {
        codec::encode_json(
            &ServerStatus {
                ok: false,
                message: format!("dataset not allowed: {}", request.target),
            },
            output,
        )
        .await?;
        return Ok(None);
    }

    let resume_token = zfs::get_resume_token(&request.target).context("checking resume token")?;

    let raw_snaps = zfs::list_snapshots(&request.target).context("listing snapshots")?;
    let mut zrb_snaps = naming::filter_zrb(&raw_snaps);
    naming::sort_chronological(&mut zrb_snaps);
    let head = zrb_snaps.into_iter().last();

    codec::encode_json(
        &ServerHello {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            head,
            resume_token,
        },
        output,
    )
    .await?;
    output.flush().await?;

    Ok(Some(ServerHandshakeResult {
        target: request.target,
        zfs_receive_opts: client_cfg.zfs_receive_opts.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::protocol::codec::ServerStatus;

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

    async fn make_client_hello_bytes(version: &str, client_name: &str, target: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        codec::encode_json(
            &ClientHello {
                version: version.to_owned(),
                client_name: client_name.to_owned(),
                target: target.to_owned(),
            },
            &mut buf,
        )
        .await
        .unwrap();
        buf
    }

    async fn read_status(output: &[u8]) -> ServerStatus {
        codec::decode_json(&mut tokio::io::BufReader::new(Cursor::new(output)))
            .await
            .unwrap()
    }

    async fn read_two_statuses(output: &[u8]) -> (ServerStatus, ServerStatus) {
        let mut cur = tokio::io::BufReader::new(Cursor::new(output));
        let first: ServerStatus = codec::decode_json(&mut cur).await.unwrap();
        let second: ServerStatus = codec::decode_json(&mut cur).await.unwrap();
        (first, second)
    }

    #[tokio::test]
    async fn version_major_mismatch_returns_none_and_sends_rejection() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes =
            make_client_hello_bytes("1.0.0", "my-laptop", "backup/laptop/home").await;
        let mut output = Vec::new();
        let result = server_handshake(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
        )
        .await
        .unwrap();
        assert!(result.is_none(), "version mismatch should return None");
        let status = read_status(&output).await;
        assert!(!status.ok);
        assert!(status.message.contains("version mismatch"));
    }

    #[tokio::test]
    async fn version_minor_mismatch_returns_none_and_sends_rejection() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes =
            make_client_hello_bytes("0.99.0", "my-laptop", "backup/laptop/home").await;
        let mut output = Vec::new();
        let result = server_handshake(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
        )
        .await
        .unwrap();
        assert!(result.is_none());
        let status = read_status(&output).await;
        assert!(!status.ok);
        assert!(status.message.contains("version mismatch"));
    }

    #[tokio::test]
    async fn unknown_client_returns_none_after_version_ok() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes =
            make_client_hello_bytes(env!("CARGO_PKG_VERSION"), "rogue-host", "backup/laptop/home")
                .await;
        let mut output = Vec::new();
        let result = server_handshake(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
        )
        .await
        .unwrap();
        assert!(result.is_none());
        let (version_status, rejection) = read_two_statuses(&output).await;
        assert!(version_status.ok, "version gate should pass");
        assert!(!rejection.ok);
        assert!(rejection.message.contains("unknown client"));
    }

    #[tokio::test]
    async fn dataset_not_allowed_returns_none_after_version_ok() {
        let cfg = test_config();
        let permitted = ["my-laptop"];
        let input_bytes =
            make_client_hello_bytes(env!("CARGO_PKG_VERSION"), "my-laptop", "backup/laptop/secret")
                .await;
        let mut output = Vec::new();
        let result = server_handshake(
            &cfg,
            &permitted,
            &mut tokio::io::BufReader::new(Cursor::new(input_bytes)),
            &mut output,
        )
        .await
        .unwrap();
        assert!(result.is_none());
        let (version_status, rejection) = read_two_statuses(&output).await;
        assert!(version_status.ok, "version gate should pass");
        assert!(!rejection.ok);
        assert!(rejection.message.contains("not allowed"));
    }

    #[tokio::test]
    async fn client_handshake_propagates_version_rejection() {
        let rejection: ServerStatus = ServerStatus {
            ok: false,
            message: "version mismatch: client 0.1.0, server 0.2.0".to_owned(),
        };
        let mut buf = Vec::new();
        codec::encode_json(&rejection, &mut buf).await.unwrap();

        let result = client_handshake(
            &mut tokio::io::BufReader::new(Cursor::new(buf)),
            &mut tokio::io::sink(),
            "backup/home",
            "my-laptop",
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("version mismatch"));
    }
}
