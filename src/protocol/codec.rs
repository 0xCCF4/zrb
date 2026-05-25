use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
};

const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHello {
    pub version: String,
    pub snapshots: Vec<String>,
    pub resume_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientHello {
    pub version: String,
    pub client_name: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerStatus {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientReady {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// # Errors
/// Returns `CodecError` on I/O or JSON serialization failure.
pub async fn encode_server_hello<W: AsyncWrite + Unpin>(
    msg: &ServerHello,
    dest: &mut W,
) -> Result<(), CodecError> {
    encode_json(msg, dest).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON deserialization failure.
pub async fn decode_server_hello<R: AsyncBufRead + Unpin>(
    src: &mut R,
) -> Result<ServerHello, CodecError> {
    decode_json(src).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON serialization failure.
pub async fn encode_client_hello<W: AsyncWrite + Unpin>(
    msg: &ClientHello,
    dest: &mut W,
) -> Result<(), CodecError> {
    encode_json(msg, dest).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON deserialization failure.
pub async fn decode_client_hello<R: AsyncBufRead + Unpin>(
    src: &mut R,
) -> Result<ClientHello, CodecError> {
    decode_json(src).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON serialization failure.
pub async fn encode_server_status<W: AsyncWrite + Unpin>(
    msg: &ServerStatus,
    dest: &mut W,
) -> Result<(), CodecError> {
    encode_json(msg, dest).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON deserialization failure.
pub async fn decode_server_status<R: AsyncBufRead + Unpin>(
    src: &mut R,
) -> Result<ServerStatus, CodecError> {
    decode_json(src).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON serialization failure.
pub async fn encode_client_ready<W: AsyncWrite + Unpin>(
    msg: &ClientReady,
    dest: &mut W,
) -> Result<(), CodecError> {
    encode_json(msg, dest).await
}

/// # Errors
/// Returns `CodecError` on I/O or JSON deserialization failure.
pub async fn decode_client_ready<R: AsyncBufRead + Unpin>(
    src: &mut R,
) -> Result<ClientReady, CodecError> {
    decode_json(src).await
}

async fn encode_json<T: Serialize, W: AsyncWrite + Unpin>(
    msg: &T,
    dest: &mut W,
) -> Result<(), CodecError> {
    let mut bytes = serde_json::to_vec(msg)?;
    bytes.push(b'\n');
    dest.write_all(&bytes).await?;
    Ok(())
}

async fn decode_json<T: for<'de> Deserialize<'de>, R: AsyncBufRead + Unpin>(
    src: &mut R,
) -> Result<T, CodecError> {
    let mut line = String::new();
    let n = src.read_line(&mut line).await?;
    if n == 0 {
        return Err(CodecError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed mid-message",
        )));
    }
    Ok(serde_json::from_str(line.trim_end_matches('\n'))?)
}

/// Write `source` as 4 MiB chunks with 5-byte Control Frames to `dest`.
///
/// Each chunk is zero-padded to exactly 4 MiB. The Control Frame is
/// `u32 actual_size` (big-endian) + `u8 has_more` (1 = more chunks follow).
///
/// When `rate_limit` is `Some(bytes_per_sec)`, a token-bucket throttle sleeps
/// after each chunk to keep average throughput at or below the cap.
///
/// # Errors
/// Returns `CodecError` on I/O failure.
pub async fn write_stream<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    source: &mut R,
    dest: &mut W,
    rate_limit: Option<u64>,
    total_bytes: u64,
    mut progress: Option<&mut (dyn FnMut(u64, u64) + Send)>,
) -> Result<(), CodecError> {
    let mut cur = vec![0u8; CHUNK_SIZE];
    let mut nxt = vec![0u8; CHUNK_SIZE];
    let throttle_start = rate_limit.map(|_| std::time::Instant::now());
    let mut bytes_sent: u64 = 0;

    let mut cur_actual = read_exact_or_eof(source, &mut cur).await?;
    cur[cur_actual..].fill(0);

    loop {
        let nxt_actual = read_exact_or_eof(source, &mut nxt).await?;
        let has_more = nxt_actual > 0;
        dest.write_all(&cur).await?;
        // CHUNK_SIZE == 4 MiB which fits comfortably in u32.
        #[allow(clippy::cast_possible_truncation)]
        dest.write_all(&make_frame(cur_actual as u32, has_more))
            .await?;

        bytes_sent += cur_actual as u64;

        if let (Some(rate), Some(started)) = (rate_limit, throttle_start) {
            // Precision loss is acceptable: token-bucket timing only needs ms accuracy.
            #[allow(clippy::cast_precision_loss)]
            let expected = std::time::Duration::from_secs_f64(bytes_sent as f64 / rate as f64);
            let elapsed = started.elapsed();
            if let Some(deficit) = expected.checked_sub(elapsed) {
                tokio::time::sleep(deficit).await;
            }
        }

        if let Some(ref mut cb) = progress {
            cb(bytes_sent, total_bytes);
        }

        if !has_more {
            break;
        }
        nxt[nxt_actual..].fill(0);
        std::mem::swap(&mut cur, &mut nxt);
        cur_actual = nxt_actual;
    }
    Ok(())
}

/// Read a stream written by [`write_stream`] and write decoded bytes to `dest`.
///
/// # Errors
/// Returns `CodecError` on I/O failure.
pub async fn read_stream<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    source: &mut R,
    dest: &mut W,
) -> Result<(), CodecError> {
    let mut chunk = vec![0u8; CHUNK_SIZE];
    let mut frame = [0u8; 5];
    loop {
        source.read_exact(&mut chunk).await?;
        source.read_exact(&mut frame).await?;
        let actual_size = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        let has_more = frame[4];
        dest.write_all(&chunk[..actual_size]).await?;
        if has_more == 0 {
            break;
        }
    }
    Ok(())
}

/// Like [`read_stream`] but checks `cancel` between chunks.
///
/// Returns `Ok(true)` if `cancel` was set after a chunk; `Ok(false)` if the
/// stream completed normally.  The current chunk is always finished before
/// checking.
///
/// # Errors
/// Returns `CodecError` on I/O failure.
pub async fn read_stream_with_cancel<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    source: &mut R,
    dest: &mut W,
    cancel: &AtomicBool,
) -> Result<bool, CodecError> {
    let mut chunk = vec![0u8; CHUNK_SIZE];
    let mut frame = [0u8; 5];
    loop {
        source.read_exact(&mut chunk).await?;
        source.read_exact(&mut frame).await?;
        let actual_size = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        let has_more = frame[4];
        dest.write_all(&chunk[..actual_size]).await?;
        if has_more == 0 {
            break;
        }
        if cancel.load(Ordering::Relaxed) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn read_exact_or_eof<R: AsyncRead + Unpin>(
    src: &mut R,
    buf: &mut [u8],
) -> Result<usize, CodecError> {
    let mut total = 0;
    while total < buf.len() {
        match src.read(&mut buf[total..]).await? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

fn make_frame(actual_size: u32, has_more: bool) -> [u8; 5] {
    let mut frame = [0u8; 5];
    frame[..4].copy_from_slice(&actual_size.to_be_bytes());
    frame[4] = u8::from(has_more);
    frame
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    // ── JSON round-trips ──────────────────────────────────────────────────

    #[tokio::test]
    async fn client_ready_ok_round_trip() {
        let msg = ClientReady {
            ok: true,
            message: "ok".to_string(),
        };
        let mut buf = Vec::new();
        encode_client_ready(&msg, &mut buf).await.unwrap();
        let decoded = decode_client_ready(&mut tokio::io::BufReader::new(Cursor::new(&buf)))
            .await
            .unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn client_ready_not_ok_round_trip() {
        let msg = ClientReady {
            ok: false,
            message: "newest snapshot already on server".to_string(),
        };
        let mut buf = Vec::new();
        encode_client_ready(&msg, &mut buf).await.unwrap();
        let decoded = decode_client_ready(&mut tokio::io::BufReader::new(Cursor::new(&buf)))
            .await
            .unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn server_hello_round_trip() {
        let msg = ServerHello {
            version: "0.1.0".to_string(),
            snapshots: vec!["tank/home@zrb-2026-05-22T14:30:00Z".to_string()],
            resume_token: Some("opaque-token".to_string()),
        };
        let mut buf = Vec::new();
        encode_server_hello(&msg, &mut buf).await.unwrap();
        let decoded = decode_server_hello(&mut tokio::io::BufReader::new(Cursor::new(&buf)))
            .await
            .unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn server_hello_no_resume_token() {
        let msg = ServerHello {
            version: "0.1.0".to_string(),
            snapshots: vec![],
            resume_token: None,
        };
        let mut buf = Vec::new();
        encode_server_hello(&msg, &mut buf).await.unwrap();
        let decoded = decode_server_hello(&mut tokio::io::BufReader::new(Cursor::new(&buf)))
            .await
            .unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn client_hello_round_trip() {
        let msg = ClientHello {
            version: "0.1.0".to_string(),
            client_name: "my-laptop".to_string(),
            target: "backup/laptop/home".to_string(),
        };
        let mut buf = Vec::new();
        encode_client_hello(&msg, &mut buf).await.unwrap();
        let decoded = decode_client_hello(&mut tokio::io::BufReader::new(Cursor::new(&buf)))
            .await
            .unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn server_status_failure_round_trip() {
        let msg = ServerStatus {
            ok: false,
            message: "dataset not allowed".to_string(),
        };
        let mut buf = Vec::new();
        encode_server_status(&msg, &mut buf).await.unwrap();
        let decoded = decode_server_status(&mut tokio::io::BufReader::new(Cursor::new(&buf)))
            .await
            .unwrap();
        assert_eq!(decoded, msg);
    }

    // ── Binary framing ────────────────────────────────────────────────────

    #[tokio::test]
    async fn stream_round_trip_arbitrary_bytes() {
        let original: Vec<u8> = (0u8..=255).cycle().take(2_500_000).collect();
        let mut wire = Vec::new();
        write_stream(&mut Cursor::new(&original), &mut wire, None, 0, None)
            .await
            .unwrap();
        let mut recovered = Vec::new();
        read_stream(&mut Cursor::new(&wire), &mut recovered)
            .await
            .unwrap();
        assert_eq!(recovered, original);
    }

    #[tokio::test]
    async fn exactly_one_chunk_control_frame() {
        let data = vec![0xABu8; CHUNK_SIZE];
        let mut wire = Vec::new();
        write_stream(&mut Cursor::new(&data), &mut wire, None, 0, None)
            .await
            .unwrap();

        assert_eq!(wire.len(), CHUNK_SIZE + 5);
        let actual_size = u32::from_be_bytes([
            wire[CHUNK_SIZE],
            wire[CHUNK_SIZE + 1],
            wire[CHUNK_SIZE + 2],
            wire[CHUNK_SIZE + 3],
        ]);
        let has_more = wire[CHUNK_SIZE + 4];
        assert_eq!(actual_size as usize, CHUNK_SIZE);
        assert_eq!(has_more, 0);
    }

    #[tokio::test]
    async fn partial_chunk_zero_padded_correct_actual_size() {
        let data = vec![0xFFu8; 42];
        let mut wire = Vec::new();
        write_stream(&mut Cursor::new(&data), &mut wire, None, 0, None)
            .await
            .unwrap();

        assert_eq!(wire.len(), CHUNK_SIZE + 5);
        assert!(wire[42..CHUNK_SIZE].iter().all(|&b| b == 0));
        let actual_size = u32::from_be_bytes([
            wire[CHUNK_SIZE],
            wire[CHUNK_SIZE + 1],
            wire[CHUNK_SIZE + 2],
            wire[CHUNK_SIZE + 3],
        ]);
        assert_eq!(actual_size, 42);
        assert_eq!(wire[CHUNK_SIZE + 4], 0);
    }

    #[tokio::test]
    async fn progress_callback_receives_cumulative_bytes_and_total() {
        let data: Vec<u8> = (0u8..=255).cycle().take(CHUNK_SIZE + 512).collect();
        let total = data.len() as u64;
        let mut calls: Vec<(u64, u64)> = Vec::new();
        let cb: &mut (dyn FnMut(u64, u64) + Send) =
            &mut |bytes: u64, t: u64| calls.push((bytes, t));
        write_stream(
            &mut Cursor::new(&data),
            &mut tokio::io::sink(),
            None,
            total,
            Some(cb),
        )
        .await
        .unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], (CHUNK_SIZE as u64, total));
        assert_eq!(calls[1], (total, total));
    }

    #[tokio::test]
    async fn read_stream_with_cancel_false_completes_normally() {
        use std::sync::atomic::AtomicBool;
        let original: Vec<u8> = (0u8..=255).cycle().take(2_500_000).collect();
        let mut wire = Vec::new();
        write_stream(&mut Cursor::new(&original), &mut wire, None, 0, None)
            .await
            .unwrap();
        let cancel = AtomicBool::new(false);
        let mut recovered = Vec::new();
        let cancelled = read_stream_with_cancel(&mut Cursor::new(&wire), &mut recovered, &cancel)
            .await
            .unwrap();
        assert!(!cancelled, "cancel=false should return Ok(false)");
        assert_eq!(recovered, original);
    }

    #[tokio::test]
    async fn read_stream_with_cancel_true_stops_early() {
        use std::sync::atomic::AtomicBool;
        // Three chunks; cancel is pre-set; should return Ok(true) after first chunk
        let data: Vec<u8> = (0u8..=255).cycle().take(CHUNK_SIZE * 3).collect();
        let mut wire = Vec::new();
        write_stream(&mut Cursor::new(&data), &mut wire, None, 0, None)
            .await
            .unwrap();
        let cancel = AtomicBool::new(true);
        let cancelled =
            read_stream_with_cancel(&mut Cursor::new(&wire), &mut tokio::io::sink(), &cancel)
                .await
                .unwrap();
        assert!(cancelled, "cancel=true should return Ok(true)");
    }

    #[tokio::test]
    async fn progress_none_is_zero_overhead() {
        let data = vec![1u8; 100];
        let mut out = Vec::new();
        write_stream(&mut Cursor::new(&data), &mut out, None, 0, None)
            .await
            .unwrap();
        let mut recovered = Vec::new();
        read_stream(&mut Cursor::new(&out), &mut recovered)
            .await
            .unwrap();
        assert_eq!(recovered, data);
    }

    #[tokio::test]
    async fn throttled_write_respects_rate_limit() {
        // 50 KiB at 100 KiB/s should take >= ~0.5 s
        let data = vec![0u8; 50 * 1024];
        let rate: u64 = 100 * 1024;
        let start = std::time::Instant::now();
        write_stream(
            &mut Cursor::new(&data),
            &mut tokio::io::sink(),
            Some(rate),
            0,
            None,
        )
        .await
        .unwrap();
        assert!(
            start.elapsed() >= std::time::Duration::from_millis(450),
            "throttled write completed too fast: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn multi_chunk_has_more_flags() {
        let data: Vec<u8> = (0u8..255).cycle().take(CHUNK_SIZE * 2 + 512).collect();
        let mut wire = Vec::new();
        write_stream(&mut Cursor::new(&data), &mut wire, None, 0, None)
            .await
            .unwrap();

        let frame_start = |chunk_idx: usize| CHUNK_SIZE * (chunk_idx + 1) + 5 * chunk_idx;
        assert_eq!(wire[frame_start(0) + 4], 1, "chunk 0 has_more");
        assert_eq!(wire[frame_start(1) + 4], 1, "chunk 1 has_more");
        assert_eq!(wire[frame_start(2) + 4], 0, "chunk 2 has_more");
    }
}
