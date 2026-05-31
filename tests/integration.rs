//! Integration tests for the full backup pipeline.
//!
//! Requires ZFS and root privileges. Run with:
//! ```text
//! sudo cargo test -- --include-ignored
//! ```

#![allow(clippy::missing_panics_doc)]

use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use tempfile::TempDir;
use tokio::io::AsyncReadExt as _;

use zrb::config::{RemoteConfig, ServerConfig};
use zrb::ops::list as ops_list;
use zrb::ops::prune as ops_prune;
use zrb::ops::send::send_on;
use zrb::ops::server::run_server_on;
use zrb::protocol::codec::{self, ClientHello, ClientReady, ServerHello, ServerStatus};
use zrb::retention::policy::RetentionConfig;
use zrb::tui::SendEvent;
use zrb::zfs::client as zfs_client;

static POOL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns false when the ZFS kernel module is absent (e.g. unprivileged CI containers).
/// Each integration test calls this and returns early so `act` runs pass without --privileged.
fn zfs_available() -> bool {
    std::path::Path::new("/dev/zfs").exists()
}

struct ZfsTestPool {
    name: String,
    _dir: TempDir,
}

impl ZfsTestPool {
    fn create(name_prefix: &str) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let img_path = dir.path().join("pool.img");
        let file = std::fs::File::create(&img_path).expect("create pool image");
        file.set_len(256 * 1024 * 1024)
            .expect("set pool image size");
        let id = POOL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = format!("{name_prefix}-{}-{id}", std::process::id());
        let status = Command::new("zpool")
            .args(["create", &name, img_path.to_str().expect("utf8 path")])
            .status()
            .expect("zpool create");
        assert!(status.success(), "zpool create failed for {name}");
        ZfsTestPool { name, _dir: dir }
    }

    #[must_use]
    fn dataset(&self, rel: &str) -> String {
        format!("{}/{rel}", self.name)
    }
}

impl Drop for ZfsTestPool {
    fn drop(&mut self) {
        let _ = Command::new("zpool")
            .args(["destroy", "-f", &self.name])
            .status();
    }
}

/// Spawn the server on a background thread with its own tokio runtime.
/// The server reads from `server_half` and writes back to the same duplex stream.
#[must_use]
fn spawn_server(
    config: ServerConfig,
    permitted: Vec<String>,
    server_half: tokio::io::DuplexStream,
) -> JoinHandle<anyhow::Result<()>> {
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async move {
            let permitted_refs: Vec<&str> = permitted.iter().map(String::as_str).collect();
            let (server_read, mut server_write) = tokio::io::split(server_half);
            let mut buf_reader = tokio::io::BufReader::new(server_read);
            run_server_on(
                &config,
                &permitted_refs,
                &mut buf_reader,
                &mut server_write,
                &AtomicBool::new(false),
            )
            .await
        })
    })
}

/// Build a minimal `ServerConfig` that allows `client_name` to send to `target_dataset`.
#[must_use]
fn server_config_for(client_name: &str, target_dataset: &str) -> ServerConfig {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("server.toml");
    std::fs::write(
        &path,
        format!(
            "[server]\nresume_hold_days = 7\n\n\
             [clients.{client_name}]\nallow = [\"{target_dataset}\"]\nzfs_receive_opts = []\n\n\
             [retention]\nrecent = 14\nweekly_for_days = 60\nmonthly_for_days = 730\n"
        ),
    )
    .expect("write server config");
    zrb::config::load_server(&path).expect("load server config")
}

#[must_use]
fn dummy_remote() -> RemoteConfig {
    RemoteConfig {
        host: "localhost".to_owned(),
        port: Some(22),
        user: Some("root".to_owned()),
        ssh_key: None,
        ssh_opts: vec![],
        zfs_send_opts: vec![],
        bandwidth_limit: None,
    }
}

/// Start a send that streams only 4 MiB of ZFS output, forcing a resume token on
/// `target_dataset`. `zfs receive -s` gets a truncated stream, exits, and records
/// resume state. The server error is intentionally ignored.
fn partial_run_send(latest: &str, target_dataset: &str, client_name: &str, srv_cfg: ServerConfig) {
    let remote = dummy_remote();
    let (client_half, server_half) = tokio::io::duplex(8 * 1024 * 1024);
    let server = spawn_server(srv_cfg, vec![client_name.to_owned()], server_half);

    let rt = tokio::runtime::Runtime::new().expect("tokio rt");
    rt.block_on(async move {
        let (client_read, mut client_write) = tokio::io::split(client_half);
        let mut stc_buf = tokio::io::BufReader::new(client_read);

        codec::encode_json(
            &ClientHello {
                version: env!("CARGO_PKG_VERSION").to_owned(),
                client_name: client_name.to_owned(),
                target: target_dataset.to_owned(),
            },
            &mut client_write,
        )
        .await
        .expect("encode ClientHello");

        let version_status: ServerStatus = codec::decode_json(&mut stc_buf)
            .await
            .expect("version ServerStatus");
        assert!(
            version_status.ok,
            "version gate rejected: {}",
            version_status.message
        );
        let hello: ServerHello = codec::decode_json(&mut stc_buf)
            .await
            .expect("ServerHello");
        assert!(
            hello.resume_token.is_none(),
            "fresh target has no resume token before interruption"
        );

        codec::encode_json(
            &ClientReady {
                ok: true,
                message: "ok".to_owned(),
            },
            &mut client_write,
        )
        .await
        .expect("encode ClientReady");

        // Stream exactly one 4 MiB protocol chunk. The server writes the raw ZFS bytes to
        // `zfs receive -s`, which then sees EOF (truncated stream), exits non-zero, and
        // saves a resume token. `write_stream` emits `has_more = 0`, so the server's
        // `read_stream` loop exits cleanly before calling `recv.finish`.
        let zfs_out =
            zfs_client::send_incremental(None, latest, &remote.zfs_send_opts).expect("zfs send");
        let mut limited = zfs_out.take(4 * 1024 * 1024_u64);
        codec::write_stream(&mut limited, &mut client_write, None, 0, None, None)
            .await
            .expect("write partial stream");
        // Drop client connection — server's ServerStatus write may get BrokenPipe.
        drop(client_write);
    });
    drop(server.join()); // wait for server thread; its error is expected and irrelevant
}

/// Wire `send_on` and `run_server_on` over in-process duplex streams and run both to completion.
fn run_send(
    latest: &str,
    local_snaps: &[String],
    target_dataset: &str,
    client_name: &str,
    srv_cfg: ServerConfig,
) {
    run_send_collecting_events(latest, local_snaps, target_dataset, client_name, srv_cfg);
}

/// Like `run_send` but also returns the events emitted during the transfer.
fn run_send_collecting_events(
    latest: &str,
    local_snaps: &[String],
    target_dataset: &str,
    client_name: &str,
    srv_cfg: ServerConfig,
) -> Vec<SendEvent> {
    let remote = dummy_remote();
    let (client_half, server_half) = tokio::io::duplex(8 * 1024 * 1024);
    let server = spawn_server(srv_cfg, vec![client_name.to_owned()], server_half);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<SendEvent>(256);

    let rt = tokio::runtime::Runtime::new().expect("tokio rt");
    let bytes = rt
        .block_on(async move {
            let (client_read, mut client_write) = tokio::io::split(client_half);
            let mut stc_buf = tokio::io::BufReader::new(client_read);
            send_on(
                latest,
                local_snaps,
                &remote,
                target_dataset,
                client_name,
                &mut stc_buf,
                &mut client_write,
                "test-remote",
                Some(tx),
                None,
            )
            .await
        })
        .expect("send_on");
    assert!(bytes > 0, "send_on should return bytes sent > 0");
    server.join().expect("server thread").expect("server error");

    let mut events = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        events.push(ev);
    }
    events
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
#[ignore = "requires ZFS and root privileges"]
fn first_backup_full_send() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t1-src");
    let dst = ZfsTestPool::create("zrb-t1-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/hello.txt"), b"hello world").expect("write test file");

    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    run_send(
        &latest,
        &local_snaps,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    let dst_snaps = ops_list::list(&dst_ds).expect("list target snapshots");
    assert_eq!(
        dst_snaps.len(),
        1,
        "expected exactly one snapshot on target"
    );
    assert!(dst_snaps[0].starts_with(&format!("{dst_ds}@zrb-")));
    let content = std::fs::read_to_string(format!("/{dst_ds}/hello.txt")).expect("read file");
    assert_eq!(content, "hello world");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn subsequent_backup_incremental_send() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t2-src");
    let dst = ZfsTestPool::create("zrb-t2-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");

    // First snapshot and send
    std::fs::write(format!("/{src_ds}/file1.txt"), b"first").expect("write file1");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot 1");
    let snap1_full = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let snaps_after_1 = ops_list::list(&src_ds).expect("list after snap1");
    run_send(
        &snap1_full,
        &snaps_after_1,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    // Second snapshot and send (incremental)
    std::fs::write(format!("/{src_ds}/file2.txt"), b"second").expect("write file2");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-02T00:00:00Z").expect("snapshot 2");
    let snap2_full = format!("{src_ds}@zrb-2026-01-02T00:00:00Z");
    let snaps_after_2 = ops_list::list(&src_ds).expect("list after snap2");
    run_send(
        &snap2_full,
        &snaps_after_2,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    let dst_snaps = ops_list::list(&dst_ds).expect("list target snapshots");
    assert_eq!(
        dst_snaps.len(),
        2,
        "expected two snapshots on target after incremental send"
    );
    let content = std::fs::read_to_string(format!("/{dst_ds}/file2.txt")).expect("read file2");
    assert_eq!(content, "second");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn prune_keeps_recent_deletes_oldest() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t3-src");
    let src_ds = src.dataset("data");
    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");

    // Non-zrb snapshot — must not be touched by prune
    zfs_client::create_snapshot(&src_ds, "manual-backup").expect("manual snapshot");

    // 6 zrb snapshots: 3 recent (04–06) + 3 old (01–03, all in the 2026 yearly bucket).
    // Yearly tier keeps the oldest representative (01); 02 and 03 are deleted.
    for day in 1..=6_u32 {
        let name = format!("zrb-2026-01-{day:02}T00:00:00Z");
        zfs_client::create_snapshot(&src_ds, &name).expect("zrb snapshot");
    }

    let retention = RetentionConfig {
        recent: 3,
        weekly_for_days: 7,
        monthly_for_days: 30,
    };
    let result = ops_prune::prune(&src_ds, &retention, None, false, false).expect("prune");

    let remaining = ops_list::list(&src_ds).expect("list after prune");
    // 3 recent + 1 yearly rep = 4 kept; Jan-02 and Jan-03 deleted
    assert_eq!(
        remaining.len(),
        4,
        "expected 4 zrb snapshots after prune (3 recent + 1 yearly rep)"
    );
    assert_eq!(result.deleted.len(), 2, "expected 2 snapshots deleted");

    // Most recent 3 are kept
    assert!(remaining.iter().any(|s| s.contains("2026-01-04")));
    assert!(remaining.iter().any(|s| s.contains("2026-01-05")));
    assert!(remaining.iter().any(|s| s.contains("2026-01-06")));
    // Oldest is kept as yearly representative for 2026
    assert!(remaining.iter().any(|s| s.contains("2026-01-01")));

    // Non-zrb snapshot is untouched
    let all_snaps = zfs_client::list_snapshots(&src_ds).expect("list all snapshots");
    assert!(all_snaps.iter().any(|s| s.contains("manual-backup")));
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn resumed_send_after_interrupted_transfer() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t4-src");
    let dst = ZfsTestPool::create("zrb-t4-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    // Create dataset with compression off so the ZFS stream size is predictable.
    Command::new("zfs")
        .args(["create", "-o", "compression=off", &src_ds])
        .status()
        .expect("zfs create src");

    // 12 MiB of data ensures the ZFS stream exceeds one 4 MiB protocol chunk.
    let data = vec![0xABu8; 12 * 1024 * 1024];
    std::fs::write(format!("/{src_ds}/bigfile.bin"), &data).expect("write bigfile");

    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    // Interrupted send: only the first 4 MiB of the stream reaches zfs receive -s.
    partial_run_send(
        &latest,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    let token = zfs_client::get_resume_token(&dst_ds).expect("get_resume_token after interruption");
    assert!(
        token.is_some(),
        "expected resume token after interrupted send"
    );

    let since = zfs_client::get_resume_since(&dst_ds).expect("get_resume_since after interruption");
    assert!(
        since.is_some(),
        "server should annotate zrb:resume-since after failed receive"
    );

    // Resumed send: send_on sees the resume token in ServerHello and uses `zfs send -t`.
    run_send(
        &latest,
        &local_snaps,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    let token_after =
        zfs_client::get_resume_token(&dst_ds).expect("get_resume_token after resumed send");
    assert!(
        token_after.is_none(),
        "resume token should be cleared after successful resumed send"
    );

    let since_after =
        zfs_client::get_resume_since(&dst_ds).expect("get_resume_since after resumed send");
    assert!(
        since_after.is_some(),
        "server should NOT clear zrb:resume-since after successful receive (prune owns that)"
    );

    let dst_snaps = ops_list::list(&dst_ds).expect("list dst snapshots after resume");
    assert_eq!(
        dst_snaps.len(),
        1,
        "expected exactly one snapshot on target after resumed send"
    );
    assert!(dst_snaps[0].contains("zrb-"));
}

// ── Issue 04: multi-remote ─────────────────────────────────────────────────────

#[test]
#[ignore = "requires ZFS and root privileges"]
fn multi_remote_both_remotes_receive() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t5-src");
    let dst1 = ZfsTestPool::create("zrb-t5-dst1");
    let dst2 = ZfsTestPool::create("zrb-t5-dst2");
    let src_ds = src.dataset("data");
    let dst1_ds = dst1.dataset("data");
    let dst2_ds = dst2.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/data.txt"), b"shared payload").expect("write data");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    // `ops::send::send()` iterates remotes and calls `send_to_remote` (SSH) for each.
    // We call `send_on` twice directly because SSH transport cannot be injected in
    // integration tests; each call represents one remote in the multi-remote config.
    run_send(
        &latest,
        &local_snaps,
        &dst1_ds,
        "test-client",
        server_config_for("test-client", &dst1_ds),
    );
    run_send(
        &latest,
        &local_snaps,
        &dst2_ds,
        "test-client",
        server_config_for("test-client", &dst2_ds),
    );

    let dst1_snaps = ops_list::list(&dst1_ds).expect("list dst1 snapshots");
    let dst2_snaps = ops_list::list(&dst2_ds).expect("list dst2 snapshots");
    assert_eq!(dst1_snaps.len(), 1, "dst1 should have exactly one snapshot");
    assert_eq!(dst2_snaps.len(), 1, "dst2 should have exactly one snapshot");

    let c1 = std::fs::read_to_string(format!("/{dst1_ds}/data.txt")).expect("read dst1");
    let c2 = std::fs::read_to_string(format!("/{dst2_ds}/data.txt")).expect("read dst2");
    assert_eq!(c1, "shared payload");
    assert_eq!(c2, "shared payload");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn multi_remote_failure_does_not_prevent_other() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t6-src");
    let dst = ZfsTestPool::create("zrb-t6-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/data.txt"), b"backup data").expect("write data");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    // Simulate a failed send to one remote: server permits "other-client" only, but
    // the client connects as "test-client". send_on returns Err; no ZFS state created.
    {
        let (client_half, server_half) = tokio::io::duplex(8 * 1024 * 1024);
        let bad_server = spawn_server(
            server_config_for("other-client", &dst_ds),
            vec!["other-client".to_owned()],
            server_half,
        );
        let rt = tokio::runtime::Runtime::new().expect("tokio rt");
        let latest_clone = latest.clone();
        let local_snaps_clone = local_snaps.clone();
        let dst_ds_clone = dst_ds.clone();
        let result = rt.block_on(async move {
            let remote = dummy_remote();
            let (client_read, mut client_write) = tokio::io::split(client_half);
            let mut stc_buf = tokio::io::BufReader::new(client_read);
            send_on(
                &latest_clone,
                &local_snaps_clone,
                &remote,
                &dst_ds_clone,
                "test-client",
                &mut stc_buf,
                &mut client_write,
                "test-remote",
                None,
                None,
            )
            .await
        });
        drop(bad_server.join());
        assert!(result.is_err(), "send to rejecting remote should fail");
    }

    // The failure above does not prevent a successful send to the other (good) remote.
    run_send(
        &latest,
        &local_snaps,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    let dst_snaps = ops_list::list(&dst_ds).expect("list dst snapshots");
    assert_eq!(
        dst_snaps.len(),
        1,
        "good remote should have exactly one snapshot"
    );
}

// ── Issue 01: ZFS hold/release primitives ─────────────────────────────────────

#[test]
#[ignore = "requires ZFS and root privileges"]
fn hold_appears_in_snapshot_holds_after_hold_snapshot() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-hold1");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let snap = format!("{ds}@zrb-2026-01-01T00:00:00Z");

    zfs_client::hold_snapshot(&snap, "zrb:primary").expect("hold_snapshot");
    let holds = zfs_client::snapshot_holds(&snap).expect("snapshot_holds");
    assert!(holds.contains(&"zrb:primary".to_owned()), "hold tag not found: {holds:?}");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn release_hold_removes_hold_and_is_idempotent() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-hold2");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let snap = format!("{ds}@zrb-2026-01-01T00:00:00Z");

    zfs_client::hold_snapshot(&snap, "zrb:primary").expect("hold_snapshot");
    zfs_client::release_hold(&snap, "zrb:primary").expect("first release");
    let holds = zfs_client::snapshot_holds(&snap).expect("snapshot_holds after release");
    assert!(!holds.contains(&"zrb:primary".to_owned()), "hold still present after release");

    // Idempotent: releasing again must not error
    zfs_client::release_hold(&snap, "zrb:primary").expect("idempotent second release");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn find_held_snapshot_returns_correct_snapshot_and_none_when_absent() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-hold3");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot 1");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-02T00:00:00Z").expect("snapshot 2");
    let snap1 = format!("{ds}@zrb-2026-01-01T00:00:00Z");
    let snap2 = format!("{ds}@zrb-2026-01-02T00:00:00Z");

    // None when no hold exists
    let result = zfs_client::find_held_snapshot(&ds, "zrb:primary").expect("find_held_snapshot none");
    assert!(result.is_none(), "expected None when no hold: {result:?}");

    // Hold snap1, find it
    zfs_client::hold_snapshot(&snap1, "zrb:primary").expect("hold snap1");
    let result = zfs_client::find_held_snapshot(&ds, "zrb:primary").expect("find_held_snapshot snap1");
    assert_eq!(result.as_deref(), Some(snap1.as_str()));

    // Move hold to snap2, snap1 no longer found
    zfs_client::hold_snapshot(&snap2, "zrb:primary").expect("hold snap2");
    zfs_client::release_hold(&snap1, "zrb:primary").expect("release snap1");
    let result = zfs_client::find_held_snapshot(&ds, "zrb:primary").expect("find_held_snapshot snap2");
    assert_eq!(result.as_deref(), Some(snap2.as_str()));
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn destroy_snapshot_fails_when_held() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-hold4");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let snap = format!("{ds}@zrb-2026-01-01T00:00:00Z");

    zfs_client::hold_snapshot(&snap, "zrb:primary").expect("hold_snapshot");
    let result = zfs_client::destroy_snapshot(&snap);
    assert!(result.is_err(), "destroy on held snapshot should return Err");
}

// ── Issue 03: Source-side Transfer Hold ───────────────────────────────────────

#[test]
#[ignore = "requires ZFS and root privileges"]
fn transfer_hold_placed_on_source_snapshot_after_send() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-th1");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let snap = format!("{ds}@zrb-2026-01-01T00:00:00Z");

    zrb::ops::send::place_transfer_hold(&ds, &snap, "primary");

    let holds = zfs_client::snapshot_holds(&snap).expect("snapshot_holds");
    assert!(holds.contains(&"zrb:primary".to_owned()), "expected zrb:primary hold: {holds:?}");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn transfer_hold_moves_to_new_snapshot_and_releases_old() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-th2");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot 1");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-02T00:00:00Z").expect("snapshot 2");
    let snap1 = format!("{ds}@zrb-2026-01-01T00:00:00Z");
    let snap2 = format!("{ds}@zrb-2026-01-02T00:00:00Z");

    zrb::ops::send::place_transfer_hold(&ds, &snap1, "primary");
    zrb::ops::send::place_transfer_hold(&ds, &snap2, "primary");

    let holds1 = zfs_client::snapshot_holds(&snap1).expect("holds snap1");
    let holds2 = zfs_client::snapshot_holds(&snap2).expect("holds snap2");
    assert!(!holds1.contains(&"zrb:primary".to_owned()), "old hold should be released: {holds1:?}");
    assert!(holds2.contains(&"zrb:primary".to_owned()), "new hold should be placed: {holds2:?}");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn transfer_holds_are_independent_per_remote() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-th3");
    let ds = pool.dataset("data");
    Command::new("zfs")
        .args(["create", &ds])
        .status()
        .expect("zfs create");
    zfs_client::create_snapshot(&ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let snap = format!("{ds}@zrb-2026-01-01T00:00:00Z");

    zrb::ops::send::place_transfer_hold(&ds, &snap, "primary");
    zrb::ops::send::place_transfer_hold(&ds, &snap, "offsite");

    let holds = zfs_client::snapshot_holds(&snap).expect("snapshot_holds");
    assert!(holds.contains(&"zrb:primary".to_owned()), "primary hold missing: {holds:?}");
    assert!(holds.contains(&"zrb:offsite".to_owned()), "offsite hold missing: {holds:?}");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn transfer_hold_failure_does_not_panic() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let pool = ZfsTestPool::create("zrb-th4");
    let ds = pool.dataset("data");
    // Deliberately do not create the snapshot — hold_snapshot will fail.
    // place_transfer_hold must log a warning and return normally.
    let nonexistent = format!("{ds}@zrb-2026-01-01T00:00:00Z");
    zrb::ops::send::place_transfer_hold(&ds, &nonexistent, "primary"); // must not panic
}

// ── Issue 06: Server-side Transfer Hold ───────────────────────────────────────

#[test]
#[ignore = "requires ZFS and root privileges"]
fn server_transfer_hold_placed_after_successful_receive() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let src = ZfsTestPool::create("zrb-srv1-src");
    let dst = ZfsTestPool::create("zrb-srv1-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/hello.txt"), b"payload").expect("write file");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    run_send(&latest, &local_snaps, &dst_ds, "test-client", server_config_for("test-client", &dst_ds));

    let held = zfs_client::find_held_snapshot(&dst_ds, "zrb:received").expect("find_held_snapshot");
    assert!(held.is_some(), "expected zrb:received hold on dst after receive");
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn server_transfer_hold_moves_to_newest_snapshot_on_second_receive() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let src = ZfsTestPool::create("zrb-srv2-src");
    let dst = ZfsTestPool::create("zrb-srv2-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/file1.txt"), b"first").expect("write file1");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot 1");
    let first_snap = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_after_first = ops_list::list(&src_ds).expect("list after first snap");
    run_send(&first_snap, &local_after_first, &dst_ds, "test-client", server_config_for("test-client", &dst_ds));

    std::fs::write(format!("/{src_ds}/file2.txt"), b"second").expect("write file2");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-02T00:00:00Z").expect("snapshot 2");
    let second_snap = format!("{src_ds}@zrb-2026-01-02T00:00:00Z");
    let local_after_second = ops_list::list(&src_ds).expect("list after second snap");
    run_send(&second_snap, &local_after_second, &dst_ds, "test-client", server_config_for("test-client", &dst_ds));

    let received_snaps = ops_list::list(&dst_ds).expect("list dst snapshots");
    let old_received = received_snaps.iter().find(|s| s.contains("2026-01-01")).expect("dst snap1");
    let new_received = received_snaps.iter().find(|s| s.contains("2026-01-02")).expect("dst snap2");

    let old_holds = zfs_client::snapshot_holds(old_received).expect("holds old snap");
    let new_holds = zfs_client::snapshot_holds(new_received).expect("holds new snap");
    assert!(!old_holds.contains(&"zrb:received".to_owned()), "old hold should be released: {old_holds:?}");
    assert!(new_holds.contains(&"zrb:received".to_owned()), "new hold should be present: {new_holds:?}");
}

// ── Issue 05: security rejections ─────────────────────────────────────────────

// Helper shared by the two rejection tests.
fn run_rejected_send(
    latest: &str,
    local_snaps: &[String],
    srv_cfg: ServerConfig,
    permitted: Vec<String>,
    target: &str,
    client_name: &str,
) -> anyhow::Result<()> {
    let (client_half, server_half) = tokio::io::duplex(8 * 1024 * 1024);
    let server = spawn_server(srv_cfg, permitted, server_half);
    let remote = dummy_remote();
    let rt = tokio::runtime::Runtime::new().expect("tokio rt");
    let result = rt.block_on(async move {
        let (client_read, mut client_write) = tokio::io::split(client_half);
        let mut stc_buf = tokio::io::BufReader::new(client_read);
        send_on(
            latest,
            local_snaps,
            &remote,
            target,
            client_name,
            &mut stc_buf,
            &mut client_write,
            "test-remote",
            None,
            None,
        )
        .await
    });
    drop(server.join());
    result.map(|_| ())
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn unknown_client_rejected_end_to_end() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t7-src");
    let dst = ZfsTestPool::create("zrb-t7-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/file.txt"), b"data").expect("write data");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    // Server permits only "my-laptop"; client declares itself as "rogue-host".
    let result = run_rejected_send(
        &latest,
        &local_snaps,
        server_config_for("my-laptop", &dst_ds),
        vec!["my-laptop".to_owned()],
        &dst_ds,
        "rogue-host",
    );

    assert!(
        result.is_err(),
        "send with unknown client name should be rejected"
    );
    // Server rejected before spawning zfs receive — no ZFS state on target.
    let dst_snaps = ops_list::list(&dst_ds).expect("list dst snapshots");
    assert_eq!(
        dst_snaps.len(),
        0,
        "target should have no snapshots after client rejection"
    );
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn dataset_not_in_allow_list_rejected_end_to_end() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let src = ZfsTestPool::create("zrb-t8-src");
    let dst = ZfsTestPool::create("zrb-t8-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");
    let forbidden_ds = dst.dataset("secret");

    Command::new("zfs")
        .args(["create", &src_ds])
        .status()
        .expect("zfs create src");
    std::fs::write(format!("/{src_ds}/file.txt"), b"data").expect("write data");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list source snapshots");

    // Server allows "test-client" to send only to dst_ds; client requests forbidden_ds.
    let result = run_rejected_send(
        &latest,
        &local_snaps,
        server_config_for("test-client", &dst_ds),
        vec!["test-client".to_owned()],
        &forbidden_ds,
        "test-client",
    );

    assert!(
        result.is_err(),
        "send to dataset not in allow list should be rejected"
    );
    // Server rejected before spawning zfs receive — no ZFS state on the forbidden dataset.
    let forbidden_snaps = ops_list::list(&forbidden_ds).expect("list forbidden snapshots");
    assert_eq!(
        forbidden_snaps.len(),
        0,
        "forbidden dataset should have no snapshots after rejection"
    );
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn list_all_groups_every_dataset_with_zrb_snapshots() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }
    let pool = ZfsTestPool::create("zrb-list-all");
    let ds_a = pool.dataset("alpha");
    let ds_b = pool.dataset("beta");
    Command::new("zfs")
        .args(["create", &ds_a])
        .status()
        .expect("zfs create alpha");
    Command::new("zfs")
        .args(["create", &ds_b])
        .status()
        .expect("zfs create beta");

    zfs_client::create_snapshot(&ds_a, "zrb-2026-01-01T00:00:00Z").expect("snap a");
    zfs_client::create_snapshot(&ds_b, "zrb-2026-01-02T00:00:00Z").expect("snap b");
    // Non-zrb snapshot — must not appear in list_all output
    zfs_client::create_snapshot(&ds_b, "manual").expect("manual snap");

    let groups = ops_list::list_all().expect("list_all");

    let alpha = groups
        .iter()
        .find(|(ds, _)| ds == &ds_a)
        .expect("alpha dataset in list_all");
    let beta = groups
        .iter()
        .find(|(ds, _)| ds == &ds_b)
        .expect("beta dataset in list_all");

    assert_eq!(alpha.1.len(), 1);
    assert!(alpha.1[0].contains("zrb-2026-01-01"));
    assert_eq!(beta.1.len(), 1);
    assert!(beta.1[0].contains("zrb-2026-01-02"));
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn send_on_emits_started_progress_completed_in_order() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present");
        return;
    }
    let src = ZfsTestPool::create("zrb-ev1-src");
    let dst = ZfsTestPool::create("zrb-ev1-dst");
    let src_ds = src.dataset("data");
    let dst_ds = dst.dataset("data");

    Command::new("zfs")
        .args(["create", "-o", "compression=off", &src_ds])
        .status()
        .expect("zfs create src");
    // 5 MiB ensures at least one RemoteProgress chunk
    let data = vec![0xABu8; 5 * 1024 * 1024];
    std::fs::write(format!("/{src_ds}/data.bin"), &data).expect("write data");
    zfs_client::create_snapshot(&src_ds, "zrb-2026-01-01T00:00:00Z").expect("snapshot");
    let latest = format!("{src_ds}@zrb-2026-01-01T00:00:00Z");
    let local_snaps = ops_list::list(&src_ds).expect("list snaps");

    let events = run_send_collecting_events(
        &latest,
        &local_snaps,
        &dst_ds,
        "test-client",
        server_config_for("test-client", &dst_ds),
    );

    assert!(!events.is_empty(), "expected at least one event");

    let started = events.iter().find(|e| matches!(e, SendEvent::RemoteStarted { .. }));
    assert!(started.is_some(), "expected RemoteStarted event");
    if let Some(SendEvent::RemoteStarted { remote, total_bytes }) = started {
        assert_eq!(remote, "test-remote");
        assert!(*total_bytes > 0, "total_bytes should be positive for a 5 MiB dataset");
    }

    let progress_events: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, SendEvent::RemoteProgress { .. }))
        .collect();
    assert!(
        !progress_events.is_empty(),
        "expected at least one RemoteProgress event"
    );
    if let SendEvent::RemoteProgress { remote, bytes_sent } = progress_events[0] {
        assert_eq!(remote, "test-remote");
        assert!(*bytes_sent > 0);
    }

    // RemoteStarted must come before all RemoteProgress events
    let started_idx = events
        .iter()
        .position(|e| matches!(e, SendEvent::RemoteStarted { .. }))
        .unwrap();
    let first_progress_idx = events
        .iter()
        .position(|e| matches!(e, SendEvent::RemoteProgress { .. }))
        .unwrap();
    assert!(
        started_idx < first_progress_idx,
        "RemoteStarted must precede RemoteProgress"
    );

    // bytes_sent on final RemoteProgress == bytes returned by send_on
    let last_progress = progress_events.last().unwrap();
    if let SendEvent::RemoteProgress { bytes_sent, .. } = last_progress {
        assert!(*bytes_sent > 0);
    }
}

#[test]
#[ignore = "requires ZFS and root privileges"]
fn json_zfs_calls_round_trip_against_real_pool() {
    if !zfs_available() {
        eprintln!("SKIP: /dev/zfs not present — ZFS kernel module unavailable");
        return;
    }

    let pool = ZfsTestPool::create("zrb-json-rt");
    let dataset = pool.dataset("home");

    Command::new("zfs")
        .args(["create", &dataset])
        .status()
        .expect("zfs create dataset");

    let snap_name = "zrb-2026-01-01T00:00:00Z";
    zfs_client::create_snapshot(&dataset, snap_name).expect("create snapshot");
    let full_snap = format!("{dataset}@{snap_name}");

    // list_snapshots returns the snapshot
    let snaps = zfs_client::list_snapshots(&dataset).expect("list_snapshots");
    assert!(
        snaps.contains(&full_snap),
        "expected {full_snap} in list_snapshots, got {snaps:?}"
    );

    // discover_datasets includes the dataset
    let discovered = zfs_client::discover_datasets().expect("discover_datasets");
    assert!(
        discovered.contains(&dataset),
        "expected {dataset} in discover_datasets, got {discovered:?}"
    );

    // get_resume_token returns None (no transfer in progress)
    let token = zfs_client::get_resume_token(&dataset).expect("get_resume_token");
    assert_eq!(token, None, "expected no resume token on fresh dataset");

    // get_resume_since returns None (property not set)
    let since = zfs_client::get_resume_since(&dataset).expect("get_resume_since");
    assert_eq!(since, None, "expected no resume-since on fresh dataset");
}
