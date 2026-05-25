use std::process::Stdio;

use thiserror::Error;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::config::RemoteConfig;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("SSH connect to {destination}: {source}")]
    Spawn {
        destination: String,
        source: std::io::Error,
    },
}

pub struct SshConnection {
    pub stdin: ChildStdin,
    pub stdout: ChildStdout,
    pub child: Child,
}

/// Spawn `ssh` to `remote` with optional extra arguments and return piped stdio.
///
/// The caller reads/writes the zrb Protocol directly over `stdout`/`stdin`.
/// Call `child.wait().await` after the session ends to reap the process.
///
/// # Errors
/// Returns [`TransportError::Spawn`] if the `ssh` process cannot be started.
///
/// # Panics
/// Never panics — stdin/stdout are always present because `Stdio::piped()` is set unconditionally.
pub fn connect(
    remote: &RemoteConfig,
    extra_opts: &[String],
) -> Result<SshConnection, TransportError> {
    let mut cmd = Command::new("ssh");
    if let Some(port) = remote.port {
        cmd.arg("-p").arg(port.to_string());
    }
    if let Some(user) = &remote.user {
        cmd.arg("-l").arg(user);
    }
    if let Some(key) = &remote.ssh_key {
        cmd.arg("-i").arg(key);
    }
    for opt in &remote.ssh_opts {
        cmd.arg(opt);
    }
    for opt in extra_opts {
        cmd.arg(opt);
    }
    cmd.arg(&remote.host);
    cmd.arg("zrb server");
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let destination = match remote.port {
        Some(p) => format!("{}:{}", remote.host, p),
        None => remote.host.clone(),
    };
    let mut child = cmd.spawn().map_err(|source| TransportError::Spawn {
        destination,
        source,
    })?;
    let stdin = child.stdin.take().expect("stdin piped");
    let stdout = child.stdout.take().expect("stdout piped");
    Ok(SshConnection {
        stdin,
        stdout,
        child,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_error_includes_host_and_port() {
        let err = TransportError::Spawn {
            destination: "nas.local:2222".to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "No such file"),
        };
        let msg = err.to_string();
        assert!(msg.contains("nas.local"), "missing host in: {msg}");
        assert!(msg.contains("2222"), "missing port in: {msg}");
    }

    #[test]
    fn spawn_error_without_port_has_no_port_number() {
        let err = TransportError::Spawn {
            destination: "nas.local".to_owned(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "No such file"),
        };
        let msg = err.to_string();
        assert!(
            msg.starts_with("SSH connect to nas.local: "),
            "unexpected format: {msg}"
        );
    }
}
