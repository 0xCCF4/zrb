use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::retention::policy::RetentionConfig;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot parse config: {0}")]
    Toml(#[from] toml::de::Error),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RemoteConfig {
    pub host: String,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub ssh_key: Option<String>,
    #[serde(default)]
    pub ssh_opts: Vec<String>,
    #[serde(default)]
    pub zfs_send_opts: Vec<String>,
    #[serde(default)]
    pub bandwidth_limit: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SourceSection {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    source: SourceSection,
    pub remotes: std::collections::HashMap<String, RemoteConfig>,
    pub datasets: std::collections::HashMap<String, std::collections::HashMap<String, String>>,
    pub retention: RetentionConfig,
}

impl SourceConfig {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.source.name
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ServerSection {
    pub resume_hold_days: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    pub allow: Vec<String>,
    #[serde(default)]
    pub zfs_receive_opts: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    server: ServerSection,
    pub clients: std::collections::HashMap<String, ClientConfig>,
    pub retention: RetentionConfig,
}

impl ServerConfig {
    #[must_use]
    pub fn resume_hold_days(&self) -> u32 {
        self.server.resume_hold_days
    }
}

/// Load and parse a source config file.
///
/// # Errors
/// Returns [`ConfigError::Io`] if the file cannot be read, or [`ConfigError::Toml`] if
/// it cannot be parsed.
pub fn load_source<P: AsRef<Path>>(path: P) -> Result<SourceConfig, ConfigError> {
    let text = std::fs::read_to_string(path.as_ref())?;
    let config = toml::from_str(&text)?;
    Ok(config)
}

/// Load and parse a server config file.
///
/// # Errors
/// Returns [`ConfigError::Io`] if the file cannot be read, or [`ConfigError::Toml`] if
/// it cannot be parsed.
pub fn load_server<P: AsRef<Path>>(path: P) -> Result<ServerConfig, ConfigError> {
    let text = std::fs::read_to_string(path.as_ref())?;
    let config = toml::from_str(&text)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE_TOML: &str = r#"
[source]
name = "my-laptop"

[remotes.primary]
host = "backup.example.com"
port = 22
user = "zfsbackup"
ssh_key = "/home/user/.ssh/id_zfsbackup"
ssh_opts = ["-o", "ServerAliveInterval=30"]
zfs_send_opts = []

[datasets."tank/home"]
primary = "backup/laptop/home"

[datasets."tank/documents"]
primary = "backup/laptop/documents"

[retention]
recent = 7
weekly_for_days = 30
monthly_for_days = 365
"#;

    const SERVER_TOML: &str = r#"
[server]
resume_hold_days = 3

[clients.my-laptop]
allow = ["backup/laptop/home", "backup/laptop/documents"]
zfs_receive_opts = []

[retention]
recent = 14
weekly_for_days = 60
monthly_for_days = 730
"#;

    #[test]
    fn source_config_deserializes_prd_example() {
        let cfg: SourceConfig = toml::from_str(SOURCE_TOML).expect("should parse");
        assert_eq!(cfg.name(), "my-laptop");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.host, "backup.example.com");
        assert_eq!(remote.port, Some(22));
        assert_eq!(remote.user.as_deref(), Some("zfsbackup"));
        assert_eq!(remote.ssh_opts, ["-o", "ServerAliveInterval=30"]);
        let home = cfg.datasets.get("tank/home").expect("tank/home dataset");
        assert_eq!(home.get("primary").map(String::as_str), Some("backup/laptop/home"));
        assert_eq!(cfg.retention.recent, 7);
        assert_eq!(cfg.retention.weekly_for_days, 30);
        assert_eq!(cfg.retention.monthly_for_days, 365);
    }

    #[test]
    fn server_config_deserializes_prd_example() {
        let cfg: ServerConfig = toml::from_str(SERVER_TOML).expect("should parse");
        assert_eq!(cfg.resume_hold_days(), 3);
        let client = cfg.clients.get("my-laptop").expect("my-laptop client");
        assert_eq!(client.allow, ["backup/laptop/home", "backup/laptop/documents"]);
        assert!(client.zfs_receive_opts.is_empty());
        assert_eq!(cfg.retention.recent, 14);
    }

    #[test]
    fn load_source_errors_on_missing_file() {
        let err = load_source(Path::new("/tmp/zrb-nonexistent-config.toml"))
            .expect_err("should fail");
        assert!(matches!(err, ConfigError::Io(_)));
    }

    #[test]
    fn load_source_accepts_str_literal() {
        let err = load_source("/tmp/zrb-nonexistent-config.toml").expect_err("should fail");
        assert!(matches!(err, ConfigError::Io(_)));
    }

    #[test]
    fn load_server_accepts_owned_pathbuf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "not valid toml [[[").unwrap();
        let err = load_server(path).expect_err("should fail");
        assert!(matches!(err, ConfigError::Toml(_)));
    }

    #[test]
    fn load_server_errors_on_malformed_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "not valid toml [[[").unwrap();
        let err = load_server(&path).expect_err("should fail");
        assert!(matches!(err, ConfigError::Toml(_)));
    }

    #[test]
    fn remote_config_ssh_key_present_is_some() {
        let cfg: SourceConfig = toml::from_str(SOURCE_TOML).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.ssh_key.as_deref(), Some("/home/user/.ssh/id_zfsbackup"));
    }

    #[test]
    fn remote_config_ssh_key_absent_is_none() {
        let no_key = SOURCE_TOML.replace("ssh_key = \"/home/user/.ssh/id_zfsbackup\"\n", "");
        let cfg: SourceConfig = toml::from_str(&no_key).expect("should parse without ssh_key");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert!(remote.ssh_key.is_none());
    }

    #[test]
    fn remote_config_bandwidth_limit_absent_is_none() {
        let cfg: SourceConfig = toml::from_str(SOURCE_TOML).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert!(remote.bandwidth_limit.is_none());
    }

    #[test]
    fn remote_config_bandwidth_limit_parses() {
        let with_limit = SOURCE_TOML.replace(
            "zfs_send_opts = []",
            "zfs_send_opts = []\nbandwidth_limit = 10485760",
        );
        let cfg: SourceConfig = toml::from_str(&with_limit).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.bandwidth_limit, Some(10_485_760));
    }

    #[test]
    fn remote_config_user_absent_is_none() {
        let no_user = SOURCE_TOML.replace("user = \"zfsbackup\"\n", "");
        let cfg: SourceConfig = toml::from_str(&no_user).expect("should parse without user");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert!(remote.user.is_none());
    }

    #[test]
    fn remote_config_port_absent_is_none() {
        let no_port = SOURCE_TOML.replace("port = 22\n", "");
        let cfg: SourceConfig = toml::from_str(&no_port).expect("should parse without port");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert!(remote.port.is_none());
    }

    #[test]
    fn load_source_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, SOURCE_TOML).unwrap();
        let cfg = load_source(&path).expect("should parse");
        assert_eq!(cfg.name(), "my-laptop");
    }
}
