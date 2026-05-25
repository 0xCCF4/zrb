use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::retention::policy::RetentionConfig;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("cannot parse config: {0}")]
    Toml(#[from] toml::de::Error),
}

/// Parse a human-readable bandwidth string into bytes/sec.
///
/// Accepts an optional SI prefix (k/K/m/M/g/G = ×1000/1e6/1e9) and an optional
/// unit suffix. If the suffix ends in `bit` or `bits`, the value is divided by 8
/// to convert from bits/sec to bytes/sec. A bare integer is treated as bytes/sec.
///
/// Examples: `"10M"` → `10_000_000`, `"100Mbit"` → `12_500_000`, `"1.5G"` → `1_500_000_000`.
///
/// # Errors
/// Returns a `String` error message if the input cannot be parsed.
pub fn parse_bandwidth(s: &str) -> Result<u64, String> {
    let s = s.trim();

    // Strip trailing "bits" or "bit" and record whether we saw them
    let lower = s.to_ascii_lowercase();
    let (s, is_bits) = if lower.ends_with("bits") {
        (&s[..s.len() - 4], true)
    } else if lower.ends_with("bit") {
        (&s[..s.len() - 3], true)
    } else if s.ends_with('B') | s.ends_with('b') {
        (&s[..s.len() - 1], false)
    } else {
        (s, false)
    };

    // Split numeric part from SI prefix
    let (number_str, scale) = match s.chars().last() {
        Some('k' | 'K') => (&s[..s.len() - 1], 1_000u64),
        Some('m' | 'M') => (&s[..s.len() - 1], 1_000_000u64),
        Some('g' | 'G') => (&s[..s.len() - 1], 1_000_000_000u64),
        _ => (s, 1u64),
    };

    let value: f64 = number_str
        .parse()
        .map_err(|_| format!("invalid bandwidth value: {number_str:?}"))?;

    if value < 0.0 {
        return Err(format!("bandwidth limit must be non-negative, got {value}"));
    }

    let divisor = if is_bits { 8.0f64 } else { 1.0f64 };
    // scale is at most 1e9; value is non-negative; truncation to u64 is intentional
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let bytes_per_sec = (value * (scale as f64) / divisor) as u64;

    Ok(bytes_per_sec)
}

fn deserialize_bandwidth_limit<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        None => Ok(None),
        Some(v) => parse_bandwidth(&v)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
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
    #[serde(default, deserialize_with = "deserialize_bandwidth_limit")]
    pub bandwidth_limit: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SourceSection {
    pub name: String,
}

/// Maps a remote name to the target dataset path on that remote.
pub type RemoteTargets = HashMap<String, String>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceConfig {
    source: SourceSection,
    pub remotes: HashMap<String, RemoteConfig>,
    pub datasets: HashMap<String, RemoteTargets>,
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
    pub clients: HashMap<String, ClientConfig>,
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
        assert_eq!(
            home.get("primary").map(String::as_str),
            Some("backup/laptop/home")
        );
        assert_eq!(cfg.retention.recent, 7);
        assert_eq!(cfg.retention.weekly_for_days, 30);
        assert_eq!(cfg.retention.monthly_for_days, 365);
    }

    #[test]
    fn server_config_deserializes_prd_example() {
        let cfg: ServerConfig = toml::from_str(SERVER_TOML).expect("should parse");
        assert_eq!(cfg.resume_hold_days(), 3);
        let client = cfg.clients.get("my-laptop").expect("my-laptop client");
        assert_eq!(
            client.allow,
            ["backup/laptop/home", "backup/laptop/documents"]
        );
        assert!(client.zfs_receive_opts.is_empty());
        assert_eq!(cfg.retention.recent, 14);
    }

    #[test]
    fn load_source_errors_on_missing_file() {
        let err =
            load_source(Path::new("/tmp/zrb-nonexistent-config.toml")).expect_err("should fail");
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
        assert_eq!(
            remote.ssh_key.as_deref(),
            Some("/home/user/.ssh/id_zfsbackup")
        );
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
    fn remote_config_bandwidth_limit_parses_megabytes() {
        let with_limit = SOURCE_TOML.replace(
            "zfs_send_opts = []",
            "zfs_send_opts = []\nbandwidth_limit = \"10M\"",
        );
        let cfg: SourceConfig = toml::from_str(&with_limit).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.bandwidth_limit, Some(10_000_000));
    }

    #[test]
    fn remote_config_bandwidth_limit_parses_mbit() {
        let with_limit = SOURCE_TOML.replace(
            "zfs_send_opts = []",
            "zfs_send_opts = []\nbandwidth_limit = \"100Mbit\"",
        );
        let cfg: SourceConfig = toml::from_str(&with_limit).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.bandwidth_limit, Some(12_500_000));
    }

    #[test]
    fn remote_config_bandwidth_limit_parses_decimal() {
        let with_limit = SOURCE_TOML.replace(
            "zfs_send_opts = []",
            "zfs_send_opts = []\nbandwidth_limit = \"1.5M\"",
        );
        let cfg: SourceConfig = toml::from_str(&with_limit).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.bandwidth_limit, Some(1_500_000));
    }

    #[test]
    fn remote_config_bandwidth_limit_parses_bare_integer() {
        let with_limit = SOURCE_TOML.replace(
            "zfs_send_opts = []",
            "zfs_send_opts = []\nbandwidth_limit = \"1048576\"",
        );
        let cfg: SourceConfig = toml::from_str(&with_limit).expect("should parse");
        let remote = cfg.remotes.get("primary").expect("primary remote");
        assert_eq!(remote.bandwidth_limit, Some(1_048_576));
    }

    #[test]
    fn parse_bandwidth_units() {
        assert_eq!(parse_bandwidth("1k").unwrap(), 1_000);
        assert_eq!(parse_bandwidth("1K").unwrap(), 1_000);
        assert_eq!(parse_bandwidth("1M").unwrap(), 1_000_000);
        assert_eq!(parse_bandwidth("1G").unwrap(), 1_000_000_000);
        assert_eq!(parse_bandwidth("100Mbit").unwrap(), 12_500_000);
        assert_eq!(parse_bandwidth("100Mbits").unwrap(), 12_500_000);
        assert_eq!(parse_bandwidth("1kbit").unwrap(), 125);
        assert_eq!(parse_bandwidth("1Gbit").unwrap(), 125_000_000);
        assert_eq!(parse_bandwidth("10MB").unwrap(), 10_000_000);
        assert_eq!(parse_bandwidth("500").unwrap(), 500);
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
