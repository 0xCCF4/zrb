use chrono::Utc;

use crate::config::SourceConfig;
use crate::snapshot::naming;
use crate::zfs::client;

/// Create a new zrb-managed snapshot of `dataset` and return its full name.
///
/// The returned name (e.g. `tank/home@zrb-2026-05-22T14:30:00Z`) is suitable
/// as the Incremental Base for the next `ops::send` invocation.
///
/// # Errors
/// Propagates any `zfs snapshot` subprocess error.
///
/// # Panics
/// Never panics — `naming::new_name` always produces a name containing `@`.
pub fn snapshot(dataset: &str, _config: &SourceConfig) -> anyhow::Result<String> {
    let full_name = naming::new_name(dataset, Utc::now());
    let snap_name = full_name
        .split_once('@')
        .expect("new_name always contains @")
        .1;
    client::create_snapshot(dataset, snap_name)?;
    Ok(full_name)
}
