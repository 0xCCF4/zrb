use crate::snapshot::naming;
use crate::zfs::client;

/// Return the zrb-managed snapshots for `dataset` in chronological order.
///
/// Snapshots not created by zrb are silently ignored.
///
/// # Errors
/// Propagates any `zfs list` subprocess error.
pub fn list(dataset: &str) -> anyhow::Result<Vec<String>> {
    let raw = client::list_snapshots(dataset)?;
    let mut managed = naming::filter_zrb(&raw);
    naming::sort_chronological(&mut managed);
    Ok(managed)
}
