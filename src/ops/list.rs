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

/// Return zrb-managed snapshots for every dataset on the host, grouped.
///
/// # Errors
/// Propagates any `zfs list` subprocess error.
pub fn list_all() -> anyhow::Result<Vec<(String, Vec<String>)>> {
    client::discover_datasets()?
        .into_iter()
        .map(|ds| {
            let snaps = list(&ds)?;
            Ok((ds, snaps))
        })
        .collect()
}

/// Return zrb-managed snapshots for `dataset` and all child datasets, grouped.
///
/// # Errors
/// Propagates any `zfs list` subprocess error.
pub fn list_recursive(dataset: &str) -> anyhow::Result<Vec<(String, Vec<String>)>> {
    datasets_matching(&client::discover_datasets()?, dataset)
        .into_iter()
        .map(|ds| {
            let snaps = list(&ds)?;
            Ok((ds, snaps))
        })
        .collect()
}

pub(crate) fn datasets_matching(datasets: &[String], root: &str) -> Vec<String> {
    let prefix = format!("{root}/");
    datasets
        .iter()
        .filter(|ds| ds.as_str() == root || ds.starts_with(&prefix))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strs(v: &[&str]) -> Vec<String> {
        v.iter().map(std::string::ToString::to_string).collect()
    }

    #[test]
    fn datasets_matching_excludes_prefix_substring() {
        let all = strs(&["tanker/docs"]);
        assert!(datasets_matching(&all, "tank").is_empty());
    }

    #[test]
    fn datasets_matching_includes_exact_root() {
        let all = strs(&["tank"]);
        assert_eq!(datasets_matching(&all, "tank"), strs(&["tank"]));
    }

    #[test]
    fn datasets_matching_includes_children_at_any_depth() {
        let all = strs(&["tank/home", "tank/home/sub", "tank/docs", "other/data"]);
        assert_eq!(
            datasets_matching(&all, "tank"),
            strs(&["tank/home", "tank/home/sub", "tank/docs"])
        );
    }
}
