use chrono::{DateTime, Utc};

const PREFIX: &str = "zrb-";
const TIMESTAMP_FMT: &str = "%Y-%m-%dT%H:%M:%SZ";

#[must_use]
pub fn new_name(dataset: &str, now: DateTime<Utc>) -> String {
    format!("{}@{}{}", dataset, PREFIX, now.format(TIMESTAMP_FMT))
}

#[must_use]
pub fn parse(name: &str) -> Option<DateTime<Utc>> {
    let (_dataset, snap) = name.split_once('@')?;
    let ts_str = snap.strip_prefix(PREFIX)?;
    DateTime::parse_from_rfc3339(ts_str)
        .ok()
        .map(|dt| dt.to_utc())
}

#[must_use]
pub fn is_zrb(name: &str) -> bool {
    parse(name).is_some()
}

#[must_use]
pub fn filter_zrb(names: &[String]) -> Vec<String> {
    names.iter().filter(|n| is_zrb(n)).cloned().collect()
}

pub fn sort_chronological(names: &mut [String]) {
    names.sort_by_key(|n| parse(n));
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_ts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
    }

    #[test]
    fn round_trip() {
        let now = make_ts(2026, 5, 22, 14, 30, 0);
        let name = new_name("tank/home", now);
        assert_eq!(parse(&name), Some(now));
    }

    #[test]
    fn is_zrb_accepts_valid() {
        assert!(is_zrb("tank/home@zrb-2026-05-22T14:30:00Z"));
    }

    #[test]
    fn is_zrb_rejects_non_zrb_prefix() {
        assert!(!is_zrb("tank/home@manual-2026-05-22T14:30:00Z"));
    }

    #[test]
    fn is_zrb_rejects_partial_prefix() {
        assert!(!is_zrb("tank/home@zrb2026-05-22T14:30:00Z"));
    }

    #[test]
    fn is_zrb_rejects_malformed_timestamp() {
        assert!(!is_zrb("tank/home@zrb-not-a-date"));
    }

    #[test]
    fn is_zrb_rejects_no_at_sign() {
        assert!(!is_zrb("zrb-2026-05-22T14:30:00Z"));
    }

    #[test]
    fn filter_zrb_keeps_only_managed() {
        let names = vec![
            "tank/home@zrb-2026-05-22T14:30:00Z".to_string(),
            "tank/home@manual-snap".to_string(),
            "tank/home@zrb-2026-05-21T10:00:00Z".to_string(),
        ];
        let kept = filter_zrb(&names);
        assert_eq!(kept.len(), 2);
        assert!(kept.contains(&"tank/home@zrb-2026-05-22T14:30:00Z".to_string()));
        assert!(kept.contains(&"tank/home@zrb-2026-05-21T10:00:00Z".to_string()));
    }

    #[test]
    fn sort_chronological_orders_oldest_first() {
        let mut names = vec![
            "tank/home@zrb-2026-05-22T14:30:00Z".to_string(),
            "tank/home@zrb-2026-05-20T08:00:00Z".to_string(),
            "tank/home@zrb-2026-05-21T10:00:00Z".to_string(),
        ];
        sort_chronological(&mut names);
        assert_eq!(
            names,
            vec![
                "tank/home@zrb-2026-05-20T08:00:00Z".to_string(),
                "tank/home@zrb-2026-05-21T10:00:00Z".to_string(),
                "tank/home@zrb-2026-05-22T14:30:00Z".to_string(),
            ]
        );
    }
}
