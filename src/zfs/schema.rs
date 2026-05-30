use std::collections::HashMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct ZfsOutput {
    pub datasets: HashMap<String, DatasetInfo>,
}

#[derive(Debug, Deserialize)]
pub struct DatasetInfo {
    pub properties: HashMap<String, PropertyValue>,
}

#[derive(Debug, Deserialize)]
pub struct PropertyValue {
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SINGLE_SNAPSHOT: &str = r#"{
        "datasets": {
            "pool/ds@zrb-snap": {
                "properties": {
                    "name": { "value": "pool/ds@zrb-snap", "source": { "type": "NONE", "data": "" } }
                }
            }
        }
    }"#;

    #[test]
    fn single_snapshot_round_trips() {
        let out: ZfsOutput = serde_json::from_str(SINGLE_SNAPSHOT).unwrap();
        let ds = out.datasets.get("pool/ds@zrb-snap").unwrap();
        assert_eq!(ds.properties["name"].value, "pool/ds@zrb-snap");
    }

    #[test]
    fn unknown_top_level_fields_ignored() {
        let json = r#"{
            "output_version": {"command": "zfs list", "vers_major": 0, "vers_minor": 1},
            "datasets": {},
            "future_field": "ignored"
        }"#;
        let out: ZfsOutput = serde_json::from_str(json).unwrap();
        assert!(out.datasets.is_empty());
    }

    #[test]
    fn missing_property_key_returns_none() {
        let out: ZfsOutput = serde_json::from_str(SINGLE_SNAPSHOT).unwrap();
        let ds = out.datasets.get("pool/ds@zrb-snap").unwrap();
        assert!(!ds.properties.contains_key("nonexistent"));
    }
}
