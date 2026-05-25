use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EstimatorError {
    #[error("expected 'estimated size is <value><unit>' line not found in output")]
    LineMissing,
    #[error("could not parse size value '{0}'")]
    MalformedValue(String),
}

/// Parse the estimated transfer size from `zfs send -n -v` output.
///
/// # Errors
/// Returns [`EstimatorError::LineMissing`] if the expected line is absent,
/// or [`EstimatorError::MalformedValue`] if the value cannot be parsed.
pub fn parse_estimated_size(output: &str) -> Result<u64, EstimatorError> {
    let line = output
        .lines()
        .find(|l| l.contains("estimated size is"))
        .ok_or(EstimatorError::LineMissing)?;

    let after = line
        .split("estimated size is ")
        .nth(1)
        .ok_or(EstimatorError::LineMissing)?
        .trim();

    parse_size_str(after).ok_or_else(|| EstimatorError::MalformedValue(after.to_string()))
}

fn parse_size_str(s: &str) -> Option<u64> {
    // Split into numeric part and unit suffix.
    let split_pos = s.find(|c: char| c.is_alphabetic()).unwrap_or(s.len());
    let (num_str, unit) = s.split_at(split_pos);
    let num: f64 = num_str.trim().parse().ok()?;
    let multiplier: u64 = match unit.trim() {
        "" | "B" => 1,
        "K" | "KB" => 1_024,
        "M" | "MB" => 1_024 * 1_024,
        "G" | "GB" => 1_024 * 1_024 * 1_024,
        "T" | "TB" => 1_024u64 * 1_024 * 1_024 * 1_024,
        _ => return None,
    };
    // Estimate: float arithmetic is intentional; precision loss acceptable for size estimates.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    Some((num * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(size_str: &str) -> String {
        format!(
            "send from @zrb-2026-05-01T00:00:00Z to tank/home@zrb-2026-05-22T14:30:00Z estimated size is {size_str}\n"
        )
    }

    #[test]
    fn parses_gigabytes() {
        // 1.23 * 1_073_741_824 ≈ 1_320_702_443
        assert_eq!(parse_estimated_size(&output("1.23G")), Ok(1_320_702_443u64));
    }

    #[test]
    fn parses_gigabytes_long_suffix() {
        assert_eq!(
            parse_estimated_size(&output("2GB")),
            Ok(2 * 1_024 * 1_024 * 1_024)
        );
    }

    #[test]
    fn parses_megabytes() {
        assert_eq!(
            parse_estimated_size(&output("512M")),
            Ok(512 * 1_024 * 1_024)
        );
    }

    #[test]
    fn parses_megabytes_long_suffix() {
        assert_eq!(
            parse_estimated_size(&output("128MB")),
            Ok(128 * 1_024 * 1_024)
        );
    }

    #[test]
    fn parses_kilobytes() {
        assert_eq!(parse_estimated_size(&output("4K")), Ok(4 * 1_024));
    }

    #[test]
    fn parses_kilobytes_long_suffix() {
        assert_eq!(parse_estimated_size(&output("8KB")), Ok(8 * 1_024));
    }

    #[test]
    fn parses_bytes_no_suffix() {
        assert_eq!(parse_estimated_size(&output("1024")), Ok(1_024));
    }

    #[test]
    fn parses_bytes_explicit_suffix() {
        assert_eq!(parse_estimated_size(&output("42B")), Ok(42));
    }

    #[test]
    fn parses_terabytes() {
        assert_eq!(
            parse_estimated_size(&output("1T")),
            Ok(1_024u64 * 1_024 * 1_024 * 1_024)
        );
    }

    #[test]
    fn parses_terabytes_long_suffix() {
        assert_eq!(
            parse_estimated_size(&output("2TB")),
            Ok(2 * 1_024u64 * 1_024 * 1_024 * 1_024)
        );
    }

    #[test]
    fn missing_line_returns_error() {
        assert_eq!(
            parse_estimated_size("no relevant output here\n"),
            Err(EstimatorError::LineMissing)
        );
    }

    #[test]
    fn malformed_value_returns_error() {
        assert!(matches!(
            parse_estimated_size(&output("??G")),
            Err(EstimatorError::MalformedValue(_))
        ));
    }

    #[test]
    fn unknown_unit_returns_error() {
        assert!(matches!(
            parse_estimated_size(&output("5X")),
            Err(EstimatorError::MalformedValue(_))
        ));
    }
}
