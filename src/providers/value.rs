pub fn u32(data: &serde_json::Value, keys: &[&str]) -> u32 {
    keys.iter()
        .find_map(|key| {
            let value = data.get(*key)?;
            value
                .as_u64()
                .and_then(|number| u32::try_from(number).ok())
                .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        })
        .unwrap_or(0)
}

pub fn f64(data: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| data.get(*key).and_then(as_f64))
}

pub fn as_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        .filter(|number| number.is_finite())
}

pub fn bool(data: &serde_json::Value, keys: &[&str]) -> bool {
    keys.iter()
        .find_map(|key| {
            let value = data.get(*key)?;
            value.as_bool().or_else(|| {
                value
                    .as_str()
                    .and_then(|text| match text.trim().to_ascii_lowercase().as_str() {
                        "1" | "true" | "yes" => Some(true),
                        "0" | "false" | "no" => Some(false),
                        _ => None,
                    })
            })
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_continue_after_invalid_values() {
        let data = serde_json::json!({
            "ReportNum": null,
            "Serial": 2,
            "Latitude": "invalid",
            "lat": 35.5,
            "Cancel": null,
            "isCancel": true
        });
        assert_eq!(u32(&data, &["ReportNum", "Serial"]), 2);
        assert_eq!(f64(&data, &["Latitude", "lat"]), Some(35.5));
        assert!(bool(&data, &["Cancel", "isCancel"]));
    }
}
