use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisasterCategory {
    EarthquakeWarning,
    EarthquakeReport,
    WeatherWarning,
    Tsunami,
    Typhoon,
}

impl DisasterCategory {
    pub const ALL: [Self; 5] = [
        Self::EarthquakeWarning,
        Self::EarthquakeReport,
        Self::WeatherWarning,
        Self::Tsunami,
        Self::Typhoon,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::EarthquakeWarning => "earthquake_warning",
            Self::EarthquakeReport => "earthquake_report",
            Self::WeatherWarning => "weather_warning",
            Self::Tsunami => "tsunami",
            Self::Typhoon => "typhoon",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::EarthquakeWarning => "地震预警",
            Self::EarthquakeReport => "地震速报",
            Self::WeatherWarning => "气象预警",
            Self::Tsunami => "海啸预警",
            Self::Typhoon => "台风信息",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DisasterEvent {
    pub category: DisasterCategory,
    pub channel: ProviderChannel,
    pub source: String,
    pub event_id: String,
    pub revision: String,
    pub report_num: u32,
    pub title: String,
    pub description: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub magnitude: Option<f64>,
    pub depth_km: Option<f64>,
    pub affected_regions: Vec<String>,
    pub radius_km: Option<f64>,
    pub level: u8,
    pub occurred_at: String,
    pub final_report: bool,
    pub cancel: bool,
    pub training: bool,
}

impl DisasterEvent {
    pub fn event_key(&self) -> String {
        let category = self.category.as_str();
        format!(
            "{}:{}{}:{}{}:{}",
            category.len(),
            category,
            self.source.len(),
            self.source,
            self.event_id.len(),
            self.event_id
        )
    }
}

pub(crate) fn event_update_digest(event: &DisasterEvent) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(b"disaster-alert:event-update:v1\0");
    hash_string(&mut hash, event.category.as_str());
    hash_string(&mut hash, event.channel.as_str());
    for value in [
        &event.source,
        &event.event_id,
        &event.revision,
        &event.title,
        &event.description,
        &event.occurred_at,
    ] {
        hash_string(&mut hash, value);
    }
    hash.update(event.report_num.to_be_bytes());
    for value in [
        event.latitude,
        event.longitude,
        event.magnitude,
        event.depth_km,
        event.radius_km,
    ] {
        hash_optional_f64(&mut hash, value);
    }
    let mut affected_regions = event
        .affected_regions
        .iter()
        .map(|region| crate::utils::region::normalize(region))
        .filter(|region| !region.is_empty())
        .collect::<Vec<_>>();
    affected_regions.sort_unstable();
    affected_regions.dedup();
    hash.update(
        u64::try_from(affected_regions.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for region in &affected_regions {
        hash_string(&mut hash, region);
    }
    hash.update([
        event.level,
        u8::from(event.final_report),
        u8::from(event.cancel),
        u8::from(event.training),
    ]);
    hash.finalize()[..16].try_into().unwrap_or([0; 16])
}

pub(crate) fn event_update_id(event: &DisasterEvent) -> String {
    URL_SAFE_NO_PAD.encode(event_update_digest(event))
}

fn hash_string(hash: &mut Sha256, value: &str) {
    hash.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(value.as_bytes());
}

fn hash_optional_f64(hash: &mut Sha256, value: Option<f64>) {
    match value {
        Some(value) => {
            hash.update([1]);
            // IEEE-754 has two zero encodings, but the domain has one zero value. Keeping both
            // encodings would create different Inbox and update IDs for equivalent reports.
            let normalized = if value == 0.0 { 0.0 } else { value };
            hash.update(normalized.to_bits().to_be_bytes());
        }
        None => hash.update([0]),
    }
}

pub(crate) fn parse_event_epoch(event: &DisasterEvent) -> Option<i64> {
    parse_datetime_epoch_seconds(
        &event.occurred_at,
        crate::source_registry::default_utc_offset_seconds(&event.source),
    )
}

fn parse_datetime_epoch_seconds(value: &str, default_offset_seconds: Option<i64>) -> Option<i64> {
    let (date, raw_time) = value.trim().split_once([' ', 'T'])?;
    let mut date_parts = date.split(['-', '/']);
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (time, explicit_offset) = parse_time_offset(raw_time)?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second = time_parts.next().map_or(Some(0), |item| {
        let (seconds, fraction) = item
            .split_once('.')
            .map_or((item, None), |(seconds, fraction)| {
                (seconds, Some(fraction))
            });
        if fraction.is_some_and(|fraction| {
            fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        }) {
            return None;
        }
        seconds.parse().ok()
    })?;
    if time_parts.next().is_some()
        || !(1970..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
    {
        return None;
    }
    Some(
        days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second
            - explicit_offset.or(default_offset_seconds)?,
    )
}

fn parse_time_offset(value: &str) -> Option<(&str, Option<i64>)> {
    if let Some(time) = value.strip_suffix('Z') {
        return Some((time, Some(0)));
    }
    let offset_index = value
        .char_indices()
        .skip(1)
        .find(|(_, character)| matches!(character, '+' | '-'))
        .map(|(index, _)| index);
    let Some(index) = offset_index else {
        return Some((value, None));
    };
    let (time, offset) = value.split_at(index);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let body = &offset[1..];
    let bytes = body.as_bytes();
    if bytes.len() != 5
        || bytes[2] != b':'
        || ![bytes[0], bytes[1], bytes[3], bytes[4]]
            .into_iter()
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let hours = body[..2].parse::<i64>().ok()?;
    let minutes = body[3..].parse::<i64>().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some((time, Some(sign * (hours * 3_600 + minutes * 60))))
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let adjusted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderChannel {
    Wolfx,
    FanStudio,
    Huania,
}

impl ProviderChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wolfx => "wolfx",
            Self::FanStudio => "fanstudio",
            Self::Huania => "huania",
        }
    }
}

impl fmt::Display for ProviderChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(category: DisasterCategory, source: &str, event_id: &str) -> DisasterEvent {
        DisasterEvent {
            category,
            channel: ProviderChannel::FanStudio,
            source: source.to_string(),
            event_id: event_id.to_string(),
            revision: "1".to_string(),
            report_num: 1,
            title: String::new(),
            description: String::new(),
            latitude: None,
            longitude: None,
            magnitude: None,
            depth_km: None,
            affected_regions: Vec::new(),
            radius_km: None,
            level: 1,
            occurred_at: "2026-07-10 12:34:56".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        }
    }

    #[test]
    fn event_key_includes_category_and_is_unambiguous() {
        let warning = event(DisasterCategory::EarthquakeWarning, "ab", "c");
        let report = event(DisasterCategory::EarthquakeReport, "ab", "c");
        let split_differently = event(DisasterCategory::EarthquakeWarning, "a", "bc");
        assert_ne!(warning.event_key(), report.event_key());
        assert_ne!(warning.event_key(), split_differently.event_key());
    }

    #[test]
    fn update_digest_covers_revisionless_payload_corrections() {
        let mut first = event(DisasterCategory::EarthquakeWarning, "source", "event");
        first.revision.clear();
        let mut correction = first.clone();
        correction.title = "corrected".to_string();
        assert_ne!(
            event_update_digest(&first),
            event_update_digest(&correction)
        );
        assert_eq!(event_update_digest(&first), event_update_digest(&first));
    }

    #[test]
    fn update_digest_normalizes_negative_zero() {
        let mut positive = event(DisasterCategory::EarthquakeReport, "source", "event");
        positive.latitude = Some(0.0);
        positive.longitude = Some(0.0);
        let mut negative = positive.clone();
        negative.latitude = Some(-0.0);
        negative.longitude = Some(-0.0);
        assert_eq!(
            event_update_digest(&positive),
            event_update_digest(&negative)
        );
    }

    #[test]
    fn update_digest_treats_affected_regions_as_a_normalized_set() {
        let mut first = event(DisasterCategory::WeatherWarning, "source", "event");
        first.affected_regions = vec!["上海市".to_string(), "浦东新区".to_string()];
        let mut reordered = first.clone();
        reordered.affected_regions = vec![
            " 浦东新区 ".to_string(),
            "上海".to_string(),
            "上海市".to_string(),
        ];
        assert_eq!(event_update_digest(&first), event_update_digest(&reordered));
    }

    #[test]
    fn interprets_fanstudio_jma_time_as_jst() {
        let fanstudio = event(
            DisasterCategory::EarthquakeWarning,
            "fanstudio.jma",
            "fan-1",
        );
        let mut wolfx = fanstudio.clone();
        wolfx.channel = ProviderChannel::Wolfx;
        wolfx.source = "wolfx.jma_eew".to_string();
        assert_eq!(parse_event_epoch(&wolfx), parse_event_epoch(&fanstudio));
    }

    #[test]
    fn interprets_all_wolfx_china_naive_times_as_utc_plus_eight() {
        let mut utc = event(
            DisasterCategory::EarthquakeWarning,
            "future.explicit-timezone",
            "event-1",
        );
        utc.occurred_at = "2026-07-10T04:34:56Z".to_string();
        let expected = parse_event_epoch(&utc);

        for source in [
            "wolfx.cenc_eew",
            "wolfx.sc_eew",
            "wolfx.fj_eew",
            "wolfx.cq_eew",
        ] {
            let value = event(DisasterCategory::EarthquakeWarning, source, "event-1");
            assert_eq!(parse_event_epoch(&value), expected, "source {source}");
        }
    }

    #[test]
    fn interprets_fanstudio_weather_alarm_naive_time_as_utc_plus_eight() {
        let weather = event(
            DisasterCategory::WeatherWarning,
            "fanstudio.weatheralarm",
            "weather-1",
        );
        let mut utc = weather.clone();
        utc.source = "future.explicit-timezone".to_string();
        utc.occurred_at = "2026-07-10T04:34:56Z".to_string();
        assert_eq!(parse_event_epoch(&weather), parse_event_epoch(&utc));
    }

    #[test]
    fn rejects_ambiguous_leap_second_values() {
        let mut value = event(
            DisasterCategory::EarthquakeWarning,
            "fanstudio.jma",
            "fan-1",
        );
        value.occurred_at = "2026-07-10 23:59:60".to_string();
        assert_eq!(parse_event_epoch(&value), None);
    }

    #[test]
    fn rejects_malformed_fractional_seconds() {
        let mut value = event(
            DisasterCategory::EarthquakeWarning,
            "fanstudio.jma",
            "fan-1",
        );
        value.occurred_at = "2026-07-10 12:34:56.".to_string();
        assert_eq!(parse_event_epoch(&value), None);
        value.occurred_at = "2026-07-10 12:34:56.invalid".to_string();
        assert_eq!(parse_event_epoch(&value), None);
    }

    #[test]
    fn accepts_only_canonical_explicit_offsets() {
        for malformed in ["+8", "+1:2", "+0800", "+24:00", "+08:60"] {
            let value = format!("2026-07-10T12:34:56{malformed}");
            assert_eq!(parse_datetime_epoch_seconds(&value, None), None);
        }
        assert!(parse_datetime_epoch_seconds("2026-07-10T12:34:56+08:00", None).is_some());
    }

    #[test]
    fn global_or_unknown_sources_require_an_explicit_timezone() {
        let mut value = event(
            DisasterCategory::EarthquakeReport,
            "fanstudio.usgs",
            "usgs-1",
        );
        assert_eq!(parse_event_epoch(&value), None);
        value.occurred_at = "2026-07-10T12:34:56Z".to_string();
        assert!(parse_event_epoch(&value).is_some());
        value.source = "future.unknown-source".to_string();
        value.occurred_at = "2026-07-10T12:34:56-04:00".to_string();
        assert!(parse_event_epoch(&value).is_some());
    }
}
