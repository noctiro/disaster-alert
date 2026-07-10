use crate::models::{DisasterCategory, DisasterEvent, ProviderChannel};
use crate::source_registry;

use super::value;

pub struct FanStudioBatch {
    pub source: String,
    pub md5: Option<String>,
    pub events: Vec<DisasterEvent>,
}

#[cfg(test)]
pub fn parse_fanstudio_update(message: &str) -> anyhow::Result<Vec<DisasterEvent>> {
    let root: serde_json::Value = serde_json::from_str(message)?;
    parse_fanstudio_update_value(&root)
}

pub fn parse_fanstudio_update_value(
    root: &serde_json::Value,
) -> anyhow::Result<Vec<DisasterEvent>> {
    if root.get("type").and_then(serde_json::Value::as_str) != Some("update") {
        return Ok(Vec::new());
    }
    let source = root
        .get("source")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("Fan Studio update is missing source"))?;
    if fanstudio_source_category(source).is_none() {
        return Ok(Vec::new());
    }
    let data = root
        .get("Data")
        .ok_or_else(|| anyhow::anyhow!("Fan Studio {source} update is missing Data"))?;
    parse_fanstudio_payload(
        source,
        data,
        root.get("md5").and_then(serde_json::Value::as_str),
    )
}

pub fn parse_fanstudio_snapshot(
    root: &serde_json::Value,
) -> Vec<(String, anyhow::Result<FanStudioBatch>)> {
    root.as_object()
        .into_iter()
        .flatten()
        .filter_map(|(source, snapshot)| {
            let data = snapshot.get("Data")?;
            let md5 = snapshot
                .get("md5")
                .and_then(serde_json::Value::as_str)
                .filter(|md5| !md5.is_empty())
                .map(ToOwned::to_owned);
            let parsed = parse_fanstudio_payload(source, data, md5.as_deref()).map(|events| {
                FanStudioBatch {
                    source: source.clone(),
                    md5,
                    events,
                }
            });
            Some((source.clone(), parsed))
        })
        .collect()
}

pub fn parse_fanstudio_payload(
    source: &str,
    data: &serde_json::Value,
    md5: Option<&str>,
) -> anyhow::Result<Vec<DisasterEvent>> {
    let mut events = parse_fanstudio_source(source, data)?;
    if events.is_empty() && source != "typhoon" {
        anyhow::bail!("Fan Studio {source} update does not match its documented schema");
    }
    if let Some(md5) = md5.filter(|md5| !md5.is_empty()) {
        for event in &mut events {
            event.revision = if event.revision.is_empty() {
                md5.to_string()
            } else {
                format!("{}:{md5}", event.revision)
            };
        }
    }
    Ok(events)
}

fn parse_fanstudio_source(
    source: &str,
    data: &serde_json::Value,
) -> anyhow::Result<Vec<DisasterEvent>> {
    match fanstudio_source_category(source) {
        Some(DisasterCategory::WeatherWarning) => {
            Ok(parse_weather_alarm(data).into_iter().collect())
        }
        Some(DisasterCategory::Tsunami) => Ok(parse_tsunami(data).into_iter().collect()),
        Some(DisasterCategory::Typhoon) => parse_typhoons(data),
        Some(
            category @ (DisasterCategory::EarthquakeWarning | DisasterCategory::EarthquakeReport),
        ) => Ok(parse_fanstudio_earthquake(source, data, category)
            .into_iter()
            .collect()),
        None => Ok(Vec::new()),
    }
}

pub fn fanstudio_source_category(source: &str) -> Option<DisasterCategory> {
    source_registry::find_provider(ProviderChannel::FanStudio, source)
        .map(|definition| definition.category)
}

fn parse_fanstudio_earthquake(
    source: &str,
    data: &serde_json::Value,
    category: DisasterCategory,
) -> Option<DisasterEvent> {
    let latitude =
        value::f64(data, &["latitude"]).filter(|latitude| (-90.0..=90.0).contains(latitude));
    let longitude =
        value::f64(data, &["longitude"]).filter(|longitude| (-180.0..=180.0).contains(longitude));
    let magnitude = data
        .get("magnitude")
        .or_else(|| data.get("allMagnitudes").and_then(|value| value.get("M")))
        .and_then(value::as_f64);
    let event_id = json_string(data, &["eventId", "id"])
        .or_else(|| fallback_earthquake_id(data, latitude, longitude, magnitude))?;
    let report_num = value::u32(data, &["updates"]);
    let place = json_string(data, &["placeName", "title"]).unwrap_or_default();
    let occurred_at =
        json_string(data, &["shockTime", "createTime", "updateTime"]).unwrap_or_default();
    Some(DisasterEvent {
        category,
        channel: ProviderChannel::FanStudio,
        source: format!("fanstudio.{source}"),
        event_id,
        revision: json_string(data, &["updates", "createTime", "updateTime", "shockTime"])
            .unwrap_or_default(),
        report_num,
        title: match category {
            DisasterCategory::EarthquakeWarning => format!("地震预警 {place}"),
            _ => format!("地震信息 {place}"),
        },
        description: magnitude.map_or_else(
            || place.clone(),
            |magnitude| format!("M{magnitude:.1} {place}"),
        ),
        latitude,
        longitude,
        magnitude,
        depth_km: flexible_depth(data.get("depth")),
        affected_regions: json_string_array(data, &["locationDesc", "affectedAreas", "province"]),
        radius_km: None,
        level: magnitude.map_or(1, severity_from_magnitude),
        occurred_at,
        final_report: value::bool(data, &["final", "Final", "isFinal"]),
        cancel: value::bool(data, &["cancel", "Cancel", "isCancel"]),
        training: false,
    })
}

fn parse_weather_alarm(data: &serde_json::Value) -> Option<DisasterEvent> {
    let title = json_string(data, &["title", "headline"])?;
    let code = json_string(data, &["type"])?;
    let affected_regions = weather_regions(&title);
    let cancel = value::bool(data, &["cancel", "Cancel", "isCancel"]) || title.contains("解除");
    Some(DisasterEvent {
        category: DisasterCategory::WeatherWarning,
        channel: ProviderChannel::FanStudio,
        source: "fanstudio.weatheralarm".to_string(),
        event_id: json_string(data, &["id"]).unwrap_or_else(|| title.clone()),
        revision: json_string(data, &["effective", "description", "headline"]).unwrap_or_default(),
        report_num: 0,
        title,
        description: json_string(data, &["description", "headline"]).unwrap_or_default(),
        latitude: value::f64(data, &["latitude"]),
        longitude: value::f64(data, &["longitude"]),
        magnitude: None,
        depth_km: None,
        affected_regions,
        radius_km: None,
        level: severity_from_warning_code(&code),
        occurred_at: json_string(data, &["effective"]).unwrap_or_default(),
        final_report: false,
        cancel,
        training: false,
    })
}

fn parse_tsunami(data: &serde_json::Value) -> Option<DisasterEvent> {
    let warning = data.get("warningInfo")?;
    let level_name = json_string(warning, &["level"])?;
    let shock = data.get("shockInfo").unwrap_or(data);
    Some(DisasterEvent {
        category: DisasterCategory::Tsunami,
        channel: ProviderChannel::FanStudio,
        source: "fanstudio.tsunami".to_string(),
        event_id: json_string(data, &["code", "id"])?,
        revision: json_string(data.get("details").unwrap_or(data), &["batch"])
            .or_else(|| json_string(data.get("timeInfo").unwrap_or(data), &["updateDate"]))
            .unwrap_or_default(),
        report_num: value::u32(data.get("details").unwrap_or(data), &["batch"]),
        title: json_string(warning, &["title"]).unwrap_or_else(|| "海啸预警".to_string()),
        description: format!(
            "{} {}",
            level_name,
            json_string(warning, &["subtitle"]).unwrap_or_default()
        ),
        latitude: value::f64(shock, &["latitude"]),
        longitude: value::f64(shock, &["longitude"]),
        magnitude: value::f64(shock, &["magnitude"]),
        depth_km: value::f64(shock, &["depth"]),
        affected_regions: tsunami_regions(data),
        radius_km: None,
        level: severity_from_cn_level(&level_name),
        occurred_at: json_string(
            data.get("timeInfo").unwrap_or(data),
            &["updateDate", "alarmDate"],
        )
        .unwrap_or_default(),
        final_report: false,
        cancel: level_name == "解除",
        training: false,
    })
}

fn parse_typhoons(data: &serde_json::Value) -> anyhow::Result<Vec<DisasterEvent>> {
    let items = data
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Fan Studio typhoon Data must be an array"))?;
    items
        .iter()
        .map(|item| {
            let id = json_string(item, &["id"])
                .ok_or_else(|| anyhow::anyhow!("Fan Studio typhoon item is missing id"))?;
            let name = json_string(item, &["name"]).unwrap_or_else(|| id.clone());
            let power = json_optional_u32(item, &["power"]);
            Ok(DisasterEvent {
                category: DisasterCategory::Typhoon,
                channel: ProviderChannel::FanStudio,
                source: "fanstudio.typhoon".to_string(),
                event_id: id,
                revision: json_string(item, &["updateTime"]).unwrap_or_default(),
                report_num: 0,
                title: format!("台风 {name}"),
                description: format!(
                    "{} 风力{}级",
                    json_string(item, &["type"]).unwrap_or_default(),
                    power.map_or_else(|| "未知".to_string(), |value| value.to_string())
                ),
                latitude: value::f64(item, &["latitude"]),
                longitude: value::f64(item, &["longitude"]),
                magnitude: None,
                depth_km: None,
                affected_regions: Vec::new(),
                radius_km: value::f64(item, &["radius7"]),
                level: if power.is_some_and(|value| value >= 14) {
                    4
                } else if power.is_some_and(|value| value >= 10) {
                    3
                } else {
                    1
                },
                occurred_at: json_string(item, &["updateTime"]).unwrap_or_default(),
                final_report: false,
                cancel: false,
                training: false,
            })
        })
        .collect()
}

fn json_string(data: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        data.get(*key)
            .and_then(|value| {
                value
                    .as_str()
                    .map(ToOwned::to_owned)
                    .or_else(|| value.as_i64().map(|number| number.to_string()))
            })
            .filter(|value| !value.trim().is_empty())
    })
}

fn json_optional_u32(data: &serde_json::Value, keys: &[&str]) -> Option<u32> {
    keys.iter().find_map(|key| {
        data.get(*key).and_then(|value| {
            value
                .as_u64()
                .and_then(|number| u32::try_from(number).ok())
                .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        })
    })
}

fn flexible_depth(value: Option<&serde_json::Value>) -> Option<f64> {
    let value = value?;
    if let Some(number) = value::as_f64(value) {
        return (number >= 0.0).then_some(number);
    }
    let text = value.as_str()?.trim();
    let numeric_prefix = text
        .char_indices()
        .take_while(|(_, character)| {
            character.is_ascii_digit() || matches!(character, '.' | '+' | '-')
        })
        .last()
        .map(|(index, character)| index + character.len_utf8())?;
    text[..numeric_prefix]
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite() && *number >= 0.0)
}

fn json_string_array(data: &serde_json::Value, keys: &[&str]) -> Vec<String> {
    let mut values = Vec::new();
    for key in keys {
        let Some(value) = data.get(*key) else {
            continue;
        };
        if let Some(items) = value.as_array() {
            values.extend(
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(ToOwned::to_owned)),
            );
        } else if let Some(text) = value.as_str()
            && !text.trim().is_empty()
        {
            values.push(text.to_string());
        }
    }
    values
}

fn fallback_earthquake_id(
    data: &serde_json::Value,
    latitude: Option<f64>,
    longitude: Option<f64>,
    magnitude: Option<f64>,
) -> Option<String> {
    let time = json_string(data, &["shockTime", "createTime", "updateTime"])?;
    Some(format!(
        "{time}:{:.3}:{:.3}:{:.1}",
        latitude?, longitude?, magnitude?
    ))
}

fn weather_regions(title: &str) -> Vec<String> {
    let Some((issuer, _)) = title.split_once("气象台") else {
        return Vec::new();
    };
    let region = issuer.trim();
    if region.is_empty() || region.chars().count() > 80 {
        return Vec::new();
    }
    vec![region.to_string()]
}

fn tsunami_regions(data: &serde_json::Value) -> Vec<String> {
    let mut regions = Vec::new();
    for forecast in data
        .get("forecasts")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        regions.extend(json_string_array(
            forecast,
            &["province", "forecastArea", "forecastPoint"],
        ));
    }
    regions.sort();
    regions.dedup();
    regions
}

fn severity_from_warning_code(value: &str) -> u8 {
    if value.contains("red") || value.contains("红") {
        4
    } else if value.contains("orange") || value.contains("橙") {
        3
    } else if value.contains("yellow") || value.contains("黄") {
        2
    } else {
        1
    }
}

fn severity_from_cn_level(value: &str) -> u8 {
    match value {
        "红色" => 4,
        "橙色" => 3,
        "黄色" => 2,
        "蓝色" => 1,
        "信息" => 1,
        _ => 0,
    }
}

fn severity_from_magnitude(magnitude: f64) -> u8 {
    if magnitude >= 7.0 {
        4
    } else if magnitude >= 6.0 {
        3
    } else if magnitude >= 5.0 {
        2
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_fanstudio_all_updates_by_source() {
        let weather = parse_fanstudio_update(
            r#"{"type":"update","source":"weatheralarm","Data":{"id":"w1","title":"雷雨大风黄色预警","description":"预计六小时内有雷雨大风","effective":"2026-07-10 02:32:27","longitude":104.67,"latitude":36.56,"type":"11B20_yellow"},"md5":"a"}"#,
        );
        assert!(weather.is_ok());
        if let Ok(events) = weather {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].category, DisasterCategory::WeatherWarning);
            assert_eq!(events[0].level, 2);
        }

        let typhoons = parse_fanstudio_update(
            r#"{"type":"update","source":"typhoon","Data":[{"id":"202609","name":"巴威","latitude":13.7,"longitude":147.1,"power":18,"type":"超强台风","updateTime":"2026-07-05 20:00:00"}],"md5":"b"}"#,
        );
        assert!(typhoons.is_ok());
        if let Ok(events) = typhoons {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].category, DisasterCategory::Typhoon);
        }
    }

    #[test]
    fn classifies_every_supported_fanstudio_all_source() {
        let warning_sources = ["cea", "cea-pr", "cwa-eew", "jma", "sa", "kma-eew"];
        let report_sources = [
            "cenc", "ningxia", "guangxi", "shanxi", "beijing", "yunnan", "cwa", "hko", "usgs",
            "emsc", "bcsf", "gfz", "usp", "kma", "fssn", "fssn-cmt",
        ];
        let data = serde_json::json!({
            "id": "event-1",
            "latitude": 35.0,
            "longitude": 105.0,
            "magnitude": 5.2,
            "shockTime": "2026-07-10 12:34:56"
        });

        for source in warning_sources {
            let events = parse_fanstudio_source(source, &data);
            assert!(
                matches!(events, Ok(ref events) if events.len() == 1 && events[0].category == DisasterCategory::EarthquakeWarning),
                "warning source {source} was not classified: {events:?}"
            );
        }
        for source in report_sources {
            let events = parse_fanstudio_source(source, &data);
            assert!(
                matches!(events, Ok(ref events) if events.len() == 1 && events[0].category == DisasterCategory::EarthquakeReport),
                "report source {source} was not classified: {events:?}"
            );
        }

        assert!(matches!(
            parse_fanstudio_source(
                "weatheralarm",
                &serde_json::json!({"id":"w1","title":"黄色预警","type":"yellow"})
            ),
            Ok(events) if events.len() == 1 && events[0].category == DisasterCategory::WeatherWarning
        ));
        assert!(matches!(
            parse_fanstudio_source(
                "tsunami",
                &serde_json::json!({
                    "code":"t1",
                    "warningInfo":{"level":"黄色","title":"海啸预警"},
                    "shockInfo":{"latitude":1.0,"longitude":2.0}
                })
            ),
            Ok(events) if events.len() == 1 && events[0].category == DisasterCategory::Tsunami
        ));
        assert!(matches!(
            parse_fanstudio_source(
                "typhoon",
                &serde_json::json!([{"id":"p1","latitude":1.0,"longitude":2.0}])
            ),
            Ok(events) if events.len() == 1 && events[0].category == DisasterCategory::Typhoon
        ));
    }

    #[test]
    fn parses_snapshot_payload_through_the_shared_source_parser() {
        let parsed = parse_fanstudio_payload(
            "weatheralarm",
            &serde_json::json!({
                "id":"w1",
                "title":"广州市气象台发布黄色预警",
                "type":"yellow"
            }),
            Some("snapshot-revision"),
        );
        assert!(matches!(
            parsed,
            Ok(events) if events.len() == 1 && events[0].revision == "snapshot-revision"
        ));
    }

    #[test]
    fn parses_initial_all_into_source_batches() {
        let batches = parse_fanstudio_snapshot(&serde_json::json!({
            "type":"initial_all",
            "weatheralarm": {
                "Data": {"id":"w1","title":"黄色预警","type":"yellow"},
                "md5":"weather-rev"
            },
            "typhoon": {
                "Data": [{"id":"202601","updateTime":"2026-07-10 12:00:00"}],
                "md5":"typhoon-rev"
            }
        }));
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|(_, batch)| batch.is_ok()));
        assert!(batches.iter().any(|(_, batch)| matches!(batch,
            Ok(batch) if batch.source == "weatheralarm"
                && batch.md5.as_deref() == Some("weather-rev")
                && batch.events.len() == 1)));
    }

    #[test]
    fn maps_documented_tsunami_information_level() {
        let parsed = parse_fanstudio_payload(
            "tsunami",
            &serde_json::json!({
                "code":"t1",
                "warningInfo":{"level":"信息","title":"海啸信息"},
                "shockInfo":{"latitude":1.0,"longitude":2.0}
            }),
            Some("t-revision"),
        );
        assert!(matches!(parsed, Ok(events) if events[0].level == 1));
    }

    #[test]
    fn rejects_malformed_supported_updates() {
        assert!(parse_fanstudio_update(r#"{"type":"update","source":"cenc"}"#).is_err());
        assert!(
            parse_fanstudio_update(
                r#"{"type":"update","source":"cenc","Data":{"unexpected":true}}"#
            )
            .is_err()
        );
        assert!(matches!(
            parse_fanstudio_update(r#"{"type":"update","source":"typhoon","Data":[]}"#),
            Ok(events) if events.is_empty()
        ));
    }

    #[test]
    fn handles_documented_nullable_and_nested_fields() {
        let sa = parse_fanstudio_update(
            r#"{"type":"update","source":"sa","Data":{"id":"sa-1","shockTime":"2025-07-13 20:27:55","latitude":36.17,"longitude":-118.03,"depth":2,"magnitude":null,"placeName":"Olancha"},"md5":"sa-rev"}"#,
        );
        assert!(matches!(sa, Ok(ref events) if events.len() == 1 && events[0].magnitude.is_none()));

        let cmt = parse_fanstudio_update(
            r#"{"type":"update","source":"fssn-cmt","Data":{"eventId":"FSSN2026eegb","shockTime":"2026-03-01 13:44:43","latitude":-21.8973,"longitude":-179.5057,"depth":"612(+/- 8)","allMagnitudes":{"M":6.1}},"md5":"cmt-rev"}"#,
        );
        assert!(matches!(cmt, Ok(ref events) if events.len() == 1
                && events[0].magnitude == Some(6.1)
                && events[0].depth_km == Some(612.0)));

        let no_id = parse_fanstudio_update(
            r#"{"type":"update","source":"guangxi","Data":{"shockTime":"2025-09-08 08:31:45","longitude":109.6,"latitude":22.2,"placeName":"广西钦州市浦北县","magnitude":2.9,"depth":10},"md5":"gx-rev"}"#,
        );
        assert!(matches!(no_id, Ok(ref events) if events.len() == 1
            && events[0].event_id == "2025-09-08 08:31:45:22.200:109.600:2.9"));
    }

    #[test]
    fn preserves_per_typhoon_revision_and_nullable_wind_radii() {
        let events = parse_fanstudio_update(
            r#"{"type":"update","source":"typhoon","Data":[{"id":"202610","name":"无名","latitude":18.1,"longitude":128.2,"power":8,"radius7":null,"radius10":null,"updateTime":"2026-07-05 21:00:00"}],"md5":"envelope-revision"}"#,
        );
        assert!(matches!(events, Ok(ref events) if events.len() == 1
            && events[0].revision == "2026-07-05 21:00:00:envelope-revision"
            && events[0].radius_km.is_none()));
    }

    #[test]
    fn extracts_only_documented_weather_issuer_regions() {
        assert_eq!(
            weather_regions("靖远县气象台继续发布雷雨大风黄色预警信号"),
            vec!["靖远县"]
        );
        assert!(weather_regions("雷雨大风黄色预警").is_empty());
    }
}
