use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub bark_id: String,
    #[serde(default, alias = "bark_server")]
    pub bark_url: String,
    #[serde(default)]
    pub location_name: String,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub locations: Vec<SubscriptionLocation>,
    #[serde(default)]
    pub notify_bands: Vec<NotificationBand>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionLocation {
    #[serde(default)]
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationBand {
    pub min: u8,
    pub max: u8,
    pub level: String,
    #[serde(default)]
    pub label: String,
}

impl Subscription {
    pub fn new(bark_id: String, latitude: f64, longitude: f64) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);

        Self {
            bark_id,
            bark_url: String::new(),
            location_name: String::new(),
            latitude,
            longitude,
            locations: Vec::new(),
            notify_bands: Vec::new(),
            created_at,
        }
    }

    pub fn normalized_locations(&self) -> Vec<SubscriptionLocation> {
        let mut locations = self
            .locations
            .iter()
            .filter(|item| valid_coordinate(item.latitude, item.longitude))
            .cloned()
            .collect::<Vec<_>>();
        if locations.is_empty() && valid_coordinate(self.latitude, self.longitude) {
            locations.push(SubscriptionLocation {
                name: self.location_name.clone(),
                latitude: self.latitude,
                longitude: self.longitude,
            });
        }
        locations.truncate(3);
        locations
    }

    pub fn for_each_normalized_location<F>(&self, mut visitor: F)
    where
        F: FnMut(&str, f64, f64),
    {
        let mut found = false;
        for location in self
            .locations
            .iter()
            .filter(|location| valid_coordinate(location.latitude, location.longitude))
            .take(3)
        {
            found = true;
            visitor(&location.name, location.latitude, location.longitude);
        }
        if !found && valid_coordinate(self.latitude, self.longitude) {
            visitor(&self.location_name, self.latitude, self.longitude);
        }
    }

    pub fn normalize_for_storage(&mut self) -> Result<(), String> {
        for band in &mut self.notify_bands {
            band.level = normalize_bark_level(&band.level);
            if !validate_bark_level(&band.level) {
                return Err("通知级别必须是 passive、active 或 critical".to_string());
            }
        }
        Ok(())
    }

    pub fn level_for_intensity(&self, estimated_intensity: u8) -> Option<&str> {
        let mut selected: Option<&NotificationBand> = None;
        for band in &self.notify_bands {
            if validate_bark_level(&band.level)
                && estimated_intensity >= band.min
                && estimated_intensity <= band.max
                && selected
                    .map(|current| band.min < current.min)
                    .unwrap_or(true)
            {
                selected = Some(band);
            }
        }
        selected.map(|band| band.level.as_str())
    }
}

pub fn normalize_bark_level(level: &str) -> String {
    level.trim().to_ascii_lowercase()
}

#[derive(Debug, Deserialize)]
pub struct SubscribeRequest {
    pub bark_id: String,
    #[serde(default)]
    pub bark_url: String,
    #[serde(default)]
    pub location_name: String,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub locations: Vec<SubscriptionLocation>,
    #[serde(default)]
    pub notify_bands: Vec<NotificationBand>,
}

#[derive(Debug, Deserialize)]
pub struct UnsubscribeRequest {
    pub bark_id: String,
}

pub fn validate_bark_level(level: &str) -> bool {
    matches!(level, "passive" | "active" | "critical")
}

pub fn valid_coordinate(lat: f64, lon: f64) -> bool {
    lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
}

pub fn mask_bark_id(value: &str) -> String {
    let value = value.trim();
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= 6 {
        "***".to_string()
    } else {
        let prefix = chars.iter().take(3).collect::<String>();
        let suffix = chars
            .iter()
            .skip(chars.len().saturating_sub(3))
            .collect::<String>();
        format!("{}***{}", prefix, suffix)
    }
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn success(message: impl Into<String>, data: Option<T>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            data: None,
        }
    }
}

/// JMA（日本气象厅）地震预警数据，时间字段为 UTC+9
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JmaEew {
    #[serde(rename = "type")]
    pub alert_type: String,
    #[serde(rename = "EventID")]
    pub event_id: String,
    #[serde(rename = "ReportNum", alias = "Serial", default)]
    pub report_num: u32,
    #[serde(rename = "AnnouncedTime")]
    pub announced_time: String,
    #[serde(rename = "OriginTime")]
    pub origin_time: String,
    #[serde(rename = "Hypocenter")]
    pub hypocenter: String,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    // 上游字段拼写为 Magunitude，反序列化时必须保留这个拼写
    #[serde(rename = "Magunitude")]
    pub magnitude: f64,
    #[serde(rename = "Depth")]
    pub depth: f64,
    #[serde(rename = "MaxIntensity")]
    pub max_intensity: String,
    #[serde(rename = "isFinal", default)]
    pub is_final: bool,
    #[serde(rename = "Cancel", alias = "isCancel", default)]
    pub cancel: bool,
    #[serde(
        rename = "isTraining",
        alias = "is_training",
        alias = "Training",
        default
    )]
    pub training: bool,
}

/// 四川地震局预警数据，时间字段为 UTC+8
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SichuanEew {
    #[serde(rename = "type")]
    pub alert_type: String,
    #[serde(rename = "EventID")]
    pub event_id: String,
    #[serde(rename = "ReportNum", alias = "Serial", default)]
    pub report_num: u32,
    #[serde(rename = "OriginTime")]
    pub origin_time: String,
    #[serde(rename = "HypoCenter")]
    pub hypocenter: String,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    // 上游字段拼写为 Magunitude，反序列化时必须保留这个拼写
    #[serde(rename = "Magunitude")]
    pub magnitude: f64,
    #[serde(rename = "Depth", default)]
    pub depth: Option<f64>,
    #[serde(rename = "MaxIntensity")]
    pub max_intensity: f64,
    #[serde(rename = "isFinal", default)]
    pub is_final: bool,
    #[serde(rename = "Cancel", alias = "isCancel", default)]
    pub cancel: bool,
    #[serde(
        rename = "isTraining",
        alias = "is_training",
        alias = "Training",
        default
    )]
    pub training: bool,
}

/// 中国地震台网中心预警数据，时间字段为 UTC+8
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CencEew {
    #[serde(rename = "type")]
    pub alert_type: String,
    #[serde(rename = "EventID")]
    pub event_id: String,
    #[serde(rename = "ReportNum", alias = "Serial", default)]
    pub report_num: u32,
    #[serde(rename = "OriginTime")]
    pub origin_time: String,
    #[serde(rename = "HypoCenter")]
    pub hypocenter: String,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    #[serde(rename = "Magnitude")]
    pub magnitude: f64,
    #[serde(rename = "Depth", default)]
    pub depth: Option<f64>,
    #[serde(rename = "MaxIntensity")]
    pub max_intensity: f64,
    #[serde(rename = "isFinal", default)]
    pub is_final: bool,
    #[serde(rename = "Cancel", alias = "isCancel", default)]
    pub cancel: bool,
    #[serde(
        rename = "isTraining",
        alias = "is_training",
        alias = "Training",
        default
    )]
    pub training: bool,
}

/// 福建地震局预警数据，时间字段为 UTC+8
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FujianEew {
    #[serde(rename = "type")]
    pub alert_type: String,
    #[serde(rename = "EventID")]
    pub event_id: String,
    #[serde(rename = "ReportNum", alias = "Serial", default)]
    pub report_num: u32,
    #[serde(rename = "OriginTime")]
    pub origin_time: String,
    #[serde(rename = "HypoCenter")]
    pub hypocenter: String,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    // 上游字段拼写为 Magunitude，反序列化时必须保留这个拼写
    #[serde(rename = "Magunitude")]
    pub magnitude: f64,
    #[serde(rename = "Depth", default)]
    pub depth: f64,
    #[serde(rename = "isFinal", default)]
    pub is_final: bool,
    #[serde(rename = "Cancel", alias = "isCancel", default)]
    pub cancel: bool,
    #[serde(
        rename = "isTraining",
        alias = "is_training",
        alias = "Training",
        default
    )]
    pub training: bool,
}

/// 重庆市地震局预警数据，字段与 CENC 类似但震级字段为 Magnitude。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChongqingEew {
    #[serde(rename = "type")]
    pub alert_type: String,
    #[serde(rename = "EventID")]
    pub event_id: String,
    #[serde(rename = "ReportNum", alias = "Serial", default)]
    pub report_num: u32,
    #[serde(rename = "OriginTime")]
    pub origin_time: String,
    #[serde(rename = "HypoCenter")]
    pub hypocenter: String,
    #[serde(rename = "Latitude")]
    pub latitude: f64,
    #[serde(rename = "Longitude")]
    pub longitude: f64,
    #[serde(rename = "Magnitude")]
    pub magnitude: f64,
    #[serde(rename = "Depth", default)]
    pub depth: Option<f64>,
    #[serde(rename = "MaxIntensity", default)]
    pub max_intensity: Option<f64>,
    #[serde(rename = "isFinal", default)]
    pub is_final: bool,
    #[serde(rename = "Cancel", alias = "isCancel", default)]
    pub cancel: bool,
    #[serde(
        rename = "isTraining",
        alias = "is_training",
        alias = "Training",
        default
    )]
    pub training: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnknownEarthquakeData {
    #[serde(rename = "type")]
    pub alert_type: String,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

#[derive(Debug, Clone)]
pub enum EarthquakeData {
    JmaEew(JmaEew),
    SichuanEew(SichuanEew),
    CencEew(CencEew),
    FujianEew(FujianEew),
    ChongqingEew(ChongqingEew),
    Unknown(UnknownEarthquakeData),
}

impl EarthquakeData {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let msg: WebSocketMessage = serde_json::from_str(json)?;

        match msg.message_type.as_str() {
            "jma_eew" => {
                let data: JmaEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::JmaEew(data))
            }
            "sc_eew" => {
                let data: SichuanEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::SichuanEew(data))
            }
            "cenc_eew" => {
                let data: CencEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::CencEew(data))
            }
            "fj_eew" => {
                let data: FujianEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::FujianEew(data))
            }
            "cq_eew" => {
                let data: ChongqingEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::ChongqingEew(data))
            }
            _ => {
                tracing::warn!(
                    event = "eew.unknown_source",
                    message_type = %msg.message_type,
                    "eew.unknown_source"
                );
                tracing::debug!(
                    event = "eew.unknown_source_payload",
                    message_type = %msg.message_type,
                    payload = %json,
                    "eew.unknown_source_payload"
                );

                let data: UnknownEarthquakeData = serde_json::from_str(json)?;
                Ok(EarthquakeData::Unknown(data))
            }
        }
    }

    pub fn to_common_info(&self) -> Option<CommonEarthquakeInfo> {
        match self {
            EarthquakeData::JmaEew(data) => Some(CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth,
                max_intensity: data.max_intensity.clone(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "jma_eew".to_string(),
            }),
            EarthquakeData::SichuanEew(data) => Some(CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth.unwrap_or(0.0),
                max_intensity: data.max_intensity.to_string(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "sc_eew".to_string(),
            }),
            EarthquakeData::CencEew(data) => Some(CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth.unwrap_or(0.0),
                max_intensity: data.max_intensity.to_string(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "cenc_eew".to_string(),
            }),
            EarthquakeData::FujianEew(data) => Some(CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth,
                max_intensity: "未知".to_string(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "fj_eew".to_string(),
            }),
            EarthquakeData::ChongqingEew(data) => Some(CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth.unwrap_or(0.0),
                max_intensity: data
                    .max_intensity
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "未知".to_string()),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "cq_eew".to_string(),
            }),
            EarthquakeData::Unknown(data) => {
                // fallback 只接受推送所需的最小字段集合，避免误推结构不明确的数据
                let latitude = json_f64(&data.data, &["Latitude"])?;
                let longitude = json_f64(&data.data, &["Longitude"])?;
                let magnitude = data
                    .data
                    .get("Magnitude")
                    .or_else(|| data.data.get("Magunitude"));
                let magnitude = magnitude.and_then(json_value_f64)?;

                let depth = json_f64(&data.data, &["Depth"]).unwrap_or(0.0);

                let max_intensity = data
                    .data
                    .get("MaxIntensity")
                    .and_then(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .or_else(|| v.as_i64().map(|i| i.to_string()))
                    })
                    .unwrap_or_else(|| "未知".to_string());

                let region = data
                    .data
                    .get("HypoCenter")
                    .or_else(|| data.data.get("Hypocenter"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let origin_time = data
                    .data
                    .get("OriginTime")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let event_id = data
                    .data
                    .get("EventID")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let report_num = json_u32(&data.data, &["ReportNum", "Serial"]);
                let final_report = json_bool(&data.data, &["isFinal", "Final"]);
                let cancel = json_bool(&data.data, &["Cancel", "isCancel"]);
                let training = json_bool(&data.data, &["isTraining", "is_training", "Training"]);

                tracing::info!(
                    event = "eew.unknown_source_normalized",
                    message_type = %data.alert_type,
                    magnitude,
                    latitude,
                    longitude,
                    "eew.unknown_source_normalized"
                );

                Some(CommonEarthquakeInfo {
                    event_id,
                    report_num,
                    final_report,
                    cancel,
                    training,
                    latitude,
                    longitude,
                    magnitude,
                    depth,
                    max_intensity,
                    region,
                    origin_time,
                    source_type: data.alert_type.clone(),
                })
            }
        }
    }

    pub fn parse_to_common_info(json: &str) -> Result<CommonEarthquakeInfo, serde_json::Error> {
        let earthquake_data = Self::from_json(json)?;
        earthquake_data.to_common_info().ok_or_else(|| {
            serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "无法从未知数据源提取通用信息",
            ))
        })
    }
}

fn json_u32(data: &serde_json::Value, keys: &[&str]) -> u32 {
    keys.iter()
        .find_map(|key| data.get(*key))
        .and_then(|value| {
            value
                .as_u64()
                .and_then(|number| u32::try_from(number).ok())
                .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        })
        .unwrap_or(0)
}

fn json_f64(data: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| data.get(*key))
        .and_then(json_value_f64)
}

fn json_value_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        .filter(|number| number.is_finite())
}

fn json_bool(data: &serde_json::Value, keys: &[&str]) -> bool {
    keys.iter()
        .find_map(|key| data.get(*key))
        .and_then(|value| {
            value.as_bool().or_else(|| {
                value.as_str().map(|text| {
                    matches!(
                        text.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes"
                    )
                })
            })
        })
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
pub struct CommonEarthquakeInfo {
    pub event_id: String,
    pub report_num: u32,
    pub latitude: f64,
    pub longitude: f64,
    pub magnitude: f64,
    pub depth: f64,
    pub max_intensity: String,
    pub region: String,
    pub origin_time: String,
    pub source_type: String,
    pub final_report: bool,
    pub cancel: bool,
    pub training: bool,
}

#[derive(Debug, Deserialize)]
pub struct WebSocketMessage {
    #[serde(rename = "type")]
    pub message_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_documented_wolfx_eew_sources() {
        let cases = [
            (
                r#"{"type":"jma_eew","EventID":"jma-1","Serial":6,"AnnouncedTime":"2026/07/10 01:22:52","OriginTime":"2026/07/10 01:21:43","Hypocenter":"宮古島北西沖","Latitude":25.5,"Longitude":125.0,"Magunitude":4.4,"Depth":100,"MaxIntensity":"2","isTraining":true,"isFinal":true,"isCancel":true}"#,
                "jma_eew",
                6,
                100.0,
            ),
            (
                r#"{"type":"sc_eew","EventID":"sc-1","ReportNum":1,"OriginTime":"2026-07-09 07:44:12","HypoCenter":"四川宜宾市高县","Latitude":28.509,"Longitude":104.687,"Magunitude":5.1,"Depth":null,"MaxIntensity":7.1}"#,
                "sc_eew",
                1,
                0.0,
            ),
            (
                r#"{"type":"cenc_eew","EventID":"cenc-1","ReportNum":2,"OriginTime":"2026-07-09 07:44:12","HypoCenter":"四川宜宾市高县","Latitude":28.509,"Longitude":104.687,"Magnitude":5.1,"Depth":null,"MaxIntensity":7.1}"#,
                "cenc_eew",
                2,
                0.0,
            ),
            (
                r#"{"type":"fj_eew","EventID":"fj-1","ReportNum":1,"OriginTime":"2026-05-14 04:45:25","HypoCenter":"江西赣州市寻乌县","Latitude":25.0,"Longitude":115.69,"Magunitude":3.4}"#,
                "fj_eew",
                1,
                0.0,
            ),
            (
                r#"{"type":"cq_eew","EventID":"cq-1","ReportNum":3,"OriginTime":"2026-07-09 07:44:12","HypoCenter":"四川宜宾市高县","Latitude":28.509,"Longitude":104.687,"Magnitude":5.1,"Depth":null,"MaxIntensity":7.1}"#,
                "cq_eew",
                3,
                0.0,
            ),
        ];

        for (json, source_type, report_num, depth) in cases {
            let parsed = EarthquakeData::parse_to_common_info(json);
            assert!(parsed.is_ok(), "failed to parse {source_type}: {parsed:?}");
            if let Ok(info) = parsed {
                assert_eq!(info.source_type, source_type);
                assert_eq!(info.report_num, report_num);
                assert_eq!(info.depth, depth);
            }
        }

        let parsed = EarthquakeData::parse_to_common_info(cases[0].0);
        assert!(parsed.is_ok(), "failed to parse JMA flags: {parsed:?}");
        if let Ok(jma) = parsed {
            assert!(jma.training);
            assert!(jma.final_report);
            assert!(jma.cancel);
        }
    }

    #[test]
    fn normalizes_future_eew_sources_with_documented_field_shapes() {
        let parsed = EarthquakeData::parse_to_common_info(
            r#"{"type":"future_eew","EventID":"future-1","ReportNum":"4","OriginTime":"2026-07-10 01:21:43","HypoCenter":"测试震源","Latitude":"25.5","Longitude":"125.0","Magnitude":"4.4","Depth":null,"MaxIntensity":3,"isTraining":true}"#,
        );

        assert!(parsed.is_ok(), "failed to parse future source: {parsed:?}");
        if let Ok(info) = parsed {
            assert_eq!(info.source_type, "future_eew");
            assert_eq!(info.report_num, 4);
            assert_eq!(info.depth, 0.0);
            assert_eq!(info.max_intensity, "3");
            assert!(info.training);
        }
    }

    #[test]
    fn level_for_intensity_uses_lowest_matching_band_min() {
        let mut subscription = Subscription::new("abc123".to_string(), 0.0, 0.0);
        subscription.notify_bands = vec![
            NotificationBand {
                min: 4,
                max: 6,
                level: "critical".to_string(),
                label: String::new(),
            },
            NotificationBand {
                min: 2,
                max: 5,
                level: "active".to_string(),
                label: String::new(),
            },
        ];

        assert_eq!(subscription.level_for_intensity(5), Some("active"));
    }
}
