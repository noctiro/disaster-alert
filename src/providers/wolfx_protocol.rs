use serde::{Deserialize, Serialize};

/// JMA earthquake warning payload. Time fields use UTC+9.
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
    pub depth: Option<f64>,
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

#[derive(Debug, Clone)]
pub enum EarthquakeData {
    Jma(JmaEew),
    Sichuan(SichuanEew),
    Cenc(CencEew),
    Fujian(FujianEew),
    Chongqing(ChongqingEew),
}

impl EarthquakeData {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let msg: WebSocketMessage = serde_json::from_str(json)?;

        match msg.message_type.as_str() {
            "jma_eew" => {
                let data: JmaEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::Jma(data))
            }
            "sc_eew" => {
                let data: SichuanEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::Sichuan(data))
            }
            "cenc_eew" => {
                let data: CencEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::Cenc(data))
            }
            "fj_eew" => {
                let data: FujianEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::Fujian(data))
            }
            "cq_eew" => {
                let data: ChongqingEew = serde_json::from_str(json)?;
                Ok(EarthquakeData::Chongqing(data))
            }
            _ => Err(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported Wolfx source: {}", msg.message_type),
            ))),
        }
    }

    pub fn to_common_info(&self) -> CommonEarthquakeInfo {
        match self {
            EarthquakeData::Jma(data) => CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: Some(data.depth),
                max_intensity: data.max_intensity.clone(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "jma_eew".to_string(),
            },
            EarthquakeData::Sichuan(data) => CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth,
                max_intensity: data.max_intensity.to_string(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "sc_eew".to_string(),
            },
            EarthquakeData::Cenc(data) => CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth,
                max_intensity: data.max_intensity.to_string(),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "cenc_eew".to_string(),
            },
            EarthquakeData::Fujian(data) => CommonEarthquakeInfo {
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
            },
            EarthquakeData::Chongqing(data) => CommonEarthquakeInfo {
                event_id: data.event_id.clone(),
                report_num: data.report_num,
                final_report: data.is_final,
                cancel: data.cancel,
                training: data.training,
                latitude: data.latitude,
                longitude: data.longitude,
                magnitude: data.magnitude,
                depth: data.depth,
                max_intensity: data
                    .max_intensity
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "未知".to_string()),
                region: data.hypocenter.clone(),
                origin_time: data.origin_time.clone(),
                source_type: "cq_eew".to_string(),
            },
        }
    }

    pub fn parse_to_common_info(json: &str) -> Result<CommonEarthquakeInfo, serde_json::Error> {
        Ok(Self::from_json(json)?.to_common_info())
    }
}

#[derive(Debug, Clone)]
pub struct CommonEarthquakeInfo {
    pub event_id: String,
    pub report_num: u32,
    pub latitude: f64,
    pub longitude: f64,
    pub magnitude: f64,
    pub depth: Option<f64>,
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

pub fn parse(json: &str) -> Result<CommonEarthquakeInfo, serde_json::Error> {
    EarthquakeData::parse_to_common_info(json)
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
                Some(100.0),
            ),
            (
                r#"{"type":"sc_eew","EventID":"sc-1","ReportNum":1,"OriginTime":"2026-07-09 07:44:12","HypoCenter":"四川宜宾市高县","Latitude":28.509,"Longitude":104.687,"Magunitude":5.1,"Depth":null,"MaxIntensity":7.1}"#,
                "sc_eew",
                1,
                None,
            ),
            (
                r#"{"type":"cenc_eew","EventID":"cenc-1","ReportNum":2,"OriginTime":"2026-07-09 07:44:12","HypoCenter":"四川宜宾市高县","Latitude":28.509,"Longitude":104.687,"Magnitude":5.1,"Depth":null,"MaxIntensity":7.1}"#,
                "cenc_eew",
                2,
                None,
            ),
            (
                r#"{"type":"fj_eew","EventID":"fj-1","ReportNum":1,"OriginTime":"2026-05-14 04:45:25","HypoCenter":"江西赣州市寻乌县","Latitude":25.0,"Longitude":115.69,"Magunitude":3.4}"#,
                "fj_eew",
                1,
                None,
            ),
            (
                r#"{"type":"cq_eew","EventID":"cq-1","ReportNum":3,"OriginTime":"2026-07-09 07:44:12","HypoCenter":"四川宜宾市高县","Latitude":28.509,"Longitude":104.687,"Magnitude":5.1,"Depth":null,"MaxIntensity":7.1}"#,
                "cq_eew",
                3,
                None,
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
}
