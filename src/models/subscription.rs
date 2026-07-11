use crate::models::DisasterCategory;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_TARGET_FIELD_CHARS: usize = 80;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub destination: NotificationDestination,
    pub targets: Vec<MonitoringTarget>,
    pub alerts: Vec<AlertRule>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NotificationDestination {
    Bark {
        base_url: String,
        device_key: String,
    },
}

impl NotificationDestination {
    pub fn bark_base_url(&self) -> &str {
        match self {
            Self::Bark { base_url, .. } => base_url,
        }
    }

    pub fn bark_device_key(&self) -> &str {
        match self {
            Self::Bark { device_key, .. } => device_key,
        }
    }

    pub fn id(&self) -> DestinationId {
        DestinationId {
            base_url: self.bark_base_url().to_string(),
            device_key: self.bark_device_key().to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DestinationId {
    pub base_url: String,
    pub device_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitoringTarget {
    #[serde(default)]
    pub label: String,
    pub point: GeoPoint,
    #[serde(default)]
    pub region: AdministrativeRegion,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeoPoint {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdministrativeRegion {
    #[serde(default)]
    pub province: String,
    #[serde(default)]
    pub city: String,
    #[serde(default)]
    pub district: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "category", rename_all = "snake_case", deny_unknown_fields)]
pub enum AlertRule {
    EarthquakeWarning {
        sources: SourceSelection,
        estimated_intensity_bands: Vec<IntensityBand>,
    },
    EarthquakeReport {
        sources: SourceSelection,
        min_magnitude: f64,
    },
    WeatherWarning {
        sources: SourceSelection,
        min_severity: u8,
        fallback_radius_km: f64,
    },
    Tsunami {
        sources: SourceSelection,
        min_severity: u8,
    },
    Typhoon {
        sources: SourceSelection,
        max_center_distance_km: f64,
    },
}

impl AlertRule {
    pub fn category(&self) -> DisasterCategory {
        match self {
            Self::EarthquakeWarning { .. } => DisasterCategory::EarthquakeWarning,
            Self::EarthquakeReport { .. } => DisasterCategory::EarthquakeReport,
            Self::WeatherWarning { .. } => DisasterCategory::WeatherWarning,
            Self::Tsunami { .. } => DisasterCategory::Tsunami,
            Self::Typhoon { .. } => DisasterCategory::Typhoon,
        }
    }

    pub fn sources(&self) -> &SourceSelection {
        match self {
            Self::EarthquakeWarning { sources, .. }
            | Self::EarthquakeReport { sources, .. }
            | Self::WeatherWarning { sources, .. }
            | Self::Tsunami { sources, .. }
            | Self::Typhoon { sources, .. } => sources,
        }
    }

    pub fn default_for(category: DisasterCategory) -> Self {
        let sources = SourceSelection::All;
        match category {
            DisasterCategory::EarthquakeWarning => Self::EarthquakeWarning {
                sources,
                estimated_intensity_bands: vec![
                    IntensityBand {
                        min: 1,
                        max: 1,
                        interruption_level: InterruptionLevel::Passive,
                    },
                    IntensityBand {
                        min: 2,
                        max: 2,
                        interruption_level: InterruptionLevel::Active,
                    },
                    IntensityBand {
                        min: 3,
                        max: 7,
                        interruption_level: InterruptionLevel::Critical,
                    },
                ],
            },
            DisasterCategory::EarthquakeReport => Self::EarthquakeReport {
                sources,
                min_magnitude: 4.5,
            },
            DisasterCategory::WeatherWarning => Self::WeatherWarning {
                sources,
                min_severity: 2,
                fallback_radius_km: 100.0,
            },
            DisasterCategory::Tsunami => Self::Tsunami {
                sources,
                min_severity: 2,
            },
            DisasterCategory::Typhoon => Self::Typhoon {
                sources,
                max_center_distance_km: 300.0,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceSelection {
    All,
    Include { ids: Vec<String> },
}

impl SourceSelection {
    pub fn includes(&self, source: &str) -> bool {
        match self {
            Self::All => true,
            Self::Include { ids } => ids.iter().any(|id| id == source),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntensityBand {
    pub min: u8,
    pub max: u8,
    pub interruption_level: InterruptionLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InterruptionLevel {
    Passive,
    Active,
    Critical,
}

impl InterruptionLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passive => "passive",
            Self::Active => "active",
            Self::Critical => "critical",
        }
    }
}

impl Subscription {
    pub fn new(
        destination: NotificationDestination,
        targets: Vec<MonitoringTarget>,
        alerts: Vec<AlertRule>,
    ) -> Self {
        let now = current_timestamp_millis();
        Self {
            destination,
            targets,
            alerts,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn device_key(&self) -> &str {
        self.destination.bark_device_key()
    }

    pub fn bark_base_url(&self) -> &str {
        self.destination.bark_base_url()
    }

    pub fn destination_id(&self) -> DestinationId {
        self.destination.id()
    }

    pub fn alert(&self, category: DisasterCategory) -> Option<&AlertRule> {
        self.alerts
            .iter()
            .find(|alert| alert.category() == category)
    }

    pub fn source_enabled(&self, category: DisasterCategory, source: &str) -> bool {
        self.alert(category)
            .is_some_and(|alert| alert.sources().includes(source))
    }

    pub fn interruption_level_for_intensity(
        &self,
        estimated_intensity: u8,
    ) -> Option<InterruptionLevel> {
        let AlertRule::EarthquakeWarning {
            estimated_intensity_bands,
            ..
        } = self.alert(DisasterCategory::EarthquakeWarning)?
        else {
            return None;
        };
        estimated_intensity_bands
            .iter()
            .find(|band| estimated_intensity >= band.min && estimated_intensity <= band.max)
            .map(|band| band.interruption_level)
    }

    pub fn prepare_for_upsert(&mut self, existing_created_at: Option<i64>) {
        if let Some(created_at) = existing_created_at {
            self.created_at = created_at;
        }
        self.updated_at = current_timestamp_millis();
    }

    pub fn validate(&self) -> Result<(), String> {
        match &self.destination {
            NotificationDestination::Bark {
                base_url,
                device_key,
            } => {
                if !matches!(
                    crate::config::normalize_bark_url(base_url),
                    Ok(normalized) if normalized == *base_url
                ) {
                    return Err("Bark URL 必须是规范化的 HTTP(S) 地址".to_string());
                }
                if device_key.is_empty()
                    || device_key.len() > 64
                    || !device_key.bytes().all(|byte| byte.is_ascii_alphanumeric())
                {
                    return Err("Bark Key 只能包含 1 到 64 个字母或数字".to_string());
                }
            }
        }
        if self.targets.is_empty() || self.targets.len() > 3 {
            return Err("监测目标数量必须在 1 到 3 个之间".to_string());
        }
        if self.alerts.is_empty() || self.alerts.len() > DisasterCategory::ALL.len() {
            return Err("请至少启用一种灾害类别".to_string());
        }

        let mut categories = HashSet::new();
        for target in &self.targets {
            validate_target(target)?;
        }
        for alert in &self.alerts {
            if !categories.insert(alert.category()) {
                return Err(format!("灾害类别 {} 不能重复", alert.category().as_str()));
            }
            validate_alert(alert)?;
        }
        Ok(())
    }
}

fn validate_target(target: &MonitoringTarget) -> Result<(), String> {
    if !crate::utils::distance::validate_coordinates(target.point.latitude, target.point.longitude)
    {
        return Err("监测地点坐标无效".to_string());
    }
    for (label, value) in [
        ("名称", &target.label),
        ("省级行政区", &target.region.province),
        ("城市", &target.region.city),
        ("区县", &target.region.district),
    ] {
        if value.chars().count() > MAX_TARGET_FIELD_CHARS {
            return Err(format!(
                "监测地点{label}最多 {MAX_TARGET_FIELD_CHARS} 个字符"
            ));
        }
    }
    Ok(())
}

fn validate_alert(alert: &AlertRule) -> Result<(), String> {
    validate_sources(alert.category(), alert.sources())?;
    match alert {
        AlertRule::EarthquakeWarning {
            estimated_intensity_bands,
            ..
        } => validate_intensity_bands(estimated_intensity_bands),
        AlertRule::EarthquakeReport { min_magnitude, .. } => {
            if min_magnitude.is_finite() && (0.0..=10.0).contains(min_magnitude) {
                Ok(())
            } else {
                Err("地震信息最低震级必须在 0 到 10 之间".to_string())
            }
        }
        AlertRule::WeatherWarning {
            min_severity,
            fallback_radius_km,
            ..
        } => {
            validate_severity(*min_severity)?;
            if fallback_radius_km.is_finite() && (1.0..=2_000.0).contains(fallback_radius_km) {
                Ok(())
            } else {
                Err("气象预警回退半径必须在 1 到 2000 公里之间".to_string())
            }
        }
        AlertRule::Tsunami { min_severity, .. } => validate_severity(*min_severity),
        AlertRule::Typhoon {
            max_center_distance_km,
            ..
        } => {
            if max_center_distance_km.is_finite()
                && (1.0..=3_000.0).contains(max_center_distance_km)
            {
                Ok(())
            } else {
                Err("台风中心最大距离必须在 1 到 3000 公里之间".to_string())
            }
        }
    }
}

fn validate_sources(category: DisasterCategory, sources: &SourceSelection) -> Result<(), String> {
    let SourceSelection::Include { ids } = sources else {
        return Ok(());
    };
    if ids.is_empty() {
        return Err(format!("{}请至少选择一个来源", category.label()));
    }
    let mut unique = HashSet::new();
    for id in ids {
        if !unique.insert(id) {
            return Err(format!("灾害来源 {id} 不能重复"));
        }
        let Some(source) = crate::source_registry::find(id) else {
            return Err(format!("未知灾害来源 {id}"));
        };
        if source.category != category {
            return Err(format!("灾害来源 {id} 不属于 {}", category.label()));
        }
    }
    Ok(())
}

fn validate_intensity_bands(bands: &[IntensityBand]) -> Result<(), String> {
    if bands.is_empty() || bands.len() > 3 {
        return Err("地震预警烈度规则数量必须在 1 到 3 条之间".to_string());
    }
    let mut levels = HashSet::new();
    let mut covered = HashSet::new();
    for band in bands {
        if band.min > band.max || band.max > 7 {
            return Err("地震预警烈度范围必须在 0 到 7 之间".to_string());
        }
        if !levels.insert(band.interruption_level) {
            return Err("每个 Bark 中断级别只能配置一条烈度规则".to_string());
        }
        if band.interruption_level == InterruptionLevel::Critical && band.max != 7 {
            return Err("critical 烈度规则必须覆盖到烈度 7".to_string());
        }
        for intensity in band.min..=band.max {
            if !covered.insert(intensity) {
                return Err("地震预警烈度范围不能重叠".to_string());
            }
        }
    }
    Ok(())
}

fn validate_severity(severity: u8) -> Result<(), String> {
    if (1..=4).contains(&severity) {
        Ok(())
    } else {
        Err("灾害最低严重度必须在 1 到 4 之间".to_string())
    }
}

fn current_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeRequest {
    pub destination: NotificationDestination,
    pub targets: Vec<MonitoringTarget>,
    pub alerts: Vec<AlertRule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnsubscribeRequest {
    pub destination: NotificationDestination,
}

pub fn mask_device_key(value: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn subscription(alerts: Vec<AlertRule>) -> Subscription {
        Subscription::new(
            NotificationDestination::Bark {
                base_url: "https://api.day.app".to_string(),
                device_key: "abc123".to_string(),
            },
            vec![MonitoringTarget {
                label: "home".to_string(),
                point: GeoPoint {
                    latitude: 35.0,
                    longitude: 105.0,
                },
                region: AdministrativeRegion::default(),
            }],
            alerts,
        )
    }

    #[test]
    fn intensity_gaps_suppress_earthquake_warning() {
        let subscription = subscription(vec![AlertRule::EarthquakeWarning {
            sources: SourceSelection::All,
            estimated_intensity_bands: vec![IntensityBand {
                min: 3,
                max: 7,
                interruption_level: InterruptionLevel::Critical,
            }],
        }]);

        assert_eq!(subscription.interruption_level_for_intensity(2), None);
        assert_eq!(
            subscription.interruption_level_for_intensity(3),
            Some(InterruptionLevel::Critical)
        );
    }

    #[test]
    fn rejects_source_from_another_category() {
        let subscription = subscription(vec![AlertRule::Tsunami {
            sources: SourceSelection::Include {
                ids: vec!["fanstudio.weatheralarm".to_string()],
            },
            min_severity: 2,
        }]);

        assert!(subscription.validate().is_err());
    }

    #[test]
    fn rejects_invalid_target_coordinates() {
        let mut subscription = subscription(vec![AlertRule::default_for(
            DisasterCategory::WeatherWarning,
        )]);
        subscription.targets[0].point.latitude = 91.0;

        assert!(subscription.validate().is_err());
    }

    #[test]
    fn deserializes_the_complete_subscribe_contract() -> anyhow::Result<()> {
        let request = serde_json::from_value::<SubscribeRequest>(serde_json::json!({
            "destination": {
                "type": "bark",
                "base_url": "https://api.day.app",
                "device_key": "abc123"
            },
            "targets": [{
                "label": "home",
                "point": { "latitude": 35.0, "longitude": 105.0 },
                "region": { "province": "四川省", "city": "成都市", "district": "" }
            }],
            "alerts": [
                {
                    "category": "earthquake_warning",
                    "sources": { "mode": "all" },
                    "estimated_intensity_bands": [{
                        "min": 3,
                        "max": 7,
                        "interruption_level": "critical"
                    }]
                },
                {
                    "category": "earthquake_report",
                    "sources": { "mode": "include", "ids": ["fanstudio.cenc"] },
                    "min_magnitude": 4.5
                },
                {
                    "category": "weather_warning",
                    "sources": { "mode": "all" },
                    "min_severity": 2,
                    "fallback_radius_km": 100
                },
                {
                    "category": "tsunami",
                    "sources": { "mode": "all" },
                    "min_severity": 2
                },
                {
                    "category": "typhoon",
                    "sources": { "mode": "all" },
                    "max_center_distance_km": 300
                }
            ]
        }))?;
        let subscription = Subscription::new(request.destination, request.targets, request.alerts);

        anyhow::ensure!(subscription.validate().is_ok());
        Ok(())
    }

    #[test]
    fn rejects_the_removed_subscription_contract() {
        let old_request = serde_json::json!({
            "bark_id": "abc123",
            "bark_url": "https://api.day.app",
            "locations": [],
            "notify_bands": [],
            "disaster_rules": {},
            "source_overrides": {}
        });

        assert!(serde_json::from_value::<SubscribeRequest>(old_request).is_err());
    }

    #[test]
    fn rejects_unknown_subscribe_fields() {
        let request = serde_json::json!({
            "destination": {
                "type": "bark",
                "base_url": "https://api.day.app",
                "device_key": "abc123",
                "legacy_option": true
            },
            "targets": [],
            "alerts": []
        });

        assert!(serde_json::from_value::<SubscribeRequest>(request).is_err());
    }

    #[test]
    fn rejects_duplicate_categories_and_empty_source_allowlists() {
        let duplicate = subscription(vec![
            AlertRule::default_for(DisasterCategory::Tsunami),
            AlertRule::default_for(DisasterCategory::Tsunami),
        ]);
        let empty_sources = subscription(vec![AlertRule::Tsunami {
            sources: SourceSelection::Include { ids: Vec::new() },
            min_severity: 2,
        }]);

        assert!(duplicate.validate().is_err());
        assert!(empty_sources.validate().is_err());
    }

    #[test]
    fn rejects_noncanonical_destination_identity() {
        let mut trailing_slash = subscription(vec![AlertRule::default_for(
            DisasterCategory::WeatherWarning,
        )]);
        trailing_slash.destination = NotificationDestination::Bark {
            base_url: "https://api.day.app/".to_string(),
            device_key: "abc123".to_string(),
        };
        let mut invalid_key = trailing_slash.clone();
        invalid_key.destination = NotificationDestination::Bark {
            base_url: "https://api.day.app".to_string(),
            device_key: "invalid-key".to_string(),
        };

        assert!(trailing_slash.validate().is_err());
        assert!(invalid_key.validate().is_err());
    }

    #[test]
    fn unsubscribe_requires_the_complete_destination() {
        assert!(
            serde_json::from_value::<UnsubscribeRequest>(serde_json::json!({
                "destination": {
                    "type": "bark",
                    "base_url": "https://api.day.app",
                    "device_key": "abc123"
                }
            }))
            .is_ok()
        );
        assert!(
            serde_json::from_value::<UnsubscribeRequest>(serde_json::json!({
                "device_key": "abc123"
            }))
            .is_err()
        );
    }
}
