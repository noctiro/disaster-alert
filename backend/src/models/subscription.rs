use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub bark_id: String,
    #[serde(default)]
    pub bark_url: String,
    pub locations: Vec<SubscriptionLocation>,
    #[serde(default)]
    pub notify_bands: Vec<NotificationBand>,
    #[serde(default)]
    pub disaster_rules: DisasterRules,
    #[serde(default)]
    pub source_overrides: HashMap<String, bool>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionLocation {
    #[serde(default)]
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub province: String,
    #[serde(default)]
    pub city: String,
    #[serde(default)]
    pub district: String,
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
    pub fn new(bark_id: String, locations: Vec<SubscriptionLocation>) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);

        Self {
            bark_id,
            bark_url: String::new(),
            locations,
            notify_bands: Vec::new(),
            disaster_rules: DisasterRules::default(),
            source_overrides: HashMap::new(),
            created_at,
        }
    }

    pub fn normalized_locations(&self) -> Vec<SubscriptionLocation> {
        self.locations
            .iter()
            .filter(|item| valid_coordinate(item.latitude, item.longitude))
            .take(3)
            .cloned()
            .collect()
    }

    pub fn for_each_normalized_location<F>(&self, mut visitor: F)
    where
        F: FnMut(&str, f64, f64),
    {
        for location in self
            .locations
            .iter()
            .filter(|location| valid_coordinate(location.latitude, location.longitude))
            .take(3)
        {
            visitor(&location.name, location.latitude, location.longitude);
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisasterRules {
    pub earthquake_warning: bool,
    pub earthquake_report: bool,
    pub weather_warning: bool,
    pub tsunami: bool,
    pub typhoon: bool,
    pub min_earthquake_magnitude: f64,
    pub weather_radius_km: f64,
    pub min_weather_level: u8,
    pub min_tsunami_level: u8,
    pub typhoon_radius_km: f64,
}

impl Default for DisasterRules {
    fn default() -> Self {
        Self {
            earthquake_warning: true,
            earthquake_report: true,
            weather_warning: true,
            tsunami: true,
            typhoon: true,
            min_earthquake_magnitude: 4.5,
            weather_radius_km: 100.0,
            min_weather_level: 2,
            min_tsunami_level: 2,
            typhoon_radius_km: 300.0,
        }
    }
}

impl DisasterRules {
    pub fn validate(&self) -> Result<(), String> {
        if !self.min_earthquake_magnitude.is_finite()
            || !(0.0..=10.0).contains(&self.min_earthquake_magnitude)
        {
            return Err("地震信息最低震级必须在 0 到 10 之间".to_string());
        }
        if !self.weather_radius_km.is_finite() || !(1.0..=2_000.0).contains(&self.weather_radius_km)
        {
            return Err("气象预警半径必须在 1 到 2000 公里之间".to_string());
        }
        if !self.typhoon_radius_km.is_finite() || !(1.0..=3_000.0).contains(&self.typhoon_radius_km)
        {
            return Err("台风通知半径必须在 1 到 3000 公里之间".to_string());
        }
        if !(1..=4).contains(&self.min_weather_level) || !(1..=4).contains(&self.min_tsunami_level)
        {
            return Err("灾害最低级别必须在 1 到 4 之间".to_string());
        }
        Ok(())
    }
}

pub fn normalize_bark_level(level: &str) -> String {
    level.trim().to_ascii_lowercase()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscribeRequest {
    pub bark_id: String,
    #[serde(default)]
    pub bark_url: String,
    pub locations: Vec<SubscriptionLocation>,
    #[serde(default)]
    pub notify_bands: Vec<NotificationBand>,
    #[serde(default)]
    pub disaster_rules: DisasterRules,
    #[serde(default)]
    pub source_overrides: HashMap<String, bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn level_for_intensity_uses_lowest_matching_band_min() {
        let mut subscription = Subscription::new("abc123".to_string(), Vec::new());
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
