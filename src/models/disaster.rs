use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone)]
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
        format!("{}:{}", self.source, self.event_id)
    }
}
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderChannel {
    Wolfx,
    FanStudio,
}

impl ProviderChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Wolfx => "wolfx",
            Self::FanStudio => "fanstudio",
        }
    }
}

impl fmt::Display for ProviderChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
