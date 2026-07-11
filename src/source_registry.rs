use crate::models::{AlertRule, DisasterCategory, ProviderChannel};
use serde::Serialize;

#[derive(Debug, Clone, Copy)]
pub struct SourceDefinition {
    pub id: &'static str,
    pub provider_key: &'static str,
    pub channel: ProviderChannel,
    pub category: DisasterCategory,
    pub group_id: &'static str,
    pub group_label: &'static str,
    pub label: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceGroup {
    pub id: &'static str,
    pub label: &'static str,
    pub sources: Vec<SourceOption>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CategoryOption {
    pub id: &'static str,
    pub label: &'static str,
    pub source_groups: Vec<SourceGroup>,
    pub default_alert: AlertRule,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SourceOption {
    pub id: &'static str,
    pub label: &'static str,
}

macro_rules! source {
    ($id:literal, $key:literal, $channel:ident, $category:ident, $group:literal, $group_label:literal, $label:literal) => {
        SourceDefinition {
            id: $id,
            provider_key: $key,
            channel: ProviderChannel::$channel,
            category: DisasterCategory::$category,
            group_id: $group,
            group_label: $group_label,
            label: $label,
        }
    };
}

pub const SOURCES: &[SourceDefinition] = &[
    source!(
        "wolfx.jma_eew",
        "jma_eew",
        Wolfx,
        EarthquakeWarning,
        "wolfx-earthquake-warning",
        "Wolfx 地震预警",
        "Wolfx 日本气象厅"
    ),
    source!(
        "wolfx.sc_eew",
        "sc_eew",
        Wolfx,
        EarthquakeWarning,
        "wolfx-earthquake-warning",
        "Wolfx 地震预警",
        "Wolfx 四川地震局"
    ),
    source!(
        "wolfx.cenc_eew",
        "cenc_eew",
        Wolfx,
        EarthquakeWarning,
        "wolfx-earthquake-warning",
        "Wolfx 地震预警",
        "Wolfx 中国地震台网"
    ),
    source!(
        "wolfx.fj_eew",
        "fj_eew",
        Wolfx,
        EarthquakeWarning,
        "wolfx-earthquake-warning",
        "Wolfx 地震预警",
        "Wolfx 福建地震局"
    ),
    source!(
        "wolfx.cq_eew",
        "cq_eew",
        Wolfx,
        EarthquakeWarning,
        "wolfx-earthquake-warning",
        "Wolfx 地震预警",
        "Wolfx 重庆地震局"
    ),
    source!(
        "fanstudio.cea",
        "cea",
        FanStudio,
        EarthquakeWarning,
        "fanstudio-earthquake-warning",
        "FAN Studio 地震预警",
        "中国地震预警网"
    ),
    source!(
        "fanstudio.cea-pr",
        "cea-pr",
        FanStudio,
        EarthquakeWarning,
        "fanstudio-earthquake-warning",
        "FAN Studio 地震预警",
        "中国地震预警网省级网"
    ),
    source!(
        "fanstudio.cwa-eew",
        "cwa-eew",
        FanStudio,
        EarthquakeWarning,
        "fanstudio-earthquake-warning",
        "FAN Studio 地震预警",
        "台湾气象署预警"
    ),
    source!(
        "fanstudio.jma",
        "jma",
        FanStudio,
        EarthquakeWarning,
        "fanstudio-earthquake-warning",
        "FAN Studio 地震预警",
        "日本气象厅预警"
    ),
    source!(
        "fanstudio.sa",
        "sa",
        FanStudio,
        EarthquakeWarning,
        "fanstudio-earthquake-warning",
        "FAN Studio 地震预警",
        "ShakeAlert"
    ),
    source!(
        "fanstudio.kma-eew",
        "kma-eew",
        FanStudio,
        EarthquakeWarning,
        "fanstudio-earthquake-warning",
        "FAN Studio 地震预警",
        "韩国气象厅预警"
    ),
    source!(
        "fanstudio.cenc",
        "cenc",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "中国地震台网测定"
    ),
    source!(
        "fanstudio.ningxia",
        "ningxia",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "宁夏地震局"
    ),
    source!(
        "fanstudio.guangxi",
        "guangxi",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "广西地震局"
    ),
    source!(
        "fanstudio.shanxi",
        "shanxi",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "山西地震局"
    ),
    source!(
        "fanstudio.beijing",
        "beijing",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "北京地震局"
    ),
    source!(
        "fanstudio.yunnan",
        "yunnan",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "云南地震局"
    ),
    source!(
        "fanstudio.cwa",
        "cwa",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "台湾气象署报告"
    ),
    source!(
        "fanstudio.hko",
        "hko",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "香港天文台"
    ),
    source!(
        "fanstudio.usgs",
        "usgs",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "USGS"
    ),
    source!(
        "fanstudio.emsc",
        "emsc",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "EMSC"
    ),
    source!(
        "fanstudio.bcsf",
        "bcsf",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "BCSF"
    ),
    source!(
        "fanstudio.gfz",
        "gfz",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "GFZ"
    ),
    source!(
        "fanstudio.usp",
        "usp",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "USP"
    ),
    source!(
        "fanstudio.kma",
        "kma",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "韩国气象厅报告"
    ),
    source!(
        "fanstudio.fssn",
        "fssn",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "FSSN"
    ),
    source!(
        "fanstudio.fssn-cmt",
        "fssn-cmt",
        FanStudio,
        EarthquakeReport,
        "fanstudio-earthquake-report",
        "地震信息",
        "FSSN CMT"
    ),
    source!(
        "fanstudio.weatheralarm",
        "weatheralarm",
        FanStudio,
        WeatherWarning,
        "fanstudio-weather-warning",
        "气象预警",
        "中国气象局气象预警"
    ),
    source!(
        "fanstudio.tsunami",
        "tsunami",
        FanStudio,
        Tsunami,
        "fanstudio-tsunami",
        "海啸",
        "自然资源部海啸预警中心"
    ),
    source!(
        "fanstudio.typhoon",
        "typhoon",
        FanStudio,
        Typhoon,
        "fanstudio-typhoon",
        "台风",
        "中国气象局活跃台风"
    ),
];

pub fn find(id: &str) -> Option<&'static SourceDefinition> {
    SOURCES.iter().find(|source| source.id == id)
}

pub fn find_provider(
    channel: ProviderChannel,
    provider_key: &str,
) -> Option<&'static SourceDefinition> {
    SOURCES
        .iter()
        .find(|source| source.channel == channel && source.provider_key == provider_key)
}

pub fn category_options() -> Vec<CategoryOption> {
    DisasterCategory::ALL
        .into_iter()
        .map(|category| {
            let mut source_groups = Vec::<SourceGroup>::new();
            for source in SOURCES.iter().filter(|source| source.category == category) {
                if let Some(group) = source_groups
                    .iter_mut()
                    .find(|group| group.id == source.group_id)
                {
                    group.sources.push(SourceOption {
                        id: source.id,
                        label: source.label,
                    });
                } else {
                    source_groups.push(SourceGroup {
                        id: source.group_id,
                        label: source.group_label,
                        sources: vec![SourceOption {
                            id: source.id,
                            label: source.label,
                        }],
                    });
                }
            }
            CategoryOption {
                id: category.as_str(),
                label: category.label(),
                source_groups,
                default_alert: AlertRule::default_for(category),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_ids_are_unique_and_every_source_is_grouped() {
        let ids = SOURCES
            .iter()
            .map(|source| source.id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(ids.len(), SOURCES.len());
        let provider_keys = SOURCES
            .iter()
            .map(|source| (source.channel, source.provider_key))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(provider_keys.len(), SOURCES.len());
        let mut group_metadata = std::collections::HashMap::new();
        for source in SOURCES {
            let metadata = (source.category, source.group_label);
            assert_eq!(
                group_metadata.entry(source.group_id).or_insert(metadata),
                &metadata
            );
        }
        assert_eq!(
            category_options()
                .iter()
                .flat_map(|category| &category.source_groups)
                .map(|group| group.sources.len())
                .sum::<usize>(),
            SOURCES.len()
        );
    }
}
