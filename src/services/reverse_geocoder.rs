use crate::config::Config;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use url::Url;

const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MIN_REQUEST_INTERVAL: Duration = Duration::from_secs(1);
const MAX_CACHE_ENTRIES: usize = 1_024;

#[derive(Debug, Clone, Serialize)]
pub struct ReverseGeocodeResult {
    pub province: String,
    pub city: String,
    pub district: String,
}

#[derive(Clone)]
pub struct ReverseGeocoder {
    enabled: bool,
    endpoint: Url,
    client: reqwest::Client,
    state: Arc<Mutex<GeocoderState>>,
}

#[derive(Default)]
struct GeocoderState {
    cache: HashMap<CoordinateKey, CacheEntry>,
    cache_order: VecDeque<CoordinateKey>,
    last_request: Option<Instant>,
}

struct CacheEntry {
    value: ReverseGeocodeResult,
    stored_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CoordinateKey {
    latitude: i32,
    longitude: i32,
}

#[derive(Deserialize)]
struct NominatimResponse {
    #[serde(default)]
    address: NominatimAddress,
}

#[derive(Default, Deserialize)]
struct NominatimAddress {
    state: Option<String>,
    province: Option<String>,
    region: Option<String>,
    city: Option<String>,
    town: Option<String>,
    municipality: Option<String>,
    county: Option<String>,
    city_district: Option<String>,
    district: Option<String>,
    borough: Option<String>,
    suburb: Option<String>,
}

impl ReverseGeocoder {
    pub fn new(config: &Config) -> Result<Self> {
        let endpoint = Url::parse(&config.reverse_geocoding_url)
            .context("failed to parse reverse geocoding URL")?;
        let client = reqwest::Client::builder()
            .user_agent("disaster-alert/1.0 (https://github.com/noctiro/disaster-alert)")
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(2)
            .build()?;
        Ok(Self {
            enabled: config.reverse_geocoding_enabled,
            endpoint,
            client,
            state: Arc::new(Mutex::new(GeocoderState::default())),
        })
    }

    pub async fn resolve(&self, latitude: f64, longitude: f64) -> Result<ReverseGeocodeResult> {
        if !self.enabled {
            bail!("reverse geocoding is disabled");
        }
        let key = CoordinateKey::new(latitude, longitude)?;
        let mut state = self.state.lock().await;
        if let Some(entry) = state.cache.get(&key)
            && entry.stored_at.elapsed() <= CACHE_TTL
        {
            return Ok(entry.value.clone());
        }
        if let Some(last_request) = state.last_request {
            let elapsed = last_request.elapsed();
            if elapsed < MIN_REQUEST_INTERVAL {
                tokio::time::sleep(MIN_REQUEST_INTERVAL - elapsed).await;
            }
        }
        state.last_request = Some(Instant::now());
        drop(state);

        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("format", "jsonv2")
            .append_pair("addressdetails", "1")
            .append_pair("accept-language", "zh-CN,zh,en")
            .append_pair("zoom", "14")
            .append_pair("lat", &latitude.to_string())
            .append_pair("lon", &longitude.to_string());
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("reverse geocoding request failed")?
            .error_for_status()
            .context("reverse geocoding service returned an error")?
            .json::<NominatimResponse>()
            .await
            .context("invalid reverse geocoding response")?;
        let value = response.address.into_result();

        let mut state = self.state.lock().await;
        state.cache.insert(
            key,
            CacheEntry {
                value: value.clone(),
                stored_at: Instant::now(),
            },
        );
        state.cache_order.retain(|cached| *cached != key);
        state.cache_order.push_back(key);
        while state.cache_order.len() > MAX_CACHE_ENTRIES {
            if let Some(expired) = state.cache_order.pop_front() {
                state.cache.remove(&expired);
            }
        }
        Ok(value)
    }
}

impl CoordinateKey {
    fn new(latitude: f64, longitude: f64) -> Result<Self> {
        if !latitude.is_finite()
            || !longitude.is_finite()
            || !(-90.0..=90.0).contains(&latitude)
            || !(-180.0..=180.0).contains(&longitude)
        {
            bail!("invalid coordinates");
        }
        Ok(Self {
            latitude: (latitude * 10_000.0).round() as i32,
            longitude: (longitude * 10_000.0).round() as i32,
        })
    }
}

impl NominatimAddress {
    fn into_result(self) -> ReverseGeocodeResult {
        ReverseGeocodeResult {
            province: first_non_empty([self.state, self.province, self.region]),
            city: first_non_empty([self.city, self.town, self.municipality, self.county]),
            district: first_non_empty([
                self.city_district,
                self.district,
                self.borough,
                self.suburb,
            ]),
        }
    }
}

fn first_non_empty<const N: usize>(values: [Option<String>; N]) -> String {
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{CoordinateKey, NominatimAddress};

    #[test]
    fn maps_nominatim_address_fallbacks() -> anyhow::Result<()> {
        let result = NominatimAddress {
            state: Some("四川省".to_string()),
            province: None,
            region: None,
            city: None,
            town: Some("成都市".to_string()),
            municipality: None,
            county: None,
            city_district: Some("武侯区".to_string()),
            district: None,
            borough: None,
            suburb: None,
        }
        .into_result();
        anyhow::ensure!(result.province == "四川省");
        anyhow::ensure!(result.city == "成都市");
        anyhow::ensure!(result.district == "武侯区");
        Ok(())
    }

    #[test]
    fn coordinate_cache_key_uses_four_decimal_places() -> anyhow::Result<()> {
        anyhow::ensure!(
            CoordinateKey::new(35.12344, 139.12344)? == CoordinateKey::new(35.12343, 139.12343)?
        );
        anyhow::ensure!(CoordinateKey::new(91.0, 0.0).is_err());
        Ok(())
    }
}
