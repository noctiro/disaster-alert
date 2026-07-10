use anyhow::{Context, Result, bail};
use std::env;
use url::{Host, Url};

/// 应用配置
#[derive(Debug, Clone)]
pub struct Config {
    pub server_host: String,
    pub server_port: u16,
    pub allowed_origins: Vec<String>,
    pub db_path: String,
    /// Ordered, normalized Bark server roots. The first entry is only the web UI's
    /// initial selection; it is never a server-side fallback.
    pub bark_url_allowlist: Vec<String>,
    pub bark_sound: Option<String>,
    pub bark_volume: u8,
    pub bark_group: String,
    pub bark_call: bool,
    pub eew_websocket_url: String,
    pub reconnect_min_seconds: u64,
    pub reconnect_max_seconds: u64,
    pub push_updates: bool,
    pub update_min_report_gap: u32,
    pub ignore_training: bool,
    pub ignore_cancel: bool,
    pub p_wave_km_s: f64,
    pub s_wave_km_s: f64,
    pub stale_origin_seconds: i64,
    pub dedup_keep_minutes: u64,
    pub max_distance_km: f64,
    /// 并发推送的最大数量
    pub max_concurrent_notifications: usize,
    /// HTTP 连接池大小
    pub http_pool_size: usize,
}

impl Config {
    /// 从环境变量加载配置
    pub fn from_env() -> Result<Self> {
        let config = Self {
            server_host: env_string("SERVER_HOST", "0.0.0.0"),
            server_port: env_parse("SERVER_PORT", 30010)?,
            allowed_origins: env_list("ALLOWED_ORIGINS"),
            db_path: env_string("DB_PATH", "./data/earthquake.db"),
            bark_url_allowlist: bark_url_allowlist()?,
            bark_sound: env::var("BARK_SOUND")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            bark_volume: env_parse("BARK_VOLUME", 10)?,
            bark_group: env_string("BARK_GROUP", "地震预警"),
            bark_call: env_bool("BARK_CALL", false)?,
            eew_websocket_url: env_string("EEW_WEBSOCKET_URL", "wss://ws-api.wolfx.jp/all_eew"),
            reconnect_min_seconds: env_parse("RECONNECT_MIN_SECONDS", 1)?,
            reconnect_max_seconds: env_parse("RECONNECT_MAX_SECONDS", 30)?,
            push_updates: env_bool("PUSH_UPDATES", false)?,
            update_min_report_gap: env_parse("UPDATE_MIN_REPORT_GAP", 1)?,
            ignore_training: env_bool("IGNORE_TRAINING", true)?,
            ignore_cancel: env_bool("IGNORE_CANCEL", true)?,
            p_wave_km_s: env_parse("P_WAVE_KM_S", 6.0)?,
            s_wave_km_s: env_parse("S_WAVE_KM_S", 3.5)?,
            stale_origin_seconds: env_parse("STALE_ORIGIN_SECONDS", 600)?,
            dedup_keep_minutes: env_parse("DEDUP_KEEP_MINUTES", 120)?,
            max_distance_km: env_parse("MAX_DISTANCE_KM", 1000.0)?,
            max_concurrent_notifications: env_parse("MAX_CONCURRENT_NOTIFICATIONS", 1000)?,
            http_pool_size: env_parse("HTTP_POOL_SIZE", 200)?,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.reconnect_min_seconds == 0 {
            bail!("RECONNECT_MIN_SECONDS must be greater than 0");
        }
        if self.reconnect_min_seconds > self.reconnect_max_seconds {
            bail!("RECONNECT_MIN_SECONDS must be <= RECONNECT_MAX_SECONDS");
        }
        if !(self.p_wave_km_s.is_finite() && self.p_wave_km_s > 0.0) {
            bail!("P_WAVE_KM_S must be a finite positive number");
        }
        if !(self.s_wave_km_s.is_finite() && self.s_wave_km_s > 0.0) {
            bail!("S_WAVE_KM_S must be a finite positive number");
        }
        if self.stale_origin_seconds < 0 {
            bail!("STALE_ORIGIN_SECONDS must be >= 0");
        }
        if self.dedup_keep_minutes == 0 {
            bail!("DEDUP_KEEP_MINUTES must be greater than 0");
        }
        if !(self.max_distance_km.is_finite() && self.max_distance_km >= 0.0) {
            bail!("MAX_DISTANCE_KM must be a finite non-negative number");
        }
        if self.max_concurrent_notifications == 0 || self.max_concurrent_notifications > 10_000 {
            bail!("MAX_CONCURRENT_NOTIFICATIONS must be in 1..=10000");
        }
        if self.http_pool_size == 0 || self.http_pool_size > 10_000 {
            bail!("HTTP_POOL_SIZE must be in 1..=10000");
        }
        if self.bark_volume > 10 {
            bail!("BARK_VOLUME must be in 0..=10");
        }
        if self.bark_url_allowlist.is_empty() {
            bail!("BARK_URL_ALLOWLIST must contain at least one URL");
        }
        Ok(())
    }
}

fn bark_url_allowlist() -> Result<Vec<String>> {
    let raw = env::var("BARK_URL_ALLOWLIST").unwrap_or_else(|_| "https://api.day.app".to_string());
    let mut urls = Vec::new();

    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let normalized = normalize_bark_url(entry)
            .with_context(|| format!("invalid BARK_URL_ALLOWLIST entry {entry:?}"))?;
        if !urls.contains(&normalized) {
            urls.push(normalized);
        }
    }

    if urls.is_empty() {
        bail!("BARK_URL_ALLOWLIST must contain at least one URL");
    }
    Ok(urls)
}

pub fn normalize_bark_url(value: &str) -> Result<String> {
    let parsed = Url::parse(value.trim()).context("must be an absolute URL")?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("must be an HTTPS origin without credentials, port, path, query, or fragment");
    }
    if matches!(parsed.host(), Some(Host::Ipv4(_) | Host::Ipv6(_))) {
        bail!("IP address hosts are not allowed");
    }
    Ok(parsed.origin().ascii_serialization())
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_list(name: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn env_parse<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + Copy,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<T>()
            .with_context(|| format!("failed to parse {name}={value:?}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => bail!("failed to parse {name}={value:?} as boolean"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_bark_url;

    #[test]
    fn normalizes_https_origins() -> anyhow::Result<()> {
        anyhow::ensure!(normalize_bark_url(" https://api.day.app/ ")? == "https://api.day.app");
        anyhow::ensure!(
            normalize_bark_url("https://BARK.EXAMPLE.COM")? == "https://bark.example.com"
        );
        Ok(())
    }

    #[test]
    fn rejects_non_origin_and_unsafe_urls() {
        for value in [
            "http://api.day.app",
            "https://api.day.app.evil.example/path",
            "https://api.day.app@evil.example",
            "https://127.0.0.1:8443",
            "https://127.0.0.1",
            "https://[::1]",
            "https://api.day.app/path",
            "https://api.day.app?target=localhost",
            "https://api.day.app/#fragment",
            "not-a-url",
        ] {
            assert!(normalize_bark_url(value).is_err(), "accepted {value:?}");
        }
    }
}
