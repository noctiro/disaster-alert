use crate::db::{SubscriptionSnapshot, SubscriptionStore};
use crate::models::{DestinationId, DisasterEvent, Subscription, mask_device_key};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

const MAX_RESPONSE_BYTES: usize = 16 * 1024;
const MAX_TITLE_CHARS: usize = 180;
const MAX_SUBTITLE_CHARS: usize = 180;
const MAX_BODY_CHARS: usize = 4_000;
const MAX_AFFECTED_REGIONS: usize = 20;
const MAX_REGION_CHARS: usize = 80;

#[derive(Debug, Clone)]
pub struct BarkPushConfig {
    pub sound: Option<String>,
    pub volume: u8,
    pub group: String,
    pub call: bool,
}

#[derive(Debug, Clone)]
pub struct AlertTiming {
    pub distance_km: f64,
    pub hypocentral_km: f64,
    pub estimated_intensity: u8,
    pub seconds_to_p: i64,
    pub seconds_to_s: i64,
}

#[derive(Debug, Clone)]
pub struct AlertRecipient {
    pub destination: DestinationId,
    pub location_name: String,
}

struct BarkMessage<'a> {
    bark_url: &'a str,
    device_key: &'a str,
    level: &'a str,
    title: &'a str,
    subtitle: &'a str,
    body: &'a str,
}

/// Bark 推送客户端，负责受限并发的可靠投递。
#[derive(Clone)]
pub struct BarkNotifier {
    allowed_urls: Arc<HashSet<String>>,
    client: reqwest::Client,
    subscription_store: SubscriptionStore,
    push_config: BarkPushConfig,
    concurrency: Arc<Semaphore>,
}

impl BarkNotifier {
    pub fn new(
        allowed_urls: Vec<String>,
        pool_size: usize,
        max_concurrent: usize,
        subscription_store: SubscriptionStore,
        push_config: BarkPushConfig,
    ) -> Result<Self> {
        push_config.validate()?;
        anyhow::ensure!(
            !allowed_urls.is_empty(),
            "Bark URL allowlist cannot be empty"
        );
        let client = reqwest::Client::builder()
            .user_agent("DisasterAlert/1.0")
            .timeout(Duration::from_secs(3))
            .connect_timeout(Duration::from_secs(3))
            .pool_max_idle_per_host(pool_size)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        tracing::info!(
            event = "bark.initialized",
            allowed_url_count = allowed_urls.len(),
            pool_size,
            "bark.initialized"
        );
        Ok(Self {
            allowed_urls: Arc::new(allowed_urls.into_iter().collect()),
            client,
            subscription_store,
            push_config,
            concurrency: Arc::new(Semaphore::new(max_concurrent.max(1))),
        })
    }

    pub fn allows_bark_url(&self, bark_url: &str) -> bool {
        self.allowed_urls.contains(bark_url)
    }

    pub fn is_subscription_current(&self, subscription: &SubscriptionSnapshot) -> bool {
        self.subscription_store.is_current(subscription)
    }

    pub async fn send_disaster_alert(
        &self,
        recipient: &AlertRecipient,
        level: &str,
        event: &DisasterEvent,
        timing: Option<&AlertTiming>,
    ) -> Result<()> {
        let location = match (event.latitude, event.longitude) {
            (Some(latitude), Some(longitude)) => format!("{latitude:.2}, {longitude:.2}"),
            _ => "位置未知".to_string(),
        };
        let display_title = if event.cancel {
            format!("{}（解除/取消）", event.title)
        } else if event.final_report {
            format!("{}（终报）", event.title)
        } else {
            event.title.clone()
        };
        let subtitle = format!("{} · {}", event.category.as_str(), event.source);
        let mut lines = Vec::new();
        if event.training {
            lines.push("[测试] 这是一条模拟灾害信息".to_string());
        }
        if !recipient.location_name.trim().is_empty() {
            lines.push(format!("监测点: {}", recipient.location_name.trim()));
        }
        lines.push(format!("位置: {location}"));
        if let Some(magnitude) = event.magnitude {
            lines.push(format!("震级: M{magnitude:.1}"));
        }
        if let Some(timing) = timing {
            lines.push(format!(
                "距离: 震中{:.0}km 震源{:.0}km",
                timing.distance_km, timing.hypocentral_km
            ));
            lines.push(format!(
                "预计: P波{:+}秒 S波{:+}秒 烈度{}",
                timing.seconds_to_p, timing.seconds_to_s, timing.estimated_intensity
            ));
        }
        if let Some(radius_km) = event.radius_km {
            lines.push(format!("七级风圈: {radius_km:.0}km"));
        }
        if !event.affected_regions.is_empty() {
            let regions = event
                .affected_regions
                .iter()
                .take(MAX_AFFECTED_REGIONS)
                .map(|region| truncate_chars(region, MAX_REGION_CHARS))
                .collect::<Vec<_>>();
            lines.push(format!("影响区域: {}", regions.join("、")));
        }
        if !event.description.trim().is_empty() {
            lines.push(truncate_chars(&event.description, MAX_BODY_CHARS));
        }
        lines.push(format!("来源: {} ({})", event.source, event.channel));
        if !event.occurred_at.trim().is_empty() {
            lines.push(format!("时间: {}", event.occurred_at));
        }
        let display_title = truncate_chars(&display_title, MAX_TITLE_CHARS);
        let subtitle = truncate_chars(&subtitle, MAX_SUBTITLE_CHARS);
        let body = truncate_chars(&lines.join("\n"), MAX_BODY_CHARS);
        self.send_notification(BarkMessage {
            bark_url: &recipient.destination.base_url,
            device_key: &recipient.destination.device_key,
            level,
            title: &display_title,
            subtitle: &subtitle,
            body: &body,
        })
        .await
    }

    pub async fn send_subscription_confirm(&self, subscription: &Subscription) -> Result<()> {
        let title = "灾害预警订阅成功";
        let subtitle = if subscription.targets.len() > 1 {
            format!("已保存 {} 个监测地点", subscription.targets.len())
        } else {
            subscription
                .targets
                .first()
                .map(|target| target.label.trim())
                .filter(|name| !name.is_empty())
                .map_or_else(
                    || "已保存监测地点".to_string(),
                    |name| format!("已保存 {name}"),
                )
        };
        let mut lines = vec!["你将按当前类别和通知级别规则接收灾害信息".to_string()];
        for target in &subscription.targets {
            let name = if target.label.trim().is_empty() {
                "未命名地点"
            } else {
                target.label.trim()
            };
            lines.push(format!(
                "{}: {:.4}, {:.4}",
                name, target.point.latitude, target.point.longitude
            ));
        }
        let body = lines.join("\n");

        self.send_notification(BarkMessage {
            bark_url: subscription.bark_base_url(),
            device_key: subscription.device_key(),
            level: "active",
            title,
            subtitle: &subtitle,
            body: &body,
        })
        .await
    }

    async fn send_notification(&self, message: BarkMessage<'_>) -> Result<()> {
        let BarkMessage {
            bark_url,
            device_key,
            level,
            title,
            subtitle,
            body,
        } = message;
        let level = match level.trim().to_ascii_lowercase().as_str() {
            "passive" => "passive",
            "active" => "active",
            "critical" => "critical",
            _ => "critical",
        };
        anyhow::ensure!(
            self.allows_bark_url(bark_url),
            "订阅使用的 Bark URL 已被管理员停用，请重新配置"
        );
        let url = format!("{bark_url}/push");
        let mut payload = serde_json::json!({
            "device_key": device_key,
            "title": title,
            "subtitle": subtitle,
            "body": body,
            "group": self.push_config.group,
            "level": level,
        });
        if level != "passive" {
            payload["volume"] = serde_json::json!(self.push_config.volume);
            if self.push_config.call {
                payload["call"] = serde_json::json!("1");
            }
            if let Some(sound) = &self.push_config.sound {
                payload["sound"] = serde_json::json!(sound);
            }
        }

        let mut retries = 0;
        let max_retries = 2;

        loop {
            let permit = self
                .concurrency
                .acquire()
                .await
                .context("Bark notification dispatcher closed")?;
            let response = self.client.post(&url).json(&payload).send().await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    let status_code = status.as_u16();

                    if status.is_success() {
                        let body_text = limited_response_text(response).await?;
                        if bark_response_succeeded(&body_text) {
                            tracing::debug!(
                                event = "bark.push_succeeded",
                                device_key = %mask_device_key(device_key),
                                status = status_code,
                                "bark.push_succeeded"
                            );
                            drop(permit);
                            return Ok(());
                        }

                        tracing::warn!(
                            event = "bark.push_rejected",
                            device_key = %mask_device_key(device_key),
                            status = status_code,
                            cleanup = false,
                            "bark.push_rejected"
                        );
                        drop(permit);
                        return Err(anyhow::anyhow!("Bark 服务拒绝了推送"));
                    } else {
                        let _error_text = limited_response_text(response).await?;

                        if status.is_client_error() {
                            tracing::warn!(
                                event = "bark.push_rejected",
                                device_key = %mask_device_key(device_key),
                                status = status_code,
                                cleanup = false,
                                "bark.push_rejected"
                            );
                            drop(permit);
                            return Err(anyhow::anyhow!(
                                "Bark 服务拒绝了推送 (HTTP {})",
                                status_code
                            ));
                        }

                        if status.is_server_error() && retries < max_retries {
                            drop(permit);
                            retries += 1;
                            tracing::warn!(
                                event = "bark.push_retrying",
                                device_key = %mask_device_key(device_key),
                                retry = retries,
                                max_retries,
                                status = status.as_u16(),
                                "bark.push_retrying"
                            );
                            tokio::time::sleep(backoff_delay(retries)).await;
                            continue;
                        }

                        tracing::error!(
                            event = "bark.push_failed",
                            device_key = %mask_device_key(device_key),
                            status = status.as_u16(),
                            "bark.push_failed"
                        );
                        drop(permit);
                        return Err(anyhow::anyhow!("Bark 推送失败: {}", status));
                    }
                }
                Err(e) => {
                    if retries < max_retries {
                        drop(permit);
                        retries += 1;
                        tracing::warn!(
                            event = "bark.request_retrying",
                            device_key = %mask_device_key(device_key),
                            retry = retries,
                            max_retries,
                            error = ?e,
                            "bark.request_retrying"
                        );
                        tokio::time::sleep(backoff_delay(retries)).await;
                        continue;
                    }

                    tracing::error!(
                        event = "bark.request_failed",
                        device_key = %mask_device_key(device_key),
                        error = ?e,
                        "bark.request_failed"
                    );
                    drop(permit);
                    return Err(e.into());
                }
            }
        }
    }
}

impl BarkPushConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(self.volume <= 10, "BARK_VOLUME must be in 0..=10");
        Ok(())
    }
}

fn bark_response_succeeded(body: &str) -> bool {
    if body.trim().is_empty() {
        return true;
    }

    #[derive(Deserialize)]
    struct BarkEnvelope {
        code: Option<i64>,
        success: Option<bool>,
    }

    match serde_json::from_str::<BarkEnvelope>(body) {
        Ok(response) => response.code == Some(200) || response.success == Some(true),
        Err(_) => false,
    }
}

async fn limited_response_text(mut response: reqwest::Response) -> Result<String> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        anyhow::bail!("Bark response exceeded size limit");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read Bark response")?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            anyhow::bail!("Bark response exceeded size limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn backoff_delay(retry: u32) -> Duration {
    let base = 100u64.saturating_mul(1u64 << retry.saturating_sub(1));
    Duration::from_millis(base + jitter_millis())
}

fn jitter_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_nanos()) % 50)
        .unwrap_or(0)
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value
        .chars()
        .take(max_chars.saturating_sub(1))
        .chain(['…'])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::truncate_chars;

    #[test]
    fn truncation_preserves_utf8_boundaries() {
        let truncated = truncate_chars("灾害预警abcdef", 6);
        assert_eq!(truncated, "灾害预警a…");
        assert_eq!(truncated.chars().count(), 6);
    }
}
