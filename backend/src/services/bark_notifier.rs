use crate::db::SubscriptionStore;
use crate::models::{CommonEarthquakeInfo, Subscription, mask_bark_id};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_RESPONSE_BYTES: usize = 16 * 1024;

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
    pub bark_id: String,
    pub bark_url: String,
    pub location_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

struct BarkMessage<'a> {
    bark_url: &'a str,
    bark_id: &'a str,
    level: &'a str,
    title: &'a str,
    subtitle: &'a str,
    body: &'a str,
    cleanup_invalid_subscription: bool,
}

/// Bark 推送客户端，负责重试和无效订阅清理
#[derive(Clone)]
pub struct BarkNotifier {
    allowed_urls: Arc<HashSet<String>>,
    client: reqwest::Client,
    subscription_store: SubscriptionStore,
    push_config: BarkPushConfig,
}

impl BarkNotifier {
    pub fn new(
        allowed_urls: Vec<String>,
        pool_size: usize,
        subscription_store: SubscriptionStore,
        push_config: BarkPushConfig,
    ) -> Result<Self> {
        push_config.validate()?;
        anyhow::ensure!(
            !allowed_urls.is_empty(),
            "Bark URL allowlist cannot be empty"
        );
        let client = reqwest::Client::builder()
            .user_agent("EarthquakeAlert/1.0")
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
        })
    }

    pub fn allows_bark_url(&self, bark_url: &str) -> bool {
        self.allowed_urls.contains(bark_url)
    }

    pub async fn send_earthquake_alert(
        &self,
        recipient: &AlertRecipient,
        level: &str,
        earthquake: &CommonEarthquakeInfo,
        timing: &AlertTiming,
    ) -> Result<()> {
        let eta = if timing.seconds_to_s > 0 {
            format!("{}秒后到达", timing.seconds_to_s)
        } else {
            "已到达".to_string()
        };

        let prefix = if earthquake.training {
            "地震预警测试"
        } else {
            "地震预警"
        };
        let title = format!("{} {}", prefix, eta);

        let subtitle = format!(
            "M{:.1} 预计烈度{} 距{:.0}km",
            earthquake.magnitude, timing.estimated_intensity, timing.distance_km
        );

        let region_text = if earthquake.region.is_empty() {
            format!(
                "{:.2}°N, {:.2}°E",
                earthquake.latitude, earthquake.longitude
            )
        } else {
            earthquake.region.clone()
        };

        let report_label = if earthquake.report_num > 0 {
            format!(" 第{}报", earthquake.report_num)
        } else {
            String::new()
        };
        let status_label = if earthquake.final_report {
            " 终报"
        } else {
            ""
        };
        let mut lines = Vec::new();
        if earthquake.training {
            lines.push("[测试] 这是一条模拟预警，不是真实地震".to_string());
        }
        if !recipient.location_name.trim().is_empty() {
            lines.push(format!(
                "监测点: {} {:.4}, {:.4}",
                recipient.location_name.trim(),
                recipient.latitude,
                recipient.longitude
            ));
        }
        lines.extend([
            format!("地点: {}", region_text),
            format!(
                "震源: {:.2}, {:.2} 深度{:.0}km",
                earthquake.latitude, earthquake.longitude, earthquake.depth
            ),
            format!(
                "距离: 震中{:.0}km 震源{:.0}km",
                timing.distance_km, timing.hypocentral_km
            ),
            format!(
                "预计: P波{:+}秒 S波{:+}秒 烈度{}",
                timing.seconds_to_p, timing.seconds_to_s, timing.estimated_intensity
            ),
            format!(
                "震级: M{:.1} 最大烈度{}",
                earthquake.magnitude, earthquake.max_intensity
            ),
            format!(
                "来源: {}{}{}",
                earthquake.source_type, report_label, status_label
            ),
            format!("发震: {}", earthquake.origin_time),
        ]);
        let body = lines.join("\n");

        self.send_notification(BarkMessage {
            bark_url: &recipient.bark_url,
            bark_id: &recipient.bark_id,
            level,
            title: &title,
            subtitle: &subtitle,
            body: &body,
            cleanup_invalid_subscription: true,
        })
        .await
    }

    pub async fn send_subscription_confirm(&self, subscription: &Subscription) -> Result<()> {
        let title = "地震预警订阅成功";
        let subtitle = if subscription.locations.len() > 1 {
            format!("已保存 {} 个监测地点", subscription.locations.len())
        } else if subscription.location_name.trim().is_empty() {
            "已保存监测地点".to_string()
        } else {
            format!("已保存 {}", subscription.location_name.trim())
        };
        let mut lines = vec!["你将按当前通知级别规则接收地震预警".to_string()];
        for location in subscription.normalized_locations() {
            let name = if location.name.trim().is_empty() {
                "未命名地点"
            } else {
                location.name.trim()
            };
            lines.push(format!(
                "{}: {:.4}, {:.4}",
                name, location.latitude, location.longitude
            ));
        }
        let body = lines.join("\n");

        self.send_notification(BarkMessage {
            bark_url: &subscription.bark_url,
            bark_id: &subscription.bark_id,
            level: "active",
            title,
            subtitle: &subtitle,
            body: &body,
            cleanup_invalid_subscription: false,
        })
        .await
    }

    async fn send_notification(&self, message: BarkMessage<'_>) -> Result<()> {
        let BarkMessage {
            bark_url,
            bark_id,
            level,
            title,
            subtitle,
            body,
            cleanup_invalid_subscription,
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
            "device_key": bark_id,
            "title": title,
            "subtitle": subtitle,
            "body": body,
            "group": self.push_config.group,
            "level": level,
        });
        if level != "passive" {
            if self.push_config.volume > 0 {
                payload["volume"] = serde_json::json!(self.push_config.volume);
            }
            if self.push_config.call {
                payload["call"] = serde_json::json!(1);
            }
            if let Some(sound) = &self.push_config.sound {
                payload["sound"] = serde_json::json!(sound);
            }
        }

        let mut retries = 0;
        let max_retries = 2;

        loop {
            match self.client.post(&url).json(&payload).send().await {
                Ok(response) => {
                    let status = response.status();
                    let status_code = status.as_u16();

                    if status.is_success() {
                        let body_text = limited_response_text(response).await?;
                        if bark_response_succeeded(&body_text) {
                            tracing::debug!(
                                event = "bark.push_succeeded",
                                bark_id = %mask_bark_id(bark_id),
                                status = status_code,
                                "bark.push_succeeded"
                            );
                            return Ok(());
                        }

                        tracing::warn!(
                            event = "bark.push_rejected",
                            bark_id = %mask_bark_id(bark_id),
                            status = status_code,
                            cleanup = false,
                            "bark.push_rejected"
                        );
                        return Err(anyhow::anyhow!("Bark 服务拒绝了推送"));
                    } else {
                        let _error_text = limited_response_text(response).await?;

                        if (status_code == 400 || status_code == 404)
                            && cleanup_invalid_subscription
                        {
                            tracing::warn!(
                                event = "bark.push_rejected",
                                bark_id = %mask_bark_id(bark_id),
                                status = status_code,
                                cleanup = true,
                                "bark.push_rejected"
                            );

                            let store = self.subscription_store.clone();
                            let bark_id_owned = bark_id.to_string();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                store.delete_subscription(&bark_id_owned)
                            })
                            .await
                            .map_err(anyhow::Error::from)
                            .and_then(|result| result)
                            {
                                tracing::error!(
                                    event = "subscription.cleanup_failed",
                                    bark_id = %mask_bark_id(bark_id),
                                    error = ?e,
                                    "subscription.cleanup_failed"
                                );
                            } else {
                                tracing::info!(
                                    event = "subscription.cleaned_up",
                                    bark_id = %mask_bark_id(bark_id),
                                    reason = "bark_rejected",
                                    "subscription.cleaned_up"
                                );
                            }

                            return Err(anyhow::anyhow!(
                                "Bark 推送失败 (HTTP {}), 已删除订阅",
                                status_code
                            ));
                        }

                        if status.is_client_error() {
                            tracing::warn!(
                                event = "bark.push_rejected",
                                bark_id = %mask_bark_id(bark_id),
                                status = status_code,
                                cleanup = false,
                                "bark.push_rejected"
                            );
                            return Err(anyhow::anyhow!(
                                "Bark 服务拒绝了推送 (HTTP {})",
                                status_code
                            ));
                        }

                        if status.is_server_error() && retries < max_retries {
                            retries += 1;
                            tracing::warn!(
                                event = "bark.push_retrying",
                                bark_id = %mask_bark_id(bark_id),
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
                            bark_id = %mask_bark_id(bark_id),
                            status = status.as_u16(),
                            "bark.push_failed"
                        );
                        return Err(anyhow::anyhow!("Bark 推送失败: {}", status));
                    }
                }
                Err(e) => {
                    if retries < max_retries {
                        retries += 1;
                        tracing::warn!(
                            event = "bark.request_retrying",
                            bark_id = %mask_bark_id(bark_id),
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
                        bark_id = %mask_bark_id(bark_id),
                        error = ?e,
                        "bark.request_failed"
                    );
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
