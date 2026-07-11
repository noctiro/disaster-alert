use super::wolfx_protocol::{self, CommonEarthquakeInfo};
use crate::config::Config;
use crate::models::{DisasterCategory, DisasterEvent, ProviderChannel};
use crate::services::{DisasterDispatcher, RuntimeStatus};
use crate::source_registry;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct WolfxSource {
    dispatcher: DisasterDispatcher,
    websocket_url: String,
    reconnect_min: Duration,
    reconnect_max: Duration,
    runtime_status: RuntimeStatus,
}

impl WolfxSource {
    pub fn new(
        config: &Config,
        dispatcher: DisasterDispatcher,
        runtime_status: RuntimeStatus,
    ) -> Self {
        Self {
            dispatcher,
            websocket_url: config.wolfx_websocket_url.clone(),
            reconnect_min: Duration::from_secs(config.reconnect_min_seconds),
            reconnect_max: Duration::from_secs(config.reconnect_max_seconds),
            runtime_status,
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut delay = self.reconnect_min;
        loop {
            if *shutdown.borrow() {
                break;
            }
            match self.connect_once(&mut delay, &mut shutdown).await {
                Ok(true) => break,
                Ok(false) => delay = self.reconnect_min,
                Err(error) => tracing::error!(
                    event = "wolfx.websocket_error",
                    error = ?error,
                    "wolfx.websocket_error"
                ),
            }
            self.runtime_status.wolfx().set_connected(false);
            self.runtime_status.wolfx().record_reconnect();
            tokio::select! {
                biased;
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                () = tokio::time::sleep(delay) => {}
            }
            delay = delay.saturating_mul(2).min(self.reconnect_max);
        }
        self.runtime_status.wolfx().set_connected(false);
        Ok(())
    }

    async fn connect_once(
        &self,
        delay: &mut Duration,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool> {
        let connect = tokio::time::timeout(
            Duration::from_secs(10),
            connect_async_with_config(
                &self.websocket_url,
                Some(
                    WebSocketConfig::default()
                        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES))
                        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_BYTES)),
                ),
                false,
            ),
        );
        let (socket, _) = tokio::select! {
            biased;
            result = shutdown.changed() => {
                return Ok(result.is_err() || *shutdown.borrow());
            }
            result = connect => result
                .map_err(|error| anyhow::anyhow!("Wolfx connection timed out: {error}"))??,
        };
        *delay = self.reconnect_min;
        self.runtime_status.wolfx().set_connected(true);
        tracing::info!(
            event = "wolfx.connected",
            websocket_url = %self.websocket_url,
            "wolfx.connected"
        );
        let (mut write, mut read) = socket.split();
        loop {
            if *shutdown.borrow() {
                return Ok(true);
            }
            let message = tokio::select! {
                biased;
                result = tokio::time::timeout(Duration::from_secs(90), read.next()) => result
                    .map_err(|error| anyhow::anyhow!("Wolfx heartbeat timed out: {error}"))?,
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        return Ok(true);
                    }
                    continue;
                }
            };
            let Some(message) = message else { break };
            match message? {
                Message::Text(text) => {
                    self.runtime_status.wolfx().record_message();
                    let message_type = message_type(&text);
                    if message_type.as_deref() == Some("heartbeat") {
                        let send = tokio::time::timeout(
                            Duration::from_secs(10),
                            write.send(Message::Text("ping".into())),
                        );
                        tokio::select! {
                            biased;
                            result = shutdown.changed() => {
                                if result.is_err() || *shutdown.borrow() {
                                    return Ok(true);
                                }
                            }
                            result = send => {
                                result.map_err(|error| anyhow::anyhow!("Wolfx heartbeat response timed out: {error}"))??;
                            }
                        }
                        continue;
                    }
                    if matches!(
                        message_type.as_deref(),
                        Some("pong" | "jma_eqlist" | "cenc_eqlist")
                    ) {
                        continue;
                    }
                    let Some(provider_key) = message_type.as_deref() else {
                        self.runtime_status.wolfx().record_parse_error();
                        continue;
                    };
                    if source_registry::find_provider(ProviderChannel::Wolfx, provider_key)
                        .is_none()
                    {
                        tracing::warn!(
                            event = "wolfx.unsupported_source",
                            provider_key,
                            "wolfx.unsupported_source"
                        );
                        continue;
                    }
                    match wolfx_protocol::parse(&text) {
                        Ok(earthquake) => {
                            if !self
                                .dispatcher
                                .submit_nonblocking(normalize(earthquake))
                                .await
                            {
                                tracing::warn!(
                                    event = "wolfx.ingress_backpressure",
                                    "wolfx.ingress_backpressure"
                                );
                            }
                        }
                        Err(error) => {
                            self.runtime_status.wolfx().record_parse_error();
                            tracing::warn!(
                                event = "wolfx.parse_failed",
                                error = ?error,
                                "wolfx.parse_failed"
                            );
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok(false)
    }
}

fn message_type(message: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(message)
        .ok()?
        .get("type")?
        .as_str()
        .map(ToOwned::to_owned)
}

pub(super) fn normalize(earthquake: CommonEarthquakeInfo) -> DisasterEvent {
    let level = if earthquake.magnitude >= 7.0 {
        4
    } else if earthquake.magnitude >= 6.0 {
        3
    } else if earthquake.magnitude >= 5.0 {
        2
    } else {
        1
    };
    DisasterEvent {
        category: DisasterCategory::EarthquakeWarning,
        channel: ProviderChannel::Wolfx,
        source: format!("wolfx.{}", earthquake.source_type),
        event_id: earthquake.event_id,
        revision: earthquake.report_num.to_string(),
        report_num: earthquake.report_num,
        title: format!("地震预警 {}", earthquake.region),
        description: format!(
            "M{:.1} 最大烈度{}",
            earthquake.magnitude, earthquake.max_intensity
        ),
        latitude: Some(earthquake.latitude),
        longitude: Some(earthquake.longitude),
        magnitude: Some(earthquake.magnitude),
        depth_km: earthquake.depth,
        affected_regions: Vec::new(),
        radius_km: None,
        level,
        occurred_at: earthquake.origin_time,
        final_report: earthquake.final_report,
        cancel: earthquake.cancel,
        training: earthquake.training,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_wolfx_into_the_shared_event_contract() {
        let event = normalize(CommonEarthquakeInfo {
            event_id: "event-1".to_string(),
            report_num: 2,
            latitude: 35.0,
            longitude: 105.0,
            magnitude: 5.2,
            depth: Some(10.0),
            max_intensity: "4".to_string(),
            region: "test".to_string(),
            origin_time: "2026-07-10 00:00:00".to_string(),
            source_type: "cenc_eew".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        });
        assert_eq!(event.channel, ProviderChannel::Wolfx);
        assert_eq!(event.source, "wolfx.cenc_eew");
        assert_eq!(event.category, DisasterCategory::EarthquakeWarning);
        assert_eq!(event.report_num, 2);
    }
}
