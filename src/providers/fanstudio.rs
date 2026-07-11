use super::fanstudio_protocol::{parse_fanstudio_snapshot, parse_fanstudio_update_value};
use crate::config::Config;
use crate::services::{DisasterDispatcher, RuntimeStatus};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct FanStudioSource {
    dispatcher: DisasterDispatcher,
    websocket_url: String,
    reconnect_min: Duration,
    reconnect_max: Duration,
    runtime_status: RuntimeStatus,
}

impl FanStudioSource {
    pub fn new(
        config: &Config,
        dispatcher: DisasterDispatcher,
        runtime_status: RuntimeStatus,
    ) -> Self {
        Self {
            dispatcher,
            websocket_url: config.fanstudio_websocket_url.clone(),
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
                    event = "fanstudio.websocket_error",
                    error = ?error,
                    "fanstudio.websocket_error"
                ),
            }
            self.runtime_status.fanstudio().set_connected(false);
            self.runtime_status.fanstudio().record_reconnect();
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
        self.runtime_status.fanstudio().set_connected(false);
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
                .map_err(|error| anyhow::anyhow!("Fan Studio connection timed out: {error}"))??,
        };
        *delay = self.reconnect_min;
        self.runtime_status.fanstudio().set_connected(true);
        // A completed handshake proves the transport is healthy; do not retain outage backoff.
        tracing::info!(
            event = "fanstudio.connected",
            websocket_url = %self.websocket_url,
            "fanstudio.connected"
        );
        let (mut write, mut read) = socket.split();
        let mut source_md5 = HashMap::new();
        loop {
            if *shutdown.borrow() {
                return Ok(true);
            }
            let message = tokio::select! {
                biased;
                result = tokio::time::timeout(Duration::from_secs(90), read.next()) => result
                    .map_err(|error| anyhow::anyhow!("Fan Studio heartbeat timed out: {error}"))?,
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
                    self.runtime_status.fanstudio().record_message();
                    let envelope: serde_json::Value = match serde_json::from_str(&text) {
                        Ok(value) => value,
                        Err(error) => {
                            self.runtime_status.fanstudio().record_parse_error();
                            tracing::warn!(
                                event = "fanstudio.invalid_json",
                                error = ?error,
                                "fanstudio.invalid_json"
                            );
                            continue;
                        }
                    };
                    match envelope.get("type").and_then(serde_json::Value::as_str) {
                        Some("heartbeat") => {
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
                                    result.map_err(|error| anyhow::anyhow!("Fan Studio heartbeat response timed out: {error}"))??;
                                }
                            }
                        }
                        Some("initial_all" | "query_response") => {
                            self.submit_snapshot(&envelope, &mut source_md5).await
                        }
                        Some("update") if is_new_update(&envelope, &source_md5) => {
                            match parse_fanstudio_update_value(&envelope) {
                                Ok(events) => {
                                    if !self.dispatcher.submit_nonblocking_batch(events).await {
                                        tracing::warn!(
                                            event = "fanstudio.ingress_backpressure",
                                            "fanstudio.ingress_backpressure"
                                        );
                                        continue;
                                    }
                                    commit_update_revision(&envelope, &mut source_md5);
                                }
                                Err(error) => {
                                    self.runtime_status.fanstudio().record_parse_error();
                                    tracing::warn!(
                                        event = "fanstudio.update_parse_failed",
                                        error = ?error,
                                        "fanstudio.update_parse_failed"
                                    );
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok(false)
    }

    async fn submit_snapshot(
        &self,
        envelope: &serde_json::Value,
        source_md5: &mut HashMap<String, String>,
    ) {
        for (source, parsed) in parse_fanstudio_snapshot(envelope) {
            match parsed {
                Ok(batch) => {
                    let accepted = self.dispatcher.submit_snapshot_batch(batch.events).await;
                    if accepted && let Some(md5) = batch.md5 {
                        source_md5.insert(batch.source, md5);
                    } else if !accepted {
                        tracing::warn!(
                            event = "fanstudio.snapshot_backpressure",
                            source,
                            "fanstudio.snapshot_backpressure"
                        );
                    }
                }
                Err(error) => {
                    self.runtime_status.fanstudio().record_parse_error();
                    tracing::warn!(event = "fanstudio.snapshot_parse_failed", source, error = ?error, "fanstudio.snapshot_parse_failed");
                }
            }
        }
    }
}

fn is_new_update(envelope: &serde_json::Value, source_md5: &HashMap<String, String>) -> bool {
    let source = envelope
        .get("source")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let md5 = envelope
        .get("md5")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if !md5.is_empty() && source_md5.get(source).is_some_and(|current| current == md5) {
        return false;
    }
    true
}

fn commit_update_revision(envelope: &serde_json::Value, source_md5: &mut HashMap<String, String>) {
    if let (Some(source), Some(md5)) = (
        envelope.get("source").and_then(serde_json::Value::as_str),
        envelope.get("md5").and_then(serde_json::Value::as_str),
    ) && !source.is_empty()
        && !md5.is_empty()
    {
        source_md5.insert(source.to_string(), md5.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppresses_only_identical_source_revisions() {
        let mut revisions = HashMap::new();
        let first = serde_json::json!({"source":"cenc","md5":"a"});
        let duplicate = serde_json::json!({"source":"cenc","md5":"a"});
        let next = serde_json::json!({"source":"cenc","md5":"b"});
        assert!(is_new_update(&first, &revisions));
        commit_update_revision(&first, &mut revisions);
        assert!(!is_new_update(&duplicate, &revisions));
        assert!(is_new_update(&next, &revisions));
    }
}
