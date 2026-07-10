use crate::models::ProviderChannel;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Default)]
pub struct RuntimeStatus {
    wolfx: Arc<ChannelMetrics>,
    fanstudio: Arc<ChannelMetrics>,
}

#[derive(Default)]
pub struct ChannelMetrics {
    connected: AtomicBool,
    last_message_epoch_ms: AtomicU64,
    reconnects: AtomicU64,
    messages: AtomicU64,
    parse_errors: AtomicU64,
    queue_depth: AtomicUsize,
    queue_backpressure: AtomicU64,
    notifications_succeeded: AtomicU64,
    notifications_failed: AtomicU64,
}

#[derive(Serialize)]
pub struct RuntimeStatusSnapshot {
    pub wolfx: ChannelSnapshot,
    pub fanstudio: ChannelSnapshot,
}

#[derive(Serialize)]
pub struct ChannelSnapshot {
    pub connected: bool,
    pub last_message_epoch_ms: Option<u64>,
    pub reconnects: u64,
    pub messages: u64,
    pub parse_errors: u64,
    pub queue_depth: usize,
    pub queue_backpressure: u64,
    pub notifications_succeeded: u64,
    pub notifications_failed: u64,
}

impl RuntimeStatus {
    pub fn channel(&self, channel: ProviderChannel) -> &ChannelMetrics {
        match channel {
            ProviderChannel::Wolfx => &self.wolfx,
            ProviderChannel::FanStudio => &self.fanstudio,
        }
    }

    pub fn wolfx(&self) -> &ChannelMetrics {
        &self.wolfx
    }

    pub fn fanstudio(&self) -> &ChannelMetrics {
        &self.fanstudio
    }

    pub fn snapshot(&self) -> RuntimeStatusSnapshot {
        RuntimeStatusSnapshot {
            wolfx: self.wolfx.snapshot(),
            fanstudio: self.fanstudio.snapshot(),
        }
    }
}

impl ChannelMetrics {
    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    pub fn record_message(&self) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.last_message_epoch_ms
            .store(current_epoch_ms(), Ordering::Relaxed);
    }

    pub fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    pub fn record_queue_backpressure(&self) {
        self.queue_backpressure.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_notification(&self, succeeded: bool) {
        if succeeded {
            self.notifications_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.notifications_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> ChannelSnapshot {
        let last_message = self.last_message_epoch_ms.load(Ordering::Relaxed);
        ChannelSnapshot {
            connected: self.connected.load(Ordering::Relaxed),
            last_message_epoch_ms: (last_message != 0).then_some(last_message),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            messages: self.messages.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_backpressure: self.queue_backpressure.load(Ordering::Relaxed),
            notifications_succeeded: self.notifications_succeeded.load(Ordering::Relaxed),
            notifications_failed: self.notifications_failed.load(Ordering::Relaxed),
        }
    }
}

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
