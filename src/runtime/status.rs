use crate::models::ProviderChannel;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Default)]
pub(crate) struct RuntimeStatus {
    wolfx: Arc<ChannelMetrics>,
    fanstudio: Arc<ChannelMetrics>,
    huania: Arc<ChannelMetrics>,
    inbox_ready: Arc<ReadyQueueMetrics>,
    match_ready: Arc<ReadyQueueMetrics>,
    delivery_ready: Arc<ReadyQueueMetrics>,
}

#[derive(Default)]
pub(crate) struct ChannelMetrics {
    connected: AtomicBool,
    last_message_epoch_ms: AtomicU64,
    reconnects: AtomicU64,
    messages: AtomicU64,
    parse_errors: AtomicU64,
    notifications_succeeded: AtomicU64,
    notifications_failed: AtomicU64,
}

#[derive(Serialize)]
pub(crate) struct RuntimeStatusSnapshot {
    pub(crate) wolfx: ChannelSnapshot,
    pub(crate) fanstudio: ChannelSnapshot,
    pub(crate) huania: ChannelSnapshot,
    pub(crate) durable: DurableBacklogSnapshot,
    pub(crate) ready_queues: ReadyQueuesSnapshot,
}

#[derive(Serialize)]
pub(crate) struct ReadyQueuesSnapshot {
    pub(crate) inbox: ReadyQueueSnapshot,
    pub(crate) matching: ReadyQueueSnapshot,
    pub(crate) delivery: ReadyQueueSnapshot,
}

#[derive(Default)]
pub(super) struct ReadyQueueMetrics {
    depth: AtomicUsize,
    bytes: AtomicUsize,
    backpressure: AtomicU64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct ReadyQueueSnapshot {
    pub(crate) depth: usize,
    pub(crate) bytes: usize,
    pub(crate) backpressure: u64,
}

#[derive(Serialize)]
pub(crate) struct ChannelSnapshot {
    pub(crate) connected: bool,
    pub(crate) last_message_epoch_ms: Option<u64>,
    pub(crate) reconnects: u64,
    pub(crate) messages: u64,
    pub(crate) parse_errors: u64,
    pub(crate) notifications_succeeded: u64,
    pub(crate) notifications_failed: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct DurableBacklogSnapshot {
    pub(crate) inbox_pending: usize,
    pub(crate) match_jobs_pending: usize,
    pub(crate) delivery_batches_pending: usize,
    pub(crate) retries_pending: usize,
    pub(crate) subscription_confirmations_pending: usize,
}

impl RuntimeStatus {
    pub(crate) fn channel(&self, channel: ProviderChannel) -> &ChannelMetrics {
        match channel {
            ProviderChannel::Wolfx => &self.wolfx,
            ProviderChannel::FanStudio => &self.fanstudio,
            ProviderChannel::Huania => &self.huania,
        }
    }

    pub(crate) fn wolfx(&self) -> &ChannelMetrics {
        &self.wolfx
    }

    pub(crate) fn fanstudio(&self) -> &ChannelMetrics {
        &self.fanstudio
    }

    pub(crate) fn huania(&self) -> &ChannelMetrics {
        &self.huania
    }

    pub(crate) fn snapshot(&self, durable: DurableBacklogSnapshot) -> RuntimeStatusSnapshot {
        RuntimeStatusSnapshot {
            wolfx: self.wolfx.snapshot(),
            fanstudio: self.fanstudio.snapshot(),
            huania: self.huania.snapshot(),
            durable,
            ready_queues: ReadyQueuesSnapshot {
                inbox: self.inbox_ready.snapshot(),
                matching: self.match_ready.snapshot(),
                delivery: self.delivery_ready.snapshot(),
            },
        }
    }

    pub(super) fn inbox_ready_metrics(&self) -> Arc<ReadyQueueMetrics> {
        Arc::clone(&self.inbox_ready)
    }

    pub(super) fn match_ready_metrics(&self) -> Arc<ReadyQueueMetrics> {
        Arc::clone(&self.match_ready)
    }

    pub(super) fn delivery_ready_metrics(&self) -> Arc<ReadyQueueMetrics> {
        Arc::clone(&self.delivery_ready)
    }
}

impl ReadyQueueMetrics {
    pub(super) fn set_depth(&self, depth: usize, bytes: usize) {
        self.depth.store(depth, Ordering::Relaxed);
        self.bytes.store(bytes, Ordering::Relaxed);
    }

    pub(super) fn record_backpressure(&self) {
        self.backpressure.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> ReadyQueueSnapshot {
        ReadyQueueSnapshot {
            depth: self.depth.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            backpressure: self.backpressure.load(Ordering::Relaxed),
        }
    }
}

impl ChannelMetrics {
    pub(crate) fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    pub(crate) fn record_message(&self) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.last_message_epoch_ms
            .store(current_epoch_ms(), Ordering::Relaxed);
    }

    pub(crate) fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_notification(&self, succeeded: bool) {
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
