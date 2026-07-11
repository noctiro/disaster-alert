use crate::config::Config;
use crate::db::{Database, SubscriptionCandidateQuery, SubscriptionSnapshot};
use crate::models::{
    AlertRule, DestinationId, DisasterCategory, DisasterEvent, MonitoringTarget, ProviderChannel,
};
use crate::services::event_aggregator::{DeliveryAttempt, parse_event_epoch};
use crate::services::{AlertRecipient, AlertTiming, BarkNotifier, EventAggregator, RuntimeStatus};
use crate::utils::{distance, intensity, region};
use anyhow::Result;
use futures_util::{StreamExt, stream};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify, mpsc};

#[derive(Clone)]
pub struct DisasterDispatcher {
    inner: Arc<DispatcherInner>,
}

struct DispatcherInner {
    db: Database,
    notifier: BarkNotifier,
    aggregator: EventAggregator,
    queue: EventQueue,
    max_concurrent: usize,
    event_workers: usize,
    retry_delay: Duration,
    policy: DispatchPolicy,
    runtime_status: RuntimeStatus,
}

#[derive(Clone, Copy)]
struct DispatchPolicy {
    push_updates: bool,
    update_min_report_gap: u32,
    ignore_training: bool,
    ignore_cancel: bool,
    stale_origin_seconds: i64,
    p_wave_km_s: f64,
    s_wave_km_s: f64,
}

#[derive(Clone)]
struct QueuedEvent {
    event: DisasterEvent,
    incident_id: u64,
    sequence: u64,
    attempts: u8,
    queued_at: Instant,
}

struct DispatchTarget {
    subscription: SubscriptionSnapshot,
    recipient: AlertRecipient,
    level: String,
    timing: Option<AlertTiming>,
}

struct EventQueue {
    state: Mutex<EventQueueState>,
    ready: Notify,
    space: Notify,
    capacity: usize,
    runtime_status: RuntimeStatus,
}

#[derive(Default)]
struct EventQueueState {
    order: VecDeque<String>,
    pending: HashMap<String, QueuedEvent>,
    latest: HashMap<String, QueuedEvent>,
    latest_order: VecDeque<(String, u64)>,
    next_sequence: u64,
    wolfx_depth: usize,
    fanstudio_depth: usize,
}

impl DisasterDispatcher {
    pub fn new(
        db: Database,
        config: &Config,
        notifier: BarkNotifier,
        aggregator: EventAggregator,
        runtime_status: RuntimeStatus,
    ) -> Self {
        let max_concurrent = config
            .max_concurrent_notifications
            .min(config.http_pool_size)
            .max(1);
        Self {
            inner: Arc::new(DispatcherInner {
                db,
                notifier,
                aggregator,
                queue: EventQueue::new(
                    max_concurrent.saturating_mul(8).clamp(256, 16_384),
                    runtime_status.clone(),
                ),
                max_concurrent,
                event_workers: max_concurrent.min(8),
                retry_delay: Duration::from_secs(config.reconnect_min_seconds),
                policy: DispatchPolicy {
                    push_updates: config.push_updates,
                    update_min_report_gap: config.update_min_report_gap.max(1),
                    ignore_training: config.ignore_training,
                    ignore_cancel: config.ignore_cancel,
                    stale_origin_seconds: config.stale_origin_seconds,
                    p_wave_km_s: config.p_wave_km_s,
                    s_wave_km_s: config.s_wave_km_s,
                },
                runtime_status,
            }),
        }
    }

    /// Provider-facing ingestion path that never waits for downstream queue capacity.
    /// The upstream read loop stays available for heartbeats and newer revisions.
    pub async fn submit_nonblocking(&self, event: DisasterEvent) -> bool {
        self.submit_nonblocking_batch(vec![event]).await
    }

    pub async fn submit_nonblocking_batch(&self, events: Vec<DisasterEvent>) -> bool {
        let mut queued = Vec::with_capacity(events.len());
        for event in events {
            if self.inner.policy.rejects(&event) {
                continue;
            }
            let incident_id = self.inner.aggregator.correlate(&event).await;
            queued.push(QueuedEvent {
                event,
                incident_id,
                sequence: 0,
                attempts: 0,
                queued_at: Instant::now(),
            });
        }
        self.inner.queue.try_push_batch(queued).await
    }

    pub async fn submit_snapshot_batch(&self, events: Vec<DisasterEvent>) -> bool {
        let mut accepted = Vec::with_capacity(events.len());
        for event in events {
            if snapshot_is_stale(&event, self.inner.policy.stale_origin_seconds) {
                tracing::info!(
                    event = "dispatcher.snapshot_rejected",
                    source = %event.source,
                    event_key = %event.event_key(),
                    reason = "stale_origin",
                    "dispatcher.snapshot_rejected"
                );
                continue;
            }
            accepted.push(event);
        }
        self.submit_nonblocking_batch(accepted).await
    }

    pub async fn run(&self) -> Result<()> {
        let mut workers = tokio::task::JoinSet::new();
        for _ in 0..self.inner.event_workers {
            let dispatcher = self.clone();
            workers.spawn(async move { dispatcher.run_worker().await });
        }
        match workers.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(anyhow::anyhow!("dispatcher worker failed: {error}")),
            None => anyhow::bail!("dispatcher started without workers"),
        }
    }

    async fn run_worker(&self) -> Result<()> {
        loop {
            let mut queued = self.inner.queue.pop().await;
            if let Err(error) = self.inner.dispatch(&queued).await {
                tracing::error!(
                    event = "dispatcher.delivery_failed",
                    event_key = %queued.event.event_key(),
                    error = ?error,
                    "dispatcher.delivery_failed"
                );
                if queued.attempts < 5 && queued.queued_at.elapsed() < Duration::from_secs(300) {
                    queued.attempts += 1;
                    let delay = self
                        .inner
                        .retry_delay
                        .saturating_mul(1u32 << u32::from(queued.attempts.min(5) - 1));
                    tokio::time::sleep(delay.min(Duration::from_secs(30))).await;
                    self.inner.queue.push(queued).await;
                } else {
                    tracing::error!(
                        event = "dispatcher.delivery_abandoned",
                        event_key = %queued.event.event_key(),
                        attempts = queued.attempts,
                        "dispatcher.delivery_abandoned"
                    );
                }
            }
        }
    }
}

impl DispatcherInner {
    async fn dispatch(&self, queued: &QueuedEvent) -> Result<()> {
        let event = Arc::new(queued.event.clone());
        let prior_recipients = if event.cancel {
            Arc::new(
                self.aggregator
                    .delivered_destinations(queued.incident_id, event.category)
                    .await,
            )
        } else {
            Arc::new(HashMap::new())
        };
        let (sender, receiver) = mpsc::channel(self.max_concurrent.saturating_mul(2).max(1));
        let store = self.db.subscriptions();
        let event_for_lookup = Arc::clone(&event);
        let recipients_for_lookup = Arc::clone(&prior_recipients);
        let destinations_for_lookup = Arc::new(
            prior_recipients
                .keys()
                .cloned()
                .collect::<HashSet<DestinationId>>(),
        );
        let destinations_for_lookup = Arc::clone(&destinations_for_lookup);
        let policy = self.policy;
        let lookup = tokio::task::spawn_blocking(move || {
            let query = candidate_query(&event_for_lookup, &destinations_for_lookup);
            store.for_each_candidate(query, |subscription| {
                if let Some(target) = match_subscription(
                    subscription,
                    &event_for_lookup,
                    &recipients_for_lookup,
                    policy,
                ) {
                    sender.blocking_send(target).map_err(|error| {
                        anyhow::anyhow!("dispatcher target receiver closed: {error}")
                    })?;
                }
                Ok(())
            })
        });

        let channel = event.channel;
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let busy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|target| (target, receiver))
        })
        .for_each_concurrent(self.max_concurrent, |target| {
            let event = Arc::clone(&event);
            let failed = Arc::clone(&failed);
            let busy = Arc::clone(&busy);
            async move {
                if !self.notifier.is_subscription_current(&target.subscription) {
                    return;
                }
                let permit = match self
                    .aggregator
                    .begin_delivery(
                        queued.incident_id,
                        target.recipient.destination.clone(),
                        target.recipient.location_name.clone(),
                        &event,
                        self.policy.push_updates,
                        self.policy.update_min_report_gap,
                    )
                    .await
                {
                    DeliveryAttempt::Acquired(permit) => permit,
                    DeliveryAttempt::Busy => {
                        busy.store(true, std::sync::atomic::Ordering::Relaxed);
                        return;
                    }
                    DeliveryAttempt::Duplicate => return,
                };
                match self
                    .notifier
                    .send_disaster_alert(
                        &target.recipient,
                        &target.level,
                        &event,
                        target.timing.as_ref(),
                    )
                    .await
                {
                    Ok(()) => {
                        permit.commit().await;
                        self.runtime_status
                            .channel(channel)
                            .record_notification(true);
                    }
                    Err(error) => {
                        tracing::error!(
                            event = "dispatcher.notification_failed",
                            source = %event.source,
                            error = ?error,
                            "dispatcher.notification_failed"
                        );
                        permit.abort().await;
                        failed.store(true, std::sync::atomic::Ordering::Relaxed);
                        self.runtime_status
                            .channel(channel)
                            .record_notification(false);
                    }
                }
            }
        })
        .await;
        lookup
            .await
            .map_err(|error| anyhow::anyhow!("dispatcher lookup task failed: {error}"))??;
        if failed.load(std::sync::atomic::Ordering::Relaxed) {
            anyhow::bail!("one or more notifications failed");
        }
        if busy.load(std::sync::atomic::Ordering::Relaxed) {
            anyhow::bail!("one or more recipients have an in-flight delivery");
        }
        Ok(())
    }
}

impl DispatchPolicy {
    fn rejects(self, event: &DisasterEvent) -> bool {
        let reason = if event.training && self.ignore_training {
            Some("training")
        } else if event.cancel && self.ignore_cancel {
            Some("cancel")
        } else if self.stale_origin_seconds > 0
            && matches!(event.category, DisasterCategory::EarthquakeWarning)
            && event_age_seconds(event).is_some_and(|age| age > self.stale_origin_seconds)
        {
            Some("stale_origin")
        } else {
            None
        };
        if let Some(reason) = reason {
            tracing::info!(
                event = "dispatcher.event_rejected",
                source = %event.source,
                event_key = %event.event_key(),
                reason,
                "dispatcher.event_rejected"
            );
            true
        } else {
            false
        }
    }
}

impl EventQueue {
    fn new(capacity: usize, runtime_status: RuntimeStatus) -> Self {
        Self {
            state: Mutex::new(EventQueueState::default()),
            ready: Notify::new(),
            space: Notify::new(),
            capacity: capacity.max(1),
            runtime_status,
        }
    }

    async fn push(&self, mut queued: QueuedEvent) {
        let key = queued.event.event_key();
        loop {
            let notified = self.space.notified();
            let mut state = self.state.lock().await;
            if queued.sequence == 0 {
                state.next_sequence = state.next_sequence.wrapping_add(1).max(1);
                queued.sequence = state.next_sequence;
            }
            if let Some(latest) = state.latest.get(&key)
                && (latest.sequence > queued.sequence
                    || !event_supersedes_or_matches(&latest.event, &queued.event))
            {
                return;
            }
            if let Some(current) = state.pending.get(&key) {
                if current.sequence > queued.sequence
                    || !event_supersedes(&current.event, &queued.event)
                {
                    return;
                }
                state.record_latest(key.clone(), queued.clone(), self.capacity.saturating_mul(4));
                state.pending.insert(key, queued);
                return;
            }
            if state.pending.len() < self.capacity {
                state.record_latest(key.clone(), queued.clone(), self.capacity.saturating_mul(4));
                state.order.push_back(key.clone());
                state.increment(event_channel(&queued.event));
                state.pending.insert(key, queued);
                state.publish_depths(&self.runtime_status);
                drop(state);
                self.ready.notify_one();
                return;
            }
            self.runtime_status
                .channel(event_channel(&queued.event))
                .record_queue_backpressure();
            drop(state);
            notified.await;
        }
    }

    async fn try_push_batch(&self, queued: Vec<QueuedEvent>) -> bool {
        if queued.is_empty() {
            return true;
        }
        let mut state = self.state.lock().await;
        let mut staged = HashMap::<String, QueuedEvent>::with_capacity(queued.len());
        let mut staged_order = Vec::with_capacity(queued.len());

        for mut candidate in queued {
            let key = candidate.event.event_key();
            state.next_sequence = state.next_sequence.wrapping_add(1).max(1);
            candidate.sequence = state.next_sequence;
            if let Some(latest) = state.latest.get(&key)
                && !event_supersedes_or_matches(&latest.event, &candidate.event)
            {
                continue;
            }
            let current = staged.get(&key).or_else(|| state.pending.get(&key));
            if current.is_some_and(|current| !event_supersedes(&current.event, &candidate.event)) {
                continue;
            }
            if !staged.contains_key(&key) {
                staged_order.push(key.clone());
            }
            staged.insert(key, candidate);
        }

        let additional = staged
            .keys()
            .filter(|key| !state.pending.contains_key(*key))
            .count();
        if state.pending.len().saturating_add(additional) > self.capacity {
            for channel in staged
                .values()
                .map(|queued| queued.event.channel)
                .collect::<HashSet<_>>()
            {
                self.runtime_status
                    .channel(channel)
                    .record_queue_backpressure();
            }
            return false;
        }

        for key in staged_order {
            let Some(queued) = staged.remove(&key) else {
                continue;
            };
            let is_new = !state.pending.contains_key(&key);
            state.record_latest(key.clone(), queued.clone(), self.capacity.saturating_mul(4));
            if is_new {
                state.order.push_back(key.clone());
                state.increment(queued.event.channel);
            }
            state.pending.insert(key, queued);
        }
        state.publish_depths(&self.runtime_status);
        drop(state);
        for _ in 0..additional {
            self.ready.notify_one();
        }
        true
    }

    async fn pop(&self) -> QueuedEvent {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = self.state.lock().await;
                while let Some(key) = state.order.pop_front() {
                    if let Some(queued) = state.pending.remove(&key) {
                        state.decrement(event_channel(&queued.event));
                        state.publish_depths(&self.runtime_status);
                        self.space.notify_one();
                        return queued;
                    }
                }
            }
            notified.await;
        }
    }
}

impl EventQueueState {
    fn record_latest(&mut self, key: String, queued: QueuedEvent, capacity: usize) {
        let sequence = queued.sequence;
        self.latest.insert(key.clone(), queued);
        self.latest_order.push_back((key, sequence));
        let capacity = capacity.max(1);
        while self.latest.len() > capacity {
            if let Some((expired, expired_sequence)) = self.latest_order.pop_front()
                && self
                    .latest
                    .get(&expired)
                    .is_some_and(|current| current.sequence == expired_sequence)
            {
                self.latest.remove(&expired);
            }
        }
        if self.latest_order.len() > capacity.saturating_mul(2) {
            self.latest_order.retain(|(key, sequence)| {
                self.latest
                    .get(key)
                    .is_some_and(|current| current.sequence == *sequence)
            });
        }
    }

    fn increment(&mut self, channel: ProviderChannel) {
        match channel {
            ProviderChannel::Wolfx => self.wolfx_depth += 1,
            ProviderChannel::FanStudio => self.fanstudio_depth += 1,
        }
    }

    fn decrement(&mut self, channel: ProviderChannel) {
        match channel {
            ProviderChannel::Wolfx => self.wolfx_depth = self.wolfx_depth.saturating_sub(1),
            ProviderChannel::FanStudio => {
                self.fanstudio_depth = self.fanstudio_depth.saturating_sub(1)
            }
        }
    }

    fn publish_depths(&self, status: &RuntimeStatus) {
        status.wolfx().set_queue_depth(self.wolfx_depth);
        status.fanstudio().set_queue_depth(self.fanstudio_depth);
    }
}

fn event_supersedes(current: &DisasterEvent, candidate: &DisasterEvent) -> bool {
    if current.cancel && !candidate.cancel
        || current.final_report && !candidate.final_report && !candidate.cancel
    {
        return false;
    }
    if candidate.report_num != current.report_num {
        return candidate.report_num > current.report_num;
    }
    candidate.cancel && !current.cancel
        || candidate.final_report && !current.final_report
        || candidate.revision != current.revision
}

fn event_supersedes_or_matches(current: &DisasterEvent, candidate: &DisasterEvent) -> bool {
    current.report_num == candidate.report_num
        && current.revision == candidate.revision
        && current.final_report == candidate.final_report
        && current.cancel == candidate.cancel
        || event_supersedes(current, candidate)
}

fn candidate_query<'a>(
    event: &'a DisasterEvent,
    prior_recipients: &'a HashSet<DestinationId>,
) -> SubscriptionCandidateQuery<'a> {
    if event.cancel {
        return SubscriptionCandidateQuery::Destinations(prior_recipients);
    }
    match event.category {
        DisasterCategory::WeatherWarning => match event.latitude.zip(event.longitude) {
            Some((latitude, longitude)) if event.affected_regions.is_empty() => {
                SubscriptionCandidateQuery::Radius {
                    latitude,
                    longitude,
                    radius_km: 2_000.0,
                }
            }
            Some((latitude, longitude)) => SubscriptionCandidateQuery::RadiusOrRegions {
                latitude,
                longitude,
                radius_km: 2_000.0,
                regions: &event.affected_regions,
            },
            None => SubscriptionCandidateQuery::Regions(&event.affected_regions),
        },
        DisasterCategory::Tsunami if !event.affected_regions.is_empty() => {
            SubscriptionCandidateQuery::Regions(&event.affected_regions)
        }
        DisasterCategory::Typhoon => match event.latitude.zip(event.longitude) {
            Some((latitude, longitude)) => SubscriptionCandidateQuery::Radius {
                latitude,
                longitude,
                radius_km: 3_000.0,
            },
            None => SubscriptionCandidateQuery::All,
        },
        _ => SubscriptionCandidateQuery::All,
    }
}

fn match_subscription(
    subscription: SubscriptionSnapshot,
    event: &DisasterEvent,
    prior_recipients: &HashMap<DestinationId, String>,
    policy: DispatchPolicy,
) -> Option<DispatchTarget> {
    let stored = &subscription.subscription;
    let destination_id = stored.destination_id();
    if event.cancel && !prior_recipients.contains_key(&destination_id) {
        return None;
    }
    if event.cancel {
        let recipient = AlertRecipient {
            destination: destination_id,
            location_name: prior_recipients
                .get(&stored.destination_id())
                .cloned()
                .unwrap_or_default(),
        };
        return Some(DispatchTarget {
            subscription,
            recipient,
            level: "active".to_string(),
            timing: None,
        });
    }
    if !stored.source_enabled(event.category, &event.source) {
        return None;
    }
    let alert = stored.alert(event.category)?;

    let administrative = stored
        .targets
        .iter()
        .find(|location| location_matches_regions(location, &event.affected_regions));
    let nearest = nearest_location(stored, event);
    let region_based = matches!(
        event.category,
        DisasterCategory::WeatherWarning | DisasterCategory::Tsunami
    );
    let (location, distance_km) = administrative
        .filter(|_| region_based)
        .map(|location| (location, 0.0))
        .or(nearest)?;

    let timing = earthquake_timing(event, distance_km, policy);
    let level = match alert {
        AlertRule::EarthquakeWarning { .. } => stored
            .interruption_level_for_intensity(timing.as_ref()?.estimated_intensity)?
            .as_str()
            .to_string(),
        AlertRule::EarthquakeReport { min_magnitude, .. } => {
            if event.magnitude.unwrap_or_default() < *min_magnitude {
                return None;
            }
            bark_level(event.level).to_string()
        }
        AlertRule::WeatherWarning {
            min_severity,
            fallback_radius_km,
            ..
        } => {
            if event.level < *min_severity
                || (administrative.is_none() && distance_km > *fallback_radius_km)
            {
                return None;
            }
            bark_level(event.level).to_string()
        }
        AlertRule::Tsunami { min_severity, .. } => {
            if event.level < *min_severity || administrative.is_none() {
                return None;
            }
            bark_level(event.level).to_string()
        }
        AlertRule::Typhoon {
            max_center_distance_km,
            ..
        } => {
            if distance_km > *max_center_distance_km {
                return None;
            }
            bark_level(event.level).to_string()
        }
    };

    let recipient = AlertRecipient {
        destination: destination_id,
        location_name: location.label.clone(),
    };
    Some(DispatchTarget {
        subscription,
        recipient,
        level,
        timing,
    })
}

fn nearest_location<'a>(
    subscription: &'a crate::models::Subscription,
    event: &DisasterEvent,
) -> Option<(&'a MonitoringTarget, f64)> {
    let (latitude, longitude) = event.latitude.zip(event.longitude)?;
    subscription
        .targets
        .iter()
        .filter_map(|target| {
            distance::vincenty_distance(
                latitude,
                longitude,
                target.point.latitude,
                target.point.longitude,
            )
            .map(|distance| (target, distance))
        })
        .min_by(|left, right| left.1.total_cmp(&right.1))
}

fn earthquake_timing(
    event: &DisasterEvent,
    epicentral_km: f64,
    policy: DispatchPolicy,
) -> Option<AlertTiming> {
    if event.category != DisasterCategory::EarthquakeWarning {
        return None;
    }
    let depth = event.depth_km.unwrap_or_default().max(0.0);
    let hypocentral_km = (epicentral_km.powi(2) + depth.powi(2)).sqrt();
    let elapsed = event_age_seconds(event);
    Some(AlertTiming {
        distance_km: epicentral_km,
        hypocentral_km,
        estimated_intensity: intensity::estimate_intensity(event.magnitude?, hypocentral_km),
        seconds_to_p: seconds_until_arrival(hypocentral_km, policy.p_wave_km_s, elapsed),
        seconds_to_s: seconds_until_arrival(hypocentral_km, policy.s_wave_km_s, elapsed),
    })
}

fn seconds_until_arrival(distance_km: f64, speed: f64, elapsed: Option<i64>) -> i64 {
    if !speed.is_finite() || speed <= 0.0 {
        return 0;
    }
    let travel = (distance_km / speed).round() as i64;
    elapsed.map_or(travel, |elapsed| travel - elapsed)
}

fn event_age_seconds(event: &DisasterEvent) -> Option<i64> {
    let occurred = parse_event_epoch(event)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())?;
    Some(now - occurred)
}

fn snapshot_is_stale(event: &DisasterEvent, stale_origin_seconds: i64) -> bool {
    let freshness_required = matches!(
        event.category,
        DisasterCategory::EarthquakeWarning
            | DisasterCategory::EarthquakeReport
            | DisasterCategory::WeatherWarning
    );
    freshness_required
        && match event_age_seconds(event) {
            Some(age) => stale_origin_seconds > 0 && age > stale_origin_seconds,
            None => true,
        }
}

fn location_matches_regions(target: &MonitoringTarget, regions: &[String]) -> bool {
    let parts = [
        target.region.province.as_str(),
        target.region.city.as_str(),
        target.region.district.as_str(),
    ];
    regions
        .iter()
        .any(|region| parts.iter().any(|part| region::equivalent(part, region)))
}

fn bark_level(level: u8) -> &'static str {
    match level {
        4 => "critical",
        3 => "active",
        _ => "passive",
    }
}

fn event_channel(event: &DisasterEvent) -> ProviderChannel {
    event.channel
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AdministrativeRegion, AlertRule, GeoPoint, IntensityBand, InterruptionLevel,
        MonitoringTarget, NotificationDestination, SourceSelection, Subscription,
    };

    fn event(category: DisasterCategory, report_num: u32) -> DisasterEvent {
        DisasterEvent {
            category,
            channel: ProviderChannel::FanStudio,
            source: "fanstudio.jma".to_string(),
            event_id: "event-1".to_string(),
            revision: report_num.to_string(),
            report_num,
            title: "test".to_string(),
            description: String::new(),
            latitude: Some(35.0),
            longitude: Some(105.0),
            magnitude: Some(5.5),
            depth_km: Some(10.0),
            affected_regions: Vec::new(),
            radius_km: None,
            level: 2,
            occurred_at: "2026-07-10 12:00:00".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        }
    }

    fn subscription() -> SubscriptionSnapshot {
        let subscription = Subscription::new(
            NotificationDestination::Bark {
                base_url: "https://api.day.app".to_string(),
                device_key: "device".to_string(),
            },
            vec![MonitoringTarget {
                label: "home".to_string(),
                point: GeoPoint {
                    latitude: 35.0,
                    longitude: 106.0,
                },
                region: AdministrativeRegion {
                    province: "四川省".to_string(),
                    city: "成都市".to_string(),
                    district: String::new(),
                },
            }],
            vec![
                AlertRule::EarthquakeWarning {
                    sources: SourceSelection::All,
                    estimated_intensity_bands: vec![IntensityBand {
                        min: 0,
                        max: 7,
                        interruption_level: InterruptionLevel::Active,
                    }],
                },
                AlertRule::default_for(DisasterCategory::EarthquakeReport),
                AlertRule::default_for(DisasterCategory::WeatherWarning),
                AlertRule::default_for(DisasterCategory::Tsunami),
                AlertRule::default_for(DisasterCategory::Typhoon),
            ],
        );
        SubscriptionSnapshot::new(Arc::new(subscription))
    }

    fn policy() -> DispatchPolicy {
        DispatchPolicy {
            push_updates: true,
            update_min_report_gap: 1,
            ignore_training: true,
            ignore_cancel: true,
            stale_origin_seconds: 600,
            p_wave_km_s: 6.0,
            s_wave_km_s: 3.5,
        }
    }

    #[test]
    fn cancellation_uses_prior_delivery_without_current_event_fields() {
        let mut event = event(DisasterCategory::Tsunami, 2);
        event.cancel = true;
        event.latitude = None;
        event.longitude = None;
        event.affected_regions.clear();
        let recipients = HashMap::from([(
            subscription().subscription.destination_id(),
            "home".to_string(),
        )]);
        let target = match_subscription(subscription(), &event, &recipients, policy());
        assert!(target.is_some());
        assert!(target.is_some_and(|target| {
            target.timing.is_none() && target.recipient.location_name == "home"
        }));
    }

    #[test]
    fn earthquake_region_does_not_replace_real_distance() {
        let mut event = event(DisasterCategory::EarthquakeWarning, 1);
        event.affected_regions = vec!["四川".to_string()];
        let target = match_subscription(subscription(), &event, &HashMap::new(), policy());
        let timing = target.and_then(|target| target.timing);
        assert!(timing.is_some_and(|timing| timing.distance_km > 50.0));
    }

    #[test]
    fn coordinate_less_tsunami_matches_administrative_region() {
        let mut event = event(DisasterCategory::Tsunami, 1);
        event.latitude = None;
        event.longitude = None;
        event.affected_regions = vec!["四川省".to_string()];
        assert!(match_subscription(subscription(), &event, &HashMap::new(), policy()).is_some());
    }

    #[tokio::test]
    async fn queue_coalesces_newer_reports_and_rejects_stale_retries() {
        let queue = EventQueue::new(4, RuntimeStatus::default());
        let first = QueuedEvent {
            event: event(DisasterCategory::EarthquakeWarning, 1),
            incident_id: 1,
            sequence: 0,
            attempts: 0,
            queued_at: Instant::now(),
        };
        queue.push(first.clone()).await;
        queue
            .push(QueuedEvent {
                event: event(DisasterCategory::EarthquakeWarning, 3),
                incident_id: 1,
                sequence: 0,
                attempts: 0,
                queued_at: Instant::now(),
            })
            .await;
        queue.push(first).await;
        assert_eq!(queue.pop().await.event.report_num, 3);
    }

    #[tokio::test]
    async fn queue_rejects_stale_retry_after_newer_report_was_processed() {
        let queue = EventQueue::new(4, RuntimeStatus::default());
        let first = QueuedEvent {
            event: event(DisasterCategory::EarthquakeWarning, 1),
            incident_id: 1,
            sequence: 0,
            attempts: 0,
            queued_at: Instant::now(),
        };
        queue.push(first).await;
        let stale_retry = queue.pop().await;
        queue
            .push(QueuedEvent {
                event: event(DisasterCategory::EarthquakeWarning, 3),
                incident_id: 1,
                sequence: 0,
                attempts: 0,
                queued_at: Instant::now(),
            })
            .await;
        assert_eq!(queue.pop().await.event.report_num, 3);
        queue.push(stale_retry).await;
        let state = queue.state.lock().await;
        assert!(state.pending.is_empty());
    }

    #[test]
    fn version_order_accepts_corrections_and_rejects_terminal_regressions() {
        let mut current = event(DisasterCategory::WeatherWarning, 0);
        current.revision = "a".to_string();
        current.level = 4;
        let mut correction = current.clone();
        correction.revision = "b".to_string();
        correction.level = 2;
        assert!(event_supersedes(&current, &correction));

        let mut report_three = event(DisasterCategory::EarthquakeWarning, 3);
        report_three.revision = "3".to_string();
        let mut stale_cancel = report_three.clone();
        stale_cancel.report_num = 2;
        stale_cancel.revision = "2".to_string();
        stale_cancel.cancel = true;
        assert!(!event_supersedes(&report_three, &stale_cancel));

        let mut cancelled = report_three.clone();
        cancelled.cancel = true;
        let mut post_cancel = report_three;
        post_cancel.report_num = 4;
        post_cancel.revision = "4".to_string();
        assert!(!event_supersedes(&cancelled, &post_cancel));

        let mut final_report = event(DisasterCategory::EarthquakeWarning, 4);
        final_report.final_report = true;
        let mut final_cancel = final_report.clone();
        final_cancel.cancel = true;
        assert!(event_supersedes(&final_report, &final_cancel));
    }

    #[tokio::test]
    async fn queue_applies_backpressure_at_capacity() {
        let status = RuntimeStatus::default();
        let queue = Arc::new(EventQueue::new(1, status.clone()));
        queue
            .push(QueuedEvent {
                event: event(DisasterCategory::EarthquakeWarning, 1),
                incident_id: 1,
                sequence: 0,
                attempts: 0,
                queued_at: Instant::now(),
            })
            .await;
        let queue_for_push = Arc::clone(&queue);
        let mut second = event(DisasterCategory::EarthquakeWarning, 1);
        second.event_id = "event-2".to_string();
        let blocked = tokio::spawn(async move {
            queue_for_push
                .push(QueuedEvent {
                    event: second,
                    incident_id: 2,
                    sequence: 0,
                    attempts: 0,
                    queued_at: Instant::now(),
                })
                .await;
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());
        queue.pop().await;
        assert!(blocked.await.is_ok());
        assert_eq!(queue.pop().await.event.event_id, "event-2");
        assert_eq!(status.snapshot().fanstudio.queue_backpressure, 1);
    }
}
