use crate::config::Config;
use crate::db::{Database, SubscriptionSnapshot};
use crate::models::{CommonEarthquakeInfo, EarthquakeData, WebSocketMessage, mask_bark_id};
use crate::services::{AlertRecipient, AlertTiming, BarkNotifier};
use crate::utils::{distance, intensity};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt, stream};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Clone)]
struct MonitorConfig {
    websocket_url: String,
    reconnect_min: Duration,
    reconnect_max: Duration,
    push_updates: bool,
    update_min_report_gap: u32,
    ignore_training: bool,
    ignore_cancel: bool,
    p_wave_km_s: f64,
    s_wave_km_s: f64,
    stale_origin_seconds: i64,
    dedup_keep: Duration,
}

#[derive(Clone)]
struct SeenEvent {
    report_num: u32,
    at: Instant,
}

struct PushTarget {
    subscription: SubscriptionSnapshot,
    recipient: AlertRecipient,
    level: String,
    timing: AlertTiming,
}

#[derive(Default)]
struct NotifyCounts {
    filtered: usize,
    notified: usize,
    errors: usize,
}

#[derive(Default)]
struct EventQueue {
    state: Mutex<EventQueueState>,
    ready: Notify,
}

#[derive(Default)]
struct EventQueueState {
    order: VecDeque<String>,
    pending: HashMap<String, CommonEarthquakeInfo>,
}

impl EventQueue {
    async fn push(&self, earthquake: CommonEarthquakeInfo) {
        let key = earthquake_key(&earthquake);
        let mut state = self.state.lock().await;
        if let Some(current) = state.pending.get(&key)
            && current.report_num > earthquake.report_num
        {
            return;
        }
        if !state.pending.contains_key(&key) {
            state.order.push_back(key.clone());
        }
        state.pending.insert(key, earthquake);
        drop(state);
        self.ready.notify_one();
    }

    async fn pop(&self) -> CommonEarthquakeInfo {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = self.state.lock().await;
                while let Some(key) = state.order.pop_front() {
                    if let Some(earthquake) = state.pending.remove(&key) {
                        return earthquake;
                    }
                }
            }
            notified.await;
        }
    }
}

/// 监听 EEW WebSocket，并把匹配订阅的事件转成 Bark 推送
pub struct EarthquakeMonitor {
    db: Database,
    bark_notifier: BarkNotifier,
    max_concurrent: usize,
    config: MonitorConfig,
    seen_events: Arc<Mutex<HashMap<String, SeenEvent>>>,
}

impl EarthquakeMonitor {
    pub fn new(db: Database, config: Config, bark_notifier: BarkNotifier) -> Result<Self> {
        let max_concurrent = config
            .max_concurrent_notifications
            .min(config.http_pool_size)
            .max(1);
        let monitor_config = MonitorConfig {
            websocket_url: config.eew_websocket_url.clone(),
            reconnect_min: Duration::from_secs(config.reconnect_min_seconds.max(1)),
            reconnect_max: Duration::from_secs(
                config
                    .reconnect_max_seconds
                    .max(config.reconnect_min_seconds.max(1)),
            ),
            push_updates: config.push_updates,
            update_min_report_gap: config.update_min_report_gap.max(1),
            ignore_training: config.ignore_training,
            ignore_cancel: config.ignore_cancel,
            p_wave_km_s: if config.p_wave_km_s > 0.0 {
                config.p_wave_km_s
            } else {
                6.0
            },
            s_wave_km_s: if config.s_wave_km_s > 0.0 {
                config.s_wave_km_s
            } else {
                3.5
            },
            stale_origin_seconds: config.stale_origin_seconds,
            dedup_keep: Duration::from_secs(config.dedup_keep_minutes.max(1) * 60),
        };

        tracing::info!(
            event = "monitor.initialized",
            max_concurrent,
            configured_max_concurrent = config.max_concurrent_notifications,
            http_pool_size = config.http_pool_size,
            websocket_url = %monitor_config.websocket_url,
            "monitor.initialized"
        );

        Ok(Self {
            db,
            bark_notifier,
            max_concurrent,
            config: monitor_config,
            seen_events: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// 启动 WebSocket 循环；连接断开后按指数退避重连
    pub async fn start(&self) -> Result<()> {
        let event_queue = EventQueue::default();
        tokio::select! {
            result = self.run_connection_loop(&event_queue) => result,
            result = self.run_notification_worker(&event_queue) => result,
        }
    }

    async fn run_connection_loop(&self, event_queue: &EventQueue) -> Result<()> {
        let mut reconnect_delay = self.config.reconnect_min;
        let mut consecutive_errors = 0u64;
        loop {
            tracing::info!(
                event = "websocket.connecting",
                websocket_url = %self.config.websocket_url,
                "websocket.connecting"
            );

            match self.connect_and_monitor(event_queue).await {
                Ok(_) => {
                    tracing::warn!(event = "websocket.closed", "websocket.closed");
                    reconnect_delay = self.config.reconnect_min;
                    consecutive_errors = 0;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    tracing::error!(event = "websocket.error", error = ?e, consecutive_errors, "websocket.error");
                }
            }

            tracing::info!(
                event = "websocket.reconnect_scheduled",
                delay_seconds = reconnect_delay.as_secs(),
                "websocket.reconnect_scheduled"
            );
            tokio::time::sleep(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(self.config.reconnect_max);
        }
    }

    async fn run_notification_worker(&self, event_queue: &EventQueue) -> Result<()> {
        loop {
            let earthquake = event_queue.pop().await;
            if let Err(error) = self.notify_subscribers(&earthquake).await {
                tracing::error!(
                    event = "eew.notify_failed",
                    event_key = %earthquake_key(&earthquake),
                    error = ?error,
                    "eew.notify_failed"
                );
                event_queue.push(earthquake).await;
                tokio::time::sleep(self.config.reconnect_min).await;
            }
        }
    }

    async fn connect_and_monitor(&self, event_queue: &EventQueue) -> Result<()> {
        let (ws_stream, _) = connect_async(&self.config.websocket_url).await?;
        tracing::info!(
            event = "websocket.connected",
            websocket_url = %self.config.websocket_url,
            "websocket.connected"
        );

        // tokio-tungstenite handles WebSocket Ping frames. Wolfx additionally recommends
        // replying to its JSON heartbeat with the text command "ping".
        let (mut write, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    if is_wolfx_heartbeat(&text) {
                        write.send(Message::Text("ping".into())).await?;
                        tracing::debug!(
                            event = "websocket.heartbeat_ping_sent",
                            "websocket.heartbeat_ping_sent"
                        );
                        continue;
                    }
                    if let Err(e) = self.enqueue_earthquake_message(event_queue, &text).await {
                        tracing::error!(event = "eew.handle_failed", error = ?e, "eew.handle_failed");
                    }
                }
                Ok(Message::Close(_)) => {
                    tracing::info!(event = "websocket.close_frame", "websocket.close_frame");
                    break;
                }
                Ok(Message::Ping(_)) => {
                    // tokio-tungstenite 会自动处理 pong
                    tracing::debug!(event = "websocket.ping", "websocket.ping");
                }
                Err(e) => {
                    tracing::error!(event = "websocket.message_error", error = ?e, "websocket.message_error");
                    return Err(e.into());
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn enqueue_earthquake_message(
        &self,
        event_queue: &EventQueue,
        message: &str,
    ) -> Result<()> {
        let msg_wrapper: WebSocketMessage = match serde_json::from_str(message) {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!(
                    event = "eew.message_type_parse_failed",
                    error = ?e,
                    message_len = message.len(),
                    "eew.message_type_parse_failed"
                );
                return Ok(());
            }
        };

        match msg_wrapper.message_type.as_str() {
            "heartbeat" => {
                tracing::debug!(event = "websocket.heartbeat", "websocket.heartbeat");
                return Ok(());
            }
            "pong" => {
                tracing::debug!(event = "websocket.pong", "websocket.pong");
                return Ok(());
            }
            "jma_eqlist" | "cenc_eqlist" => {
                tracing::debug!(
                    event = "eew.list_ignored",
                    message_type = %msg_wrapper.message_type,
                    "eew.list_ignored"
                );
                return Ok(());
            }
            _ => {}
        }

        let common_info = match EarthquakeData::parse_to_common_info(message) {
            Ok(info) => info,
            Err(e) => {
                tracing::error!(
                    event = "eew.parse_failed",
                    message_type = %msg_wrapper.message_type,
                    error = ?e,
                    "eew.parse_failed"
                );
                return Ok(());
            }
        };

        tracing::info!(
            event = "eew.received",
            source_type = %common_info.source_type,
            event_id = %common_info.event_id,
            report_num = common_info.report_num,
            final_report = common_info.final_report,
            cancel = common_info.cancel,
            training = common_info.training,
            magnitude = common_info.magnitude,
            depth_km = common_info.depth,
            latitude = common_info.latitude,
            longitude = common_info.longitude,
            region = %common_info.region,
            "eew.received"
        );

        if !self.reserve_event(&common_info).await {
            return Ok(());
        }

        event_queue.push(common_info).await;

        Ok(())
    }

    async fn notify_subscribers(&self, earthquake: &CommonEarthquakeInfo) -> Result<()> {
        let start_time = Instant::now();

        if !crate::models::valid_coordinate(earthquake.latitude, earthquake.longitude) {
            tracing::warn!(
                event = "notify.invalid_coordinates",
                latitude = earthquake.latitude,
                longitude = earthquake.longitude,
                "notify.invalid_coordinates"
            );
            return Ok(());
        }

        let event_key = earthquake_key(earthquake);
        tracing::info!(
            event = "notify.lookup_started",
            event_key = %event_key,
            "notify.lookup_started"
        );

        let channel_capacity = self.max_concurrent.clamp(1, 10_000);
        let (target_sender, target_receiver) = mpsc::channel::<PushTarget>(channel_capacity);

        let store = self.db.subscriptions();
        let config = self.config.clone();
        let earthquake_for_lookup = earthquake.clone();
        let elapsed_since_origin = elapsed_since_origin_seconds(earthquake);
        let lookup_handle = tokio::task::spawn_blocking(move || {
            let mut total_candidates = 0usize;
            store.for_each_subscription(|subscription| {
                total_candidates += 1;
                if let Some(target) = evaluate_subscription(
                    &config,
                    &subscription,
                    &earthquake_for_lookup,
                    elapsed_since_origin,
                ) {
                    target_sender.blocking_send(target).map_err(|error| {
                        anyhow::anyhow!("notification target receiver closed: {error}")
                    })?;
                }
                Ok(())
            })?;
            Ok::<_, anyhow::Error>(total_candidates)
        });

        let bark_notifier = self.bark_notifier.clone();
        let earthquake = Arc::new(earthquake.clone());

        let target_stream = stream::unfold(target_receiver, |mut receiver| async move {
            receiver.recv().await.map(|target| (target, receiver))
        });

        let counts = target_stream
            .map(|target| {
                let bark_notifier = bark_notifier.clone();
                let earthquake = Arc::clone(&earthquake);

                async move {
                    if !bark_notifier.is_subscription_current(&target.subscription) {
                        return None;
                    }
                    let bark_id = target.recipient.bark_id.clone();
                    tracing::debug!(
                        event = "notify.send_started",
                        bark_id = %mask_bark_id(&bark_id),
                        distance_km = target.timing.distance_km,
                        estimated_intensity = target.timing.estimated_intensity,
                        level = %target.level,
                        "notify.send_started"
                    );

                    match bark_notifier
                        .send_earthquake_alert(
                            &target.recipient,
                            &target.level,
                            earthquake.as_ref(),
                            &target.timing,
                        )
                        .await
                    {
                        Ok(_) => Some(true),
                        Err(e) => {
                            tracing::error!(
                                event = "notify.send_failed",
                                bark_id = %mask_bark_id(&bark_id),
                                error = ?e,
                                "notify.send_failed"
                            );
                            Some(false)
                        }
                    }
                }
            })
            .buffer_unordered(self.max_concurrent)
            .fold(NotifyCounts::default(), |mut counts, result| async move {
                if let Some(success) = result {
                    counts.filtered += 1;
                    if success {
                        counts.notified += 1;
                    } else {
                        counts.errors += 1;
                    }
                }
                counts
            })
            .await;

        let total_candidates = lookup_handle.await??;

        tracing::info!(
            event = "notify.candidates_loaded",
            event_key = %event_key,
            candidate_count = total_candidates,
            "notify.candidates_loaded"
        );

        if total_candidates == 0 {
            tracing::info!(
                event = "notify.skipped",
                event_key = %event_key,
                reason = "no_candidates",
                "notify.skipped"
            );
            return Ok(());
        }

        tracing::info!(
            event = "notify.filtered",
            event_key = %event_key,
            notification_count = counts.filtered,
            filtered_count = total_candidates.saturating_sub(counts.filtered),
            "notify.filtered"
        );

        if counts.filtered == 0 {
            tracing::info!(
                event = "notify.skipped",
                event_key = %event_key,
                reason = "below_threshold",
                "notify.skipped"
            );
            return Ok(());
        }

        let elapsed = start_time.elapsed();

        tracing::info!(
            event = "notify.completed",
            event_key = %event_key,
            candidate_count = total_candidates,
            notified_count = counts.notified,
            error_count = counts.errors,
            elapsed_seconds = elapsed.as_secs_f64(),
            throughput_per_second = if elapsed.as_secs_f64() >= 0.001 {
                counts.notified as f64 / elapsed.as_secs_f64()
            } else {
                0.0
            },
            "notify.completed"
        );

        Ok(())
    }

    async fn reserve_event(&self, earthquake: &CommonEarthquakeInfo) -> bool {
        if earthquake.training && self.config.ignore_training {
            tracing::info!(
                event = "eew.skipped",
                reason = "training",
                event_key = %earthquake_key(earthquake),
                "eew.skipped"
            );
            return false;
        }
        if earthquake.cancel && self.config.ignore_cancel {
            tracing::info!(
                event = "eew.skipped",
                reason = "cancel",
                event_key = %earthquake_key(earthquake),
                "eew.skipped"
            );
            return false;
        }
        if self.config.stale_origin_seconds > 0
            && let Some(age_seconds) = origin_age_seconds(earthquake)
            && age_seconds > self.config.stale_origin_seconds
        {
            tracing::info!(
                event = "eew.skipped",
                reason = "stale_origin",
                event_key = %earthquake_key(earthquake),
                age_seconds,
                stale_origin_seconds = self.config.stale_origin_seconds,
                "eew.skipped"
            );
            return false;
        }

        let mut seen = self.seen_events.lock().await;
        let now = Instant::now();
        seen.retain(|_, value| now.duration_since(value.at) <= self.config.dedup_keep);
        let key = earthquake_key(earthquake);
        if let Some(previous) = seen.get(&key) {
            let is_update = earthquake.report_num > previous.report_num;
            let gap = earthquake.report_num.saturating_sub(previous.report_num);
            let bypass_gap = is_update && (earthquake.final_report || earthquake.cancel);
            if !is_update
                || (!bypass_gap
                    && (!self.config.push_updates || gap < self.config.update_min_report_gap))
            {
                tracing::debug!(
                    event = "eew.skipped",
                    reason = "duplicate",
                    event_key = %key,
                    previous_report_num = previous.report_num,
                    report_num = earthquake.report_num,
                    "eew.skipped"
                );
                return false;
            }
        }
        seen.insert(
            key,
            SeenEvent {
                report_num: earthquake.report_num,
                at: now,
            },
        );
        true
    }
}

fn is_wolfx_heartbeat(message: &str) -> bool {
    serde_json::from_str::<WebSocketMessage>(message)
        .map(|message| message.message_type == "heartbeat")
        .unwrap_or(false)
}

fn evaluate_subscription(
    config: &MonitorConfig,
    subscription: &SubscriptionSnapshot,
    earthquake: &CommonEarthquakeInfo,
    elapsed_since_origin: Option<i64>,
) -> Option<PushTarget> {
    let mut best: Option<PushTarget> = None;
    subscription
        .subscription
        .for_each_normalized_location(|location_name, latitude, longitude| {
            let Some(dist) = distance::vincenty_distance(
                earthquake.latitude,
                earthquake.longitude,
                latitude,
                longitude,
            ) else {
                return;
            };
            let hypocentral_km = (dist.powi(2) + earthquake.depth.max(0.0).powi(2)).sqrt();
            let estimated_intensity =
                intensity::estimate_intensity(earthquake.magnitude, hypocentral_km);
            let Some(level) = subscription
                .subscription
                .level_for_intensity(estimated_intensity)
            else {
                return;
            };
            let timing = AlertTiming {
                distance_km: dist,
                hypocentral_km,
                estimated_intensity,
                seconds_to_p: seconds_until_arrival(
                    hypocentral_km,
                    config.p_wave_km_s,
                    elapsed_since_origin,
                ),
                seconds_to_s: seconds_until_arrival(
                    hypocentral_km,
                    config.s_wave_km_s,
                    elapsed_since_origin,
                ),
            };
            let replace = best
                .as_ref()
                .map(|current| timing.distance_km < current.timing.distance_km)
                .unwrap_or(true);
            if replace {
                best = Some(PushTarget {
                    subscription: subscription.clone(),
                    recipient: AlertRecipient {
                        bark_id: subscription.subscription.bark_id.clone(),
                        bark_url: subscription.subscription.bark_url.clone(),
                        location_name: location_name.to_string(),
                        latitude,
                        longitude,
                    },
                    level: level.to_string(),
                    timing,
                });
            }
        });
    best
}

fn earthquake_key(earthquake: &CommonEarthquakeInfo) -> String {
    if !earthquake.event_id.trim().is_empty() {
        format!("{}:{}", earthquake.source_type, earthquake.event_id)
    } else {
        format!(
            "{}:{:.3}:{:.3}:{:.1}:{}",
            earthquake.source_type,
            earthquake.latitude,
            earthquake.longitude,
            earthquake.magnitude,
            earthquake.origin_time
        )
    }
}

fn seconds_until_arrival(
    hypocentral_km: f64,
    speed: f64,
    elapsed_since_origin: Option<i64>,
) -> i64 {
    if !speed.is_finite() || speed <= 0.0 {
        return 0;
    }
    let travel_seconds = (hypocentral_km / speed).round() as i64;
    elapsed_since_origin.map_or(travel_seconds, |elapsed| travel_seconds - elapsed)
}

fn origin_age_seconds(earthquake: &CommonEarthquakeInfo) -> Option<i64> {
    elapsed_since_origin_seconds(earthquake)
}

fn elapsed_since_origin_seconds(earthquake: &CommonEarthquakeInfo) -> Option<i64> {
    let origin_epoch = parse_origin_epoch_seconds(earthquake)?;
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(error) => {
            tracing::error!(event = "clock_error", error = ?error, "clock_error");
            return None;
        }
    };
    Some(now - origin_epoch)
}

fn parse_origin_epoch_seconds(earthquake: &CommonEarthquakeInfo) -> Option<i64> {
    let offset = if earthquake.source_type == "jma_eew" {
        9 * 3600
    } else {
        8 * 3600
    };
    parse_datetime_epoch_seconds(&earthquake.origin_time, offset)
}

fn parse_datetime_epoch_seconds(value: &str, offset_seconds: i64) -> Option<i64> {
    let (date, time) = value.trim().split_once([' ', 'T'])?;
    let mut date_parts = date.split(['-', '/']);
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second = time_parts
        .next()
        .map_or(Some(0), |value| value.parse().ok())?;
    if time_parts.next().is_some() {
        return None;
    }
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_prime + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Subscription;

    fn queued_earthquake(event_id: &str, report_num: u32) -> CommonEarthquakeInfo {
        CommonEarthquakeInfo {
            event_id: event_id.to_string(),
            report_num,
            latitude: 0.0,
            longitude: 0.0,
            magnitude: 5.0,
            depth: 10.0,
            max_intensity: "3".to_string(),
            region: String::new(),
            origin_time: "2026-07-10 00:00:00".to_string(),
            source_type: "cenc_eew".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        }
    }

    #[test]
    fn parses_space_slash_and_timestamps_with_timezone_offsets() {
        let beijing = parse_datetime_epoch_seconds("2026-07-07 09:30:00", 8 * 3600);
        let slash = parse_datetime_epoch_seconds("2026/07/07 09:30:00", 8 * 3600);
        let jst = parse_datetime_epoch_seconds("2026-07-07T10:30:00", 9 * 3600);

        assert_eq!(beijing, slash);
        assert_eq!(beijing, jst);
        assert!(beijing.is_some());
    }

    #[test]
    fn rejects_malformed_and_impossible_dates() {
        assert_eq!(
            parse_datetime_epoch_seconds("2026-XX-07 09:30:00", 8 * 3600),
            None
        );
        assert_eq!(
            parse_datetime_epoch_seconds("2026-02-30 09:30:00", 8 * 3600),
            None
        );
        assert_eq!(
            parse_datetime_epoch_seconds("2026-04-31 09:30:00", 8 * 3600),
            None
        );
        assert!(parse_datetime_epoch_seconds("2024-02-29 09:30:00", 8 * 3600).is_some());
    }

    #[test]
    fn identifies_only_wolfx_json_heartbeats() {
        assert!(is_wolfx_heartbeat(r#"{"type":"heartbeat","ver":1}"#));
        assert!(!is_wolfx_heartbeat(r#"{"type":"jma_eew"}"#));
        assert!(!is_wolfx_heartbeat("heartbeat"));
    }

    #[test]
    fn evaluates_distant_locations_without_a_distance_cutoff() {
        let config = MonitorConfig {
            websocket_url: String::new(),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            push_updates: false,
            update_min_report_gap: 1,
            ignore_training: true,
            ignore_cancel: true,
            p_wave_km_s: 6.0,
            s_wave_km_s: 3.5,
            stale_origin_seconds: 600,
            dedup_keep: Duration::from_secs(120 * 60),
        };
        let mut subscription = Subscription::new("device1".to_string(), 0.0, 180.0);
        subscription.bark_url = "https://api.day.app".to_string();
        subscription.locations = vec![
            crate::models::SubscriptionLocation {
                name: "antipode".to_string(),
                latitude: 0.0,
                longitude: 180.0,
            },
            crate::models::SubscriptionLocation {
                name: "london".to_string(),
                latitude: 51.5072,
                longitude: -0.1276,
            },
        ];
        subscription.notify_bands = vec![crate::models::NotificationBand {
            min: 0,
            max: 99,
            level: "passive".to_string(),
            label: String::new(),
        }];
        let earthquake = CommonEarthquakeInfo {
            event_id: "global1".to_string(),
            report_num: 1,
            latitude: 0.0,
            longitude: 0.0,
            magnitude: 9.0,
            depth: 10.0,
            max_intensity: "7".to_string(),
            region: "test".to_string(),
            origin_time: "2026-07-10 00:00:00".to_string(),
            source_type: "cenc_eew".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        };

        let subscription = SubscriptionSnapshot::new(Arc::new(subscription));
        let target = evaluate_subscription(&config, &subscription, &earthquake, Some(0));
        assert!(
            target.is_some(),
            "a distant valid location must still be evaluated"
        );
        if let Some(target) = target {
            assert_eq!(target.recipient.location_name, "london");
            assert!(target.timing.distance_km > 1_000.0);
        }
    }

    #[tokio::test]
    async fn event_queue_coalesces_updates_without_dropping_distinct_events() {
        let queue = EventQueue::default();
        queue.push(queued_earthquake("event-a", 1)).await;
        queue.push(queued_earthquake("event-a", 3)).await;
        queue.push(queued_earthquake("event-a", 2)).await;
        queue.push(queued_earthquake("event-b", 1)).await;

        let first = queue.pop().await;
        let second = queue.pop().await;
        assert_eq!((first.event_id.as_str(), first.report_num), ("event-a", 3));
        assert_eq!(
            (second.event_id.as_str(), second.report_num),
            ("event-b", 1)
        );
    }

    #[test]
    fn evaluates_one_hundred_thousand_subscription_targets() {
        let config = MonitorConfig {
            websocket_url: String::new(),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
            push_updates: false,
            update_min_report_gap: 1,
            ignore_training: true,
            ignore_cancel: true,
            p_wave_km_s: 6.0,
            s_wave_km_s: 3.5,
            stale_origin_seconds: 600,
            dedup_keep: Duration::from_secs(120 * 60),
        };
        let mut subscription = Subscription::new("device1".to_string(), 35.6762, 139.6503);
        subscription.bark_url = "https://api.day.app".to_string();
        subscription.notify_bands = vec![crate::models::NotificationBand {
            min: 0,
            max: 99,
            level: "passive".to_string(),
            label: String::new(),
        }];
        let snapshot = SubscriptionSnapshot::new(Arc::new(subscription));
        let earthquake = queued_earthquake("performance", 1);

        let matched = (0..100_000)
            .filter(|_| evaluate_subscription(&config, &snapshot, &earthquake, Some(0)).is_some())
            .count();
        assert_eq!(matched, 100_000);
    }
}
