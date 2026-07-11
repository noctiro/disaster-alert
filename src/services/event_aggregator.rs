use crate::models::{DestinationId, DisasterCategory, DisasterEvent};
use crate::utils::distance;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const EARTHQUAKE_TIME_WINDOW_SECONDS: i64 = 120;
const EARTHQUAKE_DISTANCE_KM: f64 = 100.0;
const EARTHQUAKE_MAGNITUDE_DELTA: f64 = 1.0;

#[derive(Clone)]
pub struct EventAggregator {
    keep: Duration,
    state: Arc<Mutex<CorrelationState>>,
    deliveries: Arc<StdMutex<DeliveryState>>,
}

#[derive(Default)]
struct CorrelationState {
    next_incident_id: u64,
    by_source_event: HashMap<String, u64>,
    incidents: VecDeque<Incident>,
    last_cleanup: Option<Instant>,
}

struct Incident {
    id: u64,
    category: DisasterCategory,
    sources: HashSet<String>,
    occurred_epoch: Option<i64>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    magnitude: Option<f64>,
    at: Instant,
}

#[derive(Default)]
struct DeliveryState {
    entries: HashMap<DeliveryKey, DeliveryEntry>,
    next_token: u64,
    last_cleanup: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeliveryKey {
    incident_id: u64,
    category: DisasterCategory,
    destination: DestinationId,
}

#[derive(Default)]
struct DeliveryEntry {
    committed: Option<DeliveryVersion>,
    location_name: Option<String>,
    pending: Option<PendingClaim>,
    seen_revisions: HashSet<(String, String)>,
}

struct PendingClaim {
    token: u64,
    at: Instant,
}

struct DeliveryVersion {
    source: String,
    revision: String,
    report_num: u32,
    level: u8,
    final_report: bool,
    cancel: bool,
    at: Instant,
}

pub enum DeliveryAttempt {
    Acquired(Box<DeliveryPermit>),
    Busy,
    Duplicate,
}

#[cfg(test)]
impl DeliveryAttempt {
    fn is_acquired(&self) -> bool {
        matches!(self, Self::Acquired(_))
    }

    fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate)
    }
}

pub struct DeliveryPermit {
    aggregator: EventAggregator,
    key: DeliveryKey,
    event: DisasterEvent,
    location_name: String,
    token: u64,
    finished: bool,
}

impl EventAggregator {
    pub fn new(keep: Duration) -> Self {
        Self {
            keep,
            state: Arc::new(Mutex::new(CorrelationState::default())),
            deliveries: Arc::new(StdMutex::new(DeliveryState::default())),
        }
    }

    /// Correlates reports without discarding channel provenance.
    pub async fn correlate(&self, event: &DisasterEvent) -> u64 {
        let now = Instant::now();
        let source_key = event.event_key();
        let mut state = self.state.lock().await;
        cleanup_incidents(&mut state, now, self.keep);

        if let Some(incident_id) = state.by_source_event.get(&source_key) {
            let incident_id = *incident_id;
            if let Some(incident) = state
                .incidents
                .iter_mut()
                .find(|item| item.id == incident_id)
            {
                update_incident(incident, event, now);
            }
            return incident_id;
        }

        let occurred_epoch = parse_event_epoch(event);
        let matched = if is_earthquake(event.category) {
            state
                .incidents
                .iter()
                .filter_map(|incident| {
                    earthquake_match_score(incident, event, occurred_epoch)
                        .map(|score| (incident.id, score))
                })
                .min_by(|left, right| left.1.total_cmp(&right.1))
                .map(|(id, _score)| id)
        } else {
            None
        };

        let incident_id = matched.unwrap_or_else(|| {
            state.next_incident_id = state.next_incident_id.wrapping_add(1).max(1);
            let id = state.next_incident_id;
            state.incidents.push_back(Incident {
                id,
                category: event.category,
                sources: HashSet::from([event.source.clone()]),
                occurred_epoch,
                latitude: event.latitude,
                longitude: event.longitude,
                magnitude: event.magnitude,
                at: now,
            });
            id
        });
        if matched.is_some()
            && let Some(incident) = state
                .incidents
                .iter_mut()
                .find(|incident| incident.id == incident_id)
        {
            incident.sources.insert(event.source.clone());
            incident.at = now;
        }
        state.by_source_event.insert(source_key, incident_id);
        incident_id
    }

    /// Claims a recipient-level delivery. Failed permits are released on drop.
    pub async fn begin_delivery(
        &self,
        incident_id: u64,
        destination: DestinationId,
        location_name: String,
        event: &DisasterEvent,
        push_updates: bool,
        min_report_gap: u32,
    ) -> DeliveryAttempt {
        let now = Instant::now();
        let key = DeliveryKey {
            incident_id,
            category: event.category,
            destination,
        };
        let mut deliveries = self.lock_deliveries();
        cleanup_deliveries(&mut deliveries, now, self.keep);
        if let Some(previous) = deliveries.entries.get(&key) {
            if previous.pending.is_some() {
                return DeliveryAttempt::Busy;
            }
            let meaningful = previous.committed.as_ref().is_none_or(|committed| {
                is_meaningful_update(committed, event, push_updates, min_report_gap.max(1))
            });
            if !meaningful
                || (!event.revision.is_empty()
                    && previous
                        .seen_revisions
                        .contains(&(event.source.clone(), event.revision.clone()))
                    && !previous
                        .committed
                        .as_ref()
                        .is_some_and(|committed| safety_transition(committed, event)))
            {
                return DeliveryAttempt::Duplicate;
            }
        }
        deliveries.next_token = deliveries.next_token.wrapping_add(1).max(1);
        let token = deliveries.next_token;
        deliveries.entries.entry(key.clone()).or_default().pending =
            Some(PendingClaim { token, at: now });
        DeliveryAttempt::Acquired(Box::new(DeliveryPermit {
            aggregator: self.clone(),
            key,
            event: event.clone(),
            location_name,
            token,
            finished: false,
        }))
    }

    pub async fn delivered_destinations(
        &self,
        incident_id: u64,
        category: DisasterCategory,
    ) -> HashMap<DestinationId, String> {
        let deliveries = self.lock_deliveries();
        deliveries
            .entries
            .iter()
            .filter(|(key, entry)| {
                key.incident_id == incident_id
                    && key.category == category
                    && entry.committed.is_some()
            })
            .map(|(key, entry)| {
                (
                    key.destination.clone(),
                    entry.location_name.clone().unwrap_or_default(),
                )
            })
            .collect()
    }

    fn lock_deliveries(&self) -> StdMutexGuard<'_, DeliveryState> {
        match self.deliveries.lock() {
            Ok(guard) => guard,
            Err(error) => {
                tracing::error!(event = "delivery.lock_recovered", "delivery.lock_recovered");
                error.into_inner()
            }
        }
    }
}

impl DeliveryPermit {
    pub async fn commit(mut self) {
        let mut deliveries = self.aggregator.lock_deliveries();
        if let Some(entry) = deliveries.entries.get_mut(&self.key)
            && entry
                .pending
                .as_ref()
                .is_some_and(|claim| claim.token == self.token)
        {
            entry.pending = None;
            entry.location_name = Some(self.location_name.clone());
            if !self.event.revision.is_empty() {
                entry
                    .seen_revisions
                    .insert((self.event.source.clone(), self.event.revision.clone()));
            }
            entry.committed = Some(delivery_version(&self.event, Instant::now()));
        }
        self.finished = true;
    }

    pub async fn abort(mut self) {
        self.release();
        self.finished = true;
    }

    fn release(&self) {
        let mut deliveries = self.aggregator.lock_deliveries();
        let remove = if let Some(entry) = deliveries.entries.get_mut(&self.key) {
            if entry
                .pending
                .as_ref()
                .is_some_and(|claim| claim.token == self.token)
            {
                entry.pending = None;
            }
            entry.committed.is_none() && entry.pending.is_none()
        } else {
            false
        };
        if remove {
            deliveries.entries.remove(&self.key);
        }
    }
}

impl Drop for DeliveryPermit {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.release();
    }
}

fn is_earthquake(category: DisasterCategory) -> bool {
    matches!(
        category,
        DisasterCategory::EarthquakeWarning | DisasterCategory::EarthquakeReport
    )
}

fn earthquake_match_score(
    incident: &Incident,
    event: &DisasterEvent,
    occurred_epoch: Option<i64>,
) -> Option<f64> {
    if !is_earthquake(incident.category) {
        return None;
    }
    if incident.sources.contains(&event.source) {
        return None;
    }
    let (Some(left_time), Some(right_time)) = (incident.occurred_epoch, occurred_epoch) else {
        return None;
    };
    let time_delta = left_time.abs_diff(right_time) as f64;
    if time_delta > EARTHQUAKE_TIME_WINDOW_SECONDS as f64 {
        return None;
    }
    let (Some(left_lat), Some(left_lon), Some(right_lat), Some(right_lon)) = (
        incident.latitude,
        incident.longitude,
        event.latitude,
        event.longitude,
    ) else {
        return None;
    };
    let km = distance::vincenty_distance(left_lat, left_lon, right_lat, right_lon)?;
    if km > EARTHQUAKE_DISTANCE_KM {
        return None;
    }
    let magnitude_delta = match (incident.magnitude, event.magnitude) {
        (Some(left), Some(right)) => {
            let delta = (left - right).abs();
            if delta > EARTHQUAKE_MAGNITUDE_DELTA {
                return None;
            }
            delta
        }
        _ => 0.0,
    };
    Some(
        time_delta / EARTHQUAKE_TIME_WINDOW_SECONDS as f64
            + km / EARTHQUAKE_DISTANCE_KM
            + magnitude_delta / EARTHQUAKE_MAGNITUDE_DELTA,
    )
}

fn update_incident(incident: &mut Incident, event: &DisasterEvent, now: Instant) {
    // Preserve the original correlation anchor while filling gaps from later same-source reports.
    if incident.occurred_epoch.is_none() {
        incident.occurred_epoch = parse_event_epoch(event);
    }
    if incident.latitude.is_none() {
        incident.latitude = event.latitude;
    }
    if incident.longitude.is_none() {
        incident.longitude = event.longitude;
    }
    if incident.magnitude.is_none() {
        incident.magnitude = event.magnitude;
    }
    incident.at = now;
}

fn cleanup_incidents(state: &mut CorrelationState, now: Instant, keep: Duration) {
    if state
        .last_cleanup
        .is_some_and(|last| now.duration_since(last) < Duration::from_secs(60))
    {
        return;
    }
    state.last_cleanup = Some(now);
    state
        .incidents
        .retain(|incident| now.duration_since(incident.at) <= keep);
    let active_ids = state
        .incidents
        .iter()
        .map(|incident| incident.id)
        .collect::<std::collections::HashSet<_>>();
    state
        .by_source_event
        .retain(|_, id| active_ids.contains(id));
}

fn cleanup_deliveries(state: &mut DeliveryState, now: Instant, keep: Duration) {
    if state
        .last_cleanup
        .is_some_and(|last| now.duration_since(last) < Duration::from_secs(60))
    {
        return;
    }
    state.last_cleanup = Some(now);
    state.entries.retain(|_, entry| {
        entry
            .committed
            .as_ref()
            .is_some_and(|version| now.duration_since(version.at) <= keep)
            || entry
                .pending
                .as_ref()
                .is_some_and(|claim| now.duration_since(claim.at) <= keep)
    });
}

fn delivery_version(event: &DisasterEvent, at: Instant) -> DeliveryVersion {
    DeliveryVersion {
        source: event.source.clone(),
        revision: event.revision.clone(),
        report_num: event.report_num,
        level: event.level,
        final_report: event.final_report,
        cancel: event.cancel,
        at,
    }
}

fn is_meaningful_update(
    previous: &DeliveryVersion,
    event: &DisasterEvent,
    push_updates: bool,
    min_report_gap: u32,
) -> bool {
    let safety_transition = safety_transition(previous, event);
    if event.source != previous.source {
        return safety_transition;
    }
    if previous.cancel && !event.cancel
        || previous.final_report && !event.final_report && !event.cancel
    {
        return false;
    }
    if event.report_num < previous.report_num {
        return false;
    }
    safety_transition
        || (push_updates
            && (event.report_num.saturating_sub(previous.report_num) >= min_report_gap
                || (event.report_num == previous.report_num
                    && !event.revision.is_empty()
                    && event.revision != previous.revision)))
}

fn safety_transition(previous: &DeliveryVersion, event: &DisasterEvent) -> bool {
    event.cancel && !previous.cancel
        || event.final_report && !previous.final_report
        || event.level > previous.level
}

pub(crate) fn parse_event_epoch(event: &DisasterEvent) -> Option<i64> {
    let offset_seconds = if matches!(
        event.source.as_str(),
        "wolfx.jma_eew" | "fanstudio.jma" | "fanstudio.kma" | "fanstudio.kma-eew"
    ) {
        9 * 3600
    } else {
        8 * 3600
    };
    parse_datetime_epoch_seconds(&event.occurred_at, offset_seconds)
}

fn parse_datetime_epoch_seconds(value: &str, offset_seconds: i64) -> Option<i64> {
    let (date, raw_time) = value.trim().split_once([' ', 'T'])?;
    let mut date_parts = date.split(['-', '/']);
    let year = date_parts.next()?.parse::<i64>().ok()?;
    let month = date_parts.next()?.parse::<i64>().ok()?;
    let day = date_parts.next()?.parse::<i64>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (time, explicit_offset) = parse_time_offset(raw_time)?;
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i64>().ok()?;
    let minute = time_parts.next()?.parse::<i64>().ok()?;
    let second = time_parts.next().map_or(Some(0), |item| {
        item.split('.').next().and_then(|part| part.parse().ok())
    })?;
    if time_parts.next().is_some() {
        return None;
    }
    if !(1970..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    Some(
        days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second
            - explicit_offset.unwrap_or(offset_seconds),
    )
}

fn parse_time_offset(value: &str) -> Option<(&str, Option<i64>)> {
    if let Some(time) = value.strip_suffix('Z') {
        return Some((time, Some(0)));
    }
    let offset_index = value
        .char_indices()
        .skip(1)
        .find(|(_, character)| matches!(character, '+' | '-'))
        .map(|(index, _)| index);
    let Some(index) = offset_index else {
        return Some((value, None));
    };
    let (time, offset) = value.split_at(index);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let mut parts = offset[1..].split(':');
    let hours = parts.next()?.parse::<i64>().ok()?;
    let minutes = parts.next().unwrap_or("0").parse::<i64>().ok()?;
    if parts.next().is_some() || hours > 23 || minutes > 59 {
        return None;
    }
    Some((time, Some(sign * (hours * 3_600 + minutes * 60))))
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let adjusted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ProviderChannel;

    fn destination(device_key: &str) -> DestinationId {
        DestinationId {
            base_url: "https://api.day.app".to_string(),
            device_key: device_key.to_string(),
        }
    }

    fn earthquake(source: &str, event_id: &str, report_num: u32, lat: f64) -> DisasterEvent {
        DisasterEvent {
            category: DisasterCategory::EarthquakeWarning,
            channel: if source.starts_with("wolfx.") {
                ProviderChannel::Wolfx
            } else {
                ProviderChannel::FanStudio
            },
            source: source.to_string(),
            event_id: event_id.to_string(),
            revision: report_num.to_string(),
            report_num,
            title: "earthquake".to_string(),
            description: String::new(),
            latitude: Some(lat),
            longitude: Some(105.0),
            magnitude: Some(5.2),
            depth_km: Some(10.0),
            affected_regions: Vec::new(),
            radius_km: None,
            level: 2,
            occurred_at: "2026-07-10 12:34:56".to_string(),
            final_report: false,
            cancel: false,
            training: false,
        }
    }

    #[tokio::test]
    async fn correlates_close_cross_channel_reports_without_discarding_sources() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let wolfx = earthquake("wolfx.jma_eew", "wolfx-1", 1, 35.0);
        let fanstudio = earthquake("fanstudio.jma", "fan-1", 1, 35.2);
        let first = aggregator.correlate(&wolfx).await;
        let second = aggregator.correlate(&fanstudio).await;
        assert_eq!(first, second);

        let wolfx_permit = aggregator
            .begin_delivery(
                first,
                destination("wolfx-only"),
                String::new(),
                &wolfx,
                true,
                1,
            )
            .await;
        assert!(matches!(wolfx_permit, DeliveryAttempt::Acquired(_)));
        if let DeliveryAttempt::Acquired(permit) = wolfx_permit {
            permit.commit().await;
        }
        assert!(
            aggregator
                .begin_delivery(
                    second,
                    destination("fan-only"),
                    String::new(),
                    &fanstudio,
                    true,
                    1
                )
                .await
                .is_acquired()
        );
        assert!(
            aggregator
                .begin_delivery(
                    second,
                    destination("wolfx-only"),
                    String::new(),
                    &fanstudio,
                    true,
                    1
                )
                .await
                .is_duplicate()
        );
    }

    #[test]
    fn interprets_fanstudio_jma_time_as_jst() {
        let wolfx = earthquake("wolfx.jma_eew", "wolfx-1", 1, 35.0);
        let fanstudio = earthquake("fanstudio.jma", "fan-1", 1, 35.0);
        assert_eq!(
            parse_event_epoch(&wolfx),
            parse_event_epoch(&fanstudio),
            "JMA reports use Japan Standard Time on both channels"
        );
    }

    #[tokio::test]
    async fn does_not_correlate_distant_earthquakes() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = aggregator
            .correlate(&earthquake("wolfx.jma_eew", "a", 1, 35.0))
            .await;
        let second = aggregator
            .correlate(&earthquake("fanstudio.jma", "b", 1, 38.0))
            .await;
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn does_not_correlate_distinct_same_source_earthquakes() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = aggregator
            .correlate(&earthquake("fanstudio.jma", "a", 1, 35.0))
            .await;
        let second = aggregator
            .correlate(&earthquake("fanstudio.jma", "b", 1, 35.1))
            .await;
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn correlates_close_reports_from_distinct_fanstudio_sources() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = aggregator
            .correlate(&earthquake("fanstudio.cenc", "a", 1, 35.0))
            .await;
        let second = aggregator
            .correlate(&earthquake("fanstudio.usgs", "b", 1, 35.1))
            .await;
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn revision_deduplication_is_scoped_to_source() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = earthquake("wolfx.jma_eew", "a", 1, 35.0);
        let mut cross_source = earthquake("fanstudio.jma", "b", 1, 35.1);
        cross_source.level = 3;
        let incident = aggregator.correlate(&first).await;
        assert_eq!(incident, aggregator.correlate(&cross_source).await);
        let permit = match aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &first,
                true,
                1,
            )
            .await
        {
            DeliveryAttempt::Acquired(permit) => permit,
            other => {
                assert!(matches!(other, DeliveryAttempt::Acquired(_)));
                return;
            }
        };
        permit.commit().await;
        assert!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &cross_source,
                    true,
                    1
                )
                .await
                .is_acquired()
        );
    }

    #[tokio::test]
    async fn failed_delivery_is_released() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let event = earthquake("fanstudio.jma", "a", 1, 35.0);
        let incident = aggregator.correlate(&event).await;
        let permit = aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &event,
                true,
                1,
            )
            .await;
        assert!(permit.is_acquired());
        drop(permit);
        tokio::task::yield_now().await;
        assert!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &event,
                    true,
                    1
                )
                .await
                .is_acquired()
        );
    }

    #[tokio::test]
    async fn in_flight_update_is_busy_instead_of_duplicate() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = earthquake("wolfx.jma_eew", "a", 1, 35.0);
        let mut final_report = first.clone();
        final_report.report_num = 2;
        final_report.final_report = true;
        let incident = aggregator.correlate(&first).await;
        let claim = aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &first,
                true,
                1,
            )
            .await;
        assert!(claim.is_acquired());
        assert!(matches!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &final_report,
                    true,
                    1
                )
                .await,
            DeliveryAttempt::Busy
        ));
    }

    #[tokio::test]
    async fn failed_update_preserves_the_last_committed_version() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = earthquake("wolfx.jma_eew", "a", 1, 35.0);
        let mut update = first.clone();
        update.report_num = 2;
        update.revision = "2".to_string();
        let incident = aggregator.correlate(&first).await;
        let permit = match aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &first,
                true,
                1,
            )
            .await
        {
            DeliveryAttempt::Acquired(permit) => permit,
            other => {
                assert!(matches!(other, DeliveryAttempt::Acquired(_)));
                return;
            }
        };
        permit.commit().await;
        let permit = match aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &update,
                true,
                1,
            )
            .await
        {
            DeliveryAttempt::Acquired(permit) => permit,
            other => {
                assert!(matches!(other, DeliveryAttempt::Acquired(_)));
                return;
            }
        };
        permit.abort().await;
        assert!(matches!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &first,
                    true,
                    1
                )
                .await,
            DeliveryAttempt::Duplicate
        ));
        assert!(matches!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &update,
                    true,
                    1
                )
                .await,
            DeliveryAttempt::Acquired(_)
        ));
    }

    #[tokio::test]
    async fn safety_transition_is_not_suppressed_by_reused_revision() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let first = earthquake("wolfx.jma_eew", "a", 1, 35.0);
        let mut cancelled = first.clone();
        cancelled.cancel = true;
        let incident = aggregator.correlate(&first).await;
        let permit = match aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &first,
                true,
                1,
            )
            .await
        {
            DeliveryAttempt::Acquired(permit) => permit,
            other => {
                assert!(matches!(other, DeliveryAttempt::Acquired(_)));
                return;
            }
        };
        permit.commit().await;
        assert!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &cancelled,
                    true,
                    1
                )
                .await
                .is_acquired()
        );
    }

    #[tokio::test]
    async fn cancellation_after_a_final_report_is_delivered() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let mut final_report = earthquake("wolfx.jma_eew", "a", 2, 35.0);
        final_report.final_report = true;
        let mut cancelled = final_report.clone();
        cancelled.cancel = true;
        let incident = aggregator.correlate(&final_report).await;
        let permit = match aggregator
            .begin_delivery(
                incident,
                destination("device"),
                String::new(),
                &final_report,
                true,
                1,
            )
            .await
        {
            DeliveryAttempt::Acquired(permit) => permit,
            other => {
                assert!(matches!(other, DeliveryAttempt::Acquired(_)));
                return;
            }
        };
        permit.commit().await;

        assert!(
            aggregator
                .begin_delivery(
                    incident,
                    destination("device"),
                    String::new(),
                    &cancelled,
                    true,
                    1
                )
                .await
                .is_acquired()
        );
    }

    #[tokio::test]
    async fn later_same_source_reports_fill_missing_correlation_fields() {
        let aggregator = EventAggregator::new(Duration::from_secs(60));
        let mut incomplete = earthquake("wolfx.jma_eew", "a", 1, 35.0);
        incomplete.latitude = None;
        incomplete.longitude = None;
        incomplete.magnitude = None;
        incomplete.occurred_at.clear();
        let mut enriched = incomplete.clone();
        enriched.latitude = Some(35.0);
        enriched.longitude = Some(105.0);
        enriched.magnitude = Some(5.2);
        enriched.occurred_at = "2026-07-10 12:34:56".to_string();
        let cross_source = earthquake("fanstudio.jma", "b", 1, 35.1);

        let first = aggregator.correlate(&incomplete).await;
        assert_eq!(first, aggregator.correlate(&enriched).await);
        assert_eq!(first, aggregator.correlate(&cross_source).await);
    }
}
