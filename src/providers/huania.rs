use super::ProviderCursor;
use crate::models::{DisasterCategory, DisasterEvent, ProviderChannel};
use crate::runtime::{EventRuntime, RuntimeStatus};
use anyhow::{Context, Result, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use reqwest::Url;
use serde::{Deserialize, de::DeserializeOwned};
use std::time::Duration;
use tokio::sync::watch;

const HUANIA_API_URL_BASE64: &str = concat!(
    "aHR0cHM6Ly9tb2Jp",
    "bGUtbmV3LmNoaW5h",
    "ZWV3LmNuL3YxL2Vh",
    "cmx5d2FybmluZ3M=",
);
const HUANIA_CURSOR_STREAM: &str = "earlywarning";
const HUANIA_POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_SNAPSHOT_EVENTS: usize = 128;
const MAX_TRACKED_EVENTS: usize = MAX_SNAPSHOT_EVENTS;
const MAX_REPORTS_PER_EVENT: u32 = 128;
const START_AT_LOOKBACK_MILLIS: i64 = 60_000;

#[derive(Clone)]
pub(crate) struct HuaniaSource {
    event_runtime: EventRuntime,
    client: reqwest::Client,
    endpoint: Url,
    reconnect_min: Duration,
    reconnect_max: Duration,
    runtime_status: RuntimeStatus,
}

impl HuaniaSource {
    pub(crate) fn new(
        config: &crate::config::Config,
        event_runtime: EventRuntime,
        runtime_status: RuntimeStatus,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent("disaster-alert/1.0 (https://github.com/noctiro/disaster-alert)")
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(2)
            .build()
            .context("failed to build Huania HTTP client")?;
        Ok(Self {
            event_runtime,
            client,
            endpoint: huania_api_url()?,
            reconnect_min: Duration::from_secs(config.reconnect_min_seconds),
            reconnect_max: Duration::from_secs(config.reconnect_max_seconds),
            runtime_status,
        })
    }

    pub(crate) async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut known = self.load_cursor().await?;
        let mut delay = self.reconnect_min;
        loop {
            if *shutdown.borrow() {
                break;
            }
            let result = tokio::select! {
                biased;
                result = self.poll(known.as_ref()) => result,
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            };
            match result {
                Ok(poll) => {
                    self.runtime_status.huania().set_connected(true);
                    self.runtime_status.huania().record_message();
                    if poll.changed {
                        let cursor =
                            ProviderCursor::new(HUANIA_CURSOR_STREAM, poll.cursor.encode()?)?;
                        let accepted = self
                            .event_runtime
                            .submit_provider_snapshot_batch(
                                ProviderChannel::Huania,
                                poll.events,
                                Some(cursor),
                            )
                            .await;
                        if !accepted {
                            bail!("Huania event batch was not durably committed");
                        }
                        known = Some(poll.cursor);
                    }
                    delay = self.reconnect_min;
                    if wait_or_shutdown(HUANIA_POLL_INTERVAL, &mut shutdown).await {
                        break;
                    }
                }
                Err(error) => {
                    self.runtime_status.huania().set_connected(false);
                    self.runtime_status.huania().record_reconnect();
                    tracing::error!(
                        event = "huania.poll_failed",
                        error = ?error,
                        "huania.poll_failed"
                    );
                    if wait_or_shutdown(delay, &mut shutdown).await {
                        break;
                    }
                    delay = delay.saturating_mul(2).min(self.reconnect_max);
                }
            }
        }
        self.runtime_status.huania().set_connected(false);
        Ok(())
    }

    async fn load_cursor(&self) -> Result<Option<HuaniaCursor>> {
        let cursors = self
            .event_runtime
            .provider_cursors(
                ProviderChannel::Huania,
                vec![HUANIA_CURSOR_STREAM.to_string()],
            )
            .await?;
        cursors
            .first()
            .map(|cursor| HuaniaCursor::decode(cursor.value()))
            .transpose()
    }

    async fn poll(&self, known: Option<&HuaniaCursor>) -> Result<HuaniaPoll> {
        let request = known.map_or_else(HuaniaRequestCursor::default, HuaniaCursor::request);
        let snapshot = self.fetch_warnings(request).await?;
        let Some(known) = known else {
            let cursor = HuaniaCursor::from_initial_snapshot(&snapshot).inspect_err(|error| {
                self.runtime_status.huania().record_parse_error();
                tracing::warn!(
                    event = "huania.initial_snapshot_invalid",
                    error = ?error,
                    "huania.initial_snapshot_invalid"
                );
            })?;
            return Ok(HuaniaPoll {
                events: Vec::new(),
                cursor,
                changed: true,
            });
        };
        if snapshot.is_empty() {
            return Ok(HuaniaPoll {
                events: Vec::new(),
                cursor: known.clone(),
                changed: false,
            });
        }

        let mut cursor = known.clone();
        let mut events = Vec::new();
        for summary in snapshot {
            let event_id = summary.event_id;
            let updates = summary.updates;
            let needs_update = match cursor.needs_update(&summary) {
                Ok(needs_update) => needs_update,
                Err(error) => {
                    self.record_parse_error(event_id, updates, &error);
                    return Err(error).context("invalid Huania event summary");
                }
            };
            if !needs_update {
                continue;
            }
            let reports = self.fetch_details(event_id).await?;
            match cursor.apply_reports(summary, reports) {
                Ok(processed) => events.extend(processed),
                Err(error) => {
                    self.record_parse_error(event_id, updates, &error);
                    return Err(error).context("failed to normalize Huania reports");
                }
            }
        }
        let changed = cursor != *known;
        Ok(HuaniaPoll {
            events,
            cursor,
            changed,
        })
    }

    fn record_parse_error(&self, event_id: i64, updates: u32, error: &anyhow::Error) {
        self.runtime_status.huania().record_parse_error();
        tracing::warn!(
            event = "huania.event_parse_failed",
            event_id,
            updates,
            error = ?error,
            "huania.event_parse_failed"
        );
    }

    async fn fetch_warnings(
        &self,
        request: HuaniaRequestCursor,
    ) -> Result<Vec<HuaniaEarthquakeDto>> {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("updates", "0")
            .append_pair("start_at", &request.start_at.to_string());
        self.fetch(url).await
    }

    async fn fetch_details(&self, event_id: i64) -> Result<Vec<HuaniaEarthquakeDto>> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("Huania API URL cannot contain path segments"))?
            .push(&event_id.to_string());
        self.fetch(url).await
    }

    async fn fetch(&self, url: Url) -> Result<Vec<HuaniaEarthquakeDto>> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Huania request failed")?
            .error_for_status()
            .context("Huania service returned an HTTP error")?;
        let response = limited_response_json::<HuaniaResponse>(response).await?;
        if response.code != 0 {
            bail!(
                "Huania service returned code {}: {}",
                response.code,
                response.message
            );
        }
        if response.data.len() > MAX_SNAPSHOT_EVENTS {
            bail!("Huania response contains too many events");
        }
        Ok(response.data)
    }
}

fn huania_api_url() -> Result<Url> {
    let decoded = STANDARD
        .decode(HUANIA_API_URL_BASE64)
        .context("invalid Huania API URL encoding")?;
    let decoded = std::str::from_utf8(&decoded).context("Huania API URL is not valid UTF-8")?;
    Url::parse(decoded).context("invalid Huania API URL constant")
}

async fn wait_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        result = shutdown.changed() => result.is_err() || *shutdown.borrow(),
        () = tokio::time::sleep(delay) => false,
    }
}

async fn limited_response_json<T: DeserializeOwned>(mut response: reqwest::Response) -> Result<T> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        bail!("Huania response exceeds {MAX_RESPONSE_BYTES} bytes");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("failed to read Huania response")?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            bail!("Huania response exceeds {MAX_RESPONSE_BYTES} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).context("failed to parse Huania response")
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HuaniaResponse {
    code: i64,
    #[serde(default)]
    message: String,
    #[serde(default)]
    data: Vec<HuaniaEarthquakeDto>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HuaniaEarthquakeDto {
    event_id: i64,
    updates: u32,
    latitude: f64,
    longitude: f64,
    depth: Option<f64>,
    epicenter: String,
    start_at: i64,
    update_at: i64,
    magnitude: f64,
    epi_intensity: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct HuaniaRequestCursor {
    start_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HuaniaEventCursor {
    event_id: i64,
    start_at: i64,
    updates: u32,
    summary_update_at: i64,
    detail_update_at: i64,
    detail_known: bool,
}

impl HuaniaEventCursor {
    fn from_summary(
        report: &HuaniaEarthquakeDto,
        detail_update_at: i64,
        detail_known: bool,
    ) -> Result<Self> {
        Self::new(
            report.event_id,
            report.start_at,
            report.updates,
            report.update_at,
            detail_update_at,
            detail_known,
        )
    }

    fn new(
        event_id: i64,
        start_at: i64,
        updates: u32,
        summary_update_at: i64,
        detail_update_at: i64,
        detail_known: bool,
    ) -> Result<Self> {
        let cursor = Self {
            event_id,
            start_at,
            updates,
            summary_update_at,
            detail_update_at,
            detail_known,
        };
        validate_event_cursor(cursor)?;
        Ok(cursor)
    }

    fn summary_changed(self, summary: &HuaniaEarthquakeDto) -> bool {
        !self.detail_known
            || summary.updates > self.updates
            || summary.updates == self.updates
                && (summary.start_at != self.start_at
                    || summary.update_at != self.summary_update_at)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HuaniaCursor {
    events: Vec<HuaniaEventCursor>,
}

impl HuaniaCursor {
    fn from_initial_snapshot(snapshot: &[HuaniaEarthquakeDto]) -> Result<Self> {
        if snapshot.is_empty() {
            bail!("Huania initial snapshot is empty");
        }
        let mut cursor = Self {
            events: Vec::with_capacity(snapshot.len()),
        };
        for report in snapshot {
            normalize(report.clone()).with_context(|| {
                format!(
                    "invalid Huania baseline event {} revision {}",
                    report.event_id, report.updates
                )
            })?;
            cursor.record(HuaniaEventCursor::from_summary(report, 0, false)?);
        }
        Ok(cursor)
    }

    fn request(&self) -> HuaniaRequestCursor {
        HuaniaRequestCursor {
            start_at: self
                .events
                .iter()
                .map(|cursor| cursor.start_at)
                .min()
                .unwrap_or_default()
                .saturating_sub(START_AT_LOOKBACK_MILLIS),
        }
    }

    fn event(&self, event_id: i64) -> Option<&HuaniaEventCursor> {
        self.events
            .binary_search_by_key(&event_id, |cursor| cursor.event_id)
            .ok()
            .map(|index| &self.events[index])
    }

    fn record(&mut self, event: HuaniaEventCursor) {
        match self
            .events
            .binary_search_by_key(&event.event_id, |cursor| cursor.event_id)
        {
            Ok(index) => {
                let current = &mut self.events[index];
                if event.updates > current.updates {
                    *current = event;
                } else if event.updates == current.updates {
                    current.start_at = event.start_at;
                    current.summary_update_at = event.summary_update_at;
                    current.detail_update_at = current.detail_update_at.max(event.detail_update_at);
                    current.detail_known |= event.detail_known;
                }
            }
            Err(index) => self.events.insert(index, event),
        }
        if self.events.len() > MAX_TRACKED_EVENTS
            && let Some((oldest, _)) = self
                .events
                .iter()
                .enumerate()
                .min_by_key(|(_, cursor)| (cursor.start_at, cursor.event_id))
        {
            self.events.remove(oldest);
        }
    }

    fn needs_update(&self, summary: &HuaniaEarthquakeDto) -> Result<bool> {
        let candidate = HuaniaEventCursor::from_summary(summary, 0, false)?;
        Ok(self
            .event(candidate.event_id)
            .is_none_or(|previous| previous.summary_changed(summary)))
    }

    fn apply_reports(
        &mut self,
        summary: HuaniaEarthquakeDto,
        reports: Vec<HuaniaEarthquakeDto>,
    ) -> Result<Vec<DisasterEvent>> {
        let previous = self.event(summary.event_id).copied();
        let processed = normalize_reports(summary, previous, reports)?;
        self.record(processed.cursor);
        Ok(processed.events)
    }

    fn encode(&self) -> Result<String> {
        if self.events.is_empty() || self.events.len() > MAX_TRACKED_EVENTS {
            bail!("invalid Huania cursor event count");
        }
        let count = u16::try_from(self.events.len()).context("too many Huania cursor events")?;
        let mut bytes = Vec::with_capacity(2 + self.events.len() * 37);
        bytes.extend_from_slice(&count.to_be_bytes());
        for event in &self.events {
            validate_event_cursor(*event)?;
            bytes.extend_from_slice(&event.event_id.to_be_bytes());
            bytes.extend_from_slice(&event.start_at.to_be_bytes());
            bytes.extend_from_slice(&event.updates.to_be_bytes());
            bytes.extend_from_slice(&event.summary_update_at.to_be_bytes());
            bytes.extend_from_slice(&event.detail_update_at.to_be_bytes());
            bytes.push(u8::from(event.detail_known));
        }
        Ok(format!("v2:{}", URL_SAFE_NO_PAD.encode(bytes)))
    }

    fn decode(value: &str) -> Result<Self> {
        if let Some(encoded) = value.strip_prefix("v1:") {
            return Self::decode_v1(encoded);
        }
        let encoded = value
            .strip_prefix("v2:")
            .context("unsupported Huania cursor format")?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("invalid Huania cursor encoding")?;
        if bytes.len() < 2 {
            bail!("invalid Huania cursor length");
        }
        let count = usize::from(u16::from_be_bytes(bytes[..2].try_into().unwrap_or([0; 2])));
        if count == 0 || count > MAX_TRACKED_EVENTS || bytes.len() != 2 + count * 37 {
            bail!("invalid Huania cursor event count");
        }
        let mut events = Vec::with_capacity(count);
        for entry in bytes[2..].chunks_exact(37) {
            let event = HuaniaEventCursor {
                event_id: i64::from_be_bytes(entry[..8].try_into().unwrap_or([0; 8])),
                start_at: i64::from_be_bytes(entry[8..16].try_into().unwrap_or([0; 8])),
                updates: u32::from_be_bytes(entry[16..20].try_into().unwrap_or([0; 4])),
                summary_update_at: i64::from_be_bytes(entry[20..28].try_into().unwrap_or([0; 8])),
                detail_update_at: i64::from_be_bytes(entry[28..36].try_into().unwrap_or([0; 8])),
                detail_known: match entry[36] {
                    0 => false,
                    1 => true,
                    _ => bail!("invalid Huania cursor detail flag"),
                },
            };
            validate_event_cursor(event)?;
            if events
                .last()
                .is_some_and(|previous: &HuaniaEventCursor| previous.event_id >= event.event_id)
            {
                bail!("Huania cursor events are not uniquely sorted");
            }
            events.push(event);
        }
        Ok(Self { events })
    }

    fn decode_v1(encoded: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("invalid Huania cursor encoding")?;
        if bytes.len() != 20 {
            bail!("invalid Huania cursor entries");
        }
        let cursor = HuaniaEventCursor {
            event_id: i64::from_be_bytes(bytes[..8].try_into().unwrap_or([0; 8])),
            start_at: i64::from_be_bytes(bytes[8..16].try_into().unwrap_or([0; 8])),
            updates: u32::from_be_bytes(bytes[16..].try_into().unwrap_or([0; 4])),
            summary_update_at: 0,
            detail_update_at: 0,
            detail_known: false,
        };
        validate_event_cursor(cursor)?;
        Ok(Self {
            events: vec![cursor],
        })
    }
}

fn validate_event_cursor(cursor: HuaniaEventCursor) -> Result<()> {
    if cursor.event_id <= 0
        || cursor.start_at <= 0
        || cursor.updates == 0
        || cursor.summary_update_at < 0
        || cursor.detail_update_at < 0
    {
        bail!("invalid Huania event cursor entries");
    }
    Ok(())
}

struct ProcessedHuaniaReports {
    events: Vec<DisasterEvent>,
    cursor: HuaniaEventCursor,
}

fn normalize_reports(
    summary: HuaniaEarthquakeDto,
    previous: Option<HuaniaEventCursor>,
    mut reports: Vec<HuaniaEarthquakeDto>,
) -> Result<ProcessedHuaniaReports> {
    let event_id = summary.event_id;
    let latest_updates = summary.updates;
    let previous_updates = previous.map_or(0, |cursor| cursor.updates);
    let first_updates = if latest_updates > previous_updates {
        previous_updates.saturating_add(1)
    } else {
        latest_updates
    };
    let report_count = latest_updates
        .checked_sub(first_updates)
        .and_then(|count| count.checked_add(1))
        .context("invalid Huania report sequence")?;
    if report_count > MAX_REPORTS_PER_EVENT {
        bail!("Huania report sequence exceeds the supported range");
    }

    let summary_start_at = summary.start_at;
    let summary_update_at = summary.update_at;
    let detail_known = previous.is_some_and(|cursor| cursor.detail_known);
    reports.retain(|report| report.event_id == event_id && report.updates <= latest_updates);
    reports.sort_unstable_by_key(|report| (report.updates, std::cmp::Reverse(report.update_at)));
    reports.dedup_by_key(|report| report.updates);
    if !reports
        .iter()
        .any(|report| report.updates == latest_updates)
    {
        reports.push(summary);
    }

    let mut events = Vec::new();
    let mut latest_detail_update_at = previous.map_or(0, |cursor| cursor.detail_update_at);
    for updates in first_updates..=latest_updates {
        let index = reports
            .iter()
            .position(|report| report.updates == updates)
            .with_context(|| {
                format!("Huania event {event_id} is missing report revision {updates}")
            })?;
        let report = reports.remove(index);
        let detail_update_at = report.update_at;
        if updates > previous_updates
            || detail_known
                && updates == previous_updates
                && detail_update_at > latest_detail_update_at
        {
            events.push(normalize(report).with_context(|| {
                format!("invalid Huania event {event_id} report revision {updates}")
            })?);
        }
        if updates == latest_updates {
            latest_detail_update_at = if !detail_known || latest_updates > previous_updates {
                detail_update_at
            } else {
                latest_detail_update_at.max(detail_update_at)
            };
        }
    }

    let cursor = HuaniaEventCursor::new(
        event_id,
        summary_start_at,
        latest_updates,
        summary_update_at,
        latest_detail_update_at,
        true,
    )?;
    Ok(ProcessedHuaniaReports { events, cursor })
}

struct HuaniaPoll {
    events: Vec<DisasterEvent>,
    cursor: HuaniaCursor,
    changed: bool,
}

fn normalize(earthquake: HuaniaEarthquakeDto) -> Result<DisasterEvent> {
    if earthquake.event_id <= 0 {
        bail!("Huania event ID must be positive");
    }
    if earthquake.updates == 0 {
        bail!("Huania updates must be positive");
    }
    if earthquake.start_at < 0 || earthquake.update_at < 0 {
        bail!("Huania timestamps must be non-negative");
    }
    if !crate::utils::distance::validate_coordinates(earthquake.latitude, earthquake.longitude) {
        bail!("Huania coordinates are invalid");
    }
    if !earthquake.magnitude.is_finite() || !(0.0..=10.0).contains(&earthquake.magnitude) {
        bail!("Huania magnitude is invalid");
    }
    if earthquake
        .depth
        .is_some_and(|depth| !depth.is_finite() || depth < 0.0)
    {
        bail!("Huania depth is invalid");
    }
    let occurred_at = epoch_millis_to_rfc3339(earthquake.start_at)?;
    let place = earthquake.epicenter.trim();
    if place.is_empty() {
        bail!("Huania epicenter is empty");
    }
    let description = earthquake
        .epi_intensity
        .filter(|value| value.is_finite())
        .map_or_else(
            || format!("M{:.1} {place}", earthquake.magnitude),
            |intensity| {
                format!(
                    "M{:.1} 最大烈度{intensity:.1} {place}",
                    earthquake.magnitude
                )
            },
        );
    Ok(DisasterEvent {
        category: DisasterCategory::EarthquakeWarning,
        channel: ProviderChannel::Huania,
        source: "huania.earlywarning".to_string(),
        event_id: earthquake.event_id.to_string(),
        revision: earthquake.update_at.to_string(),
        report_num: earthquake.updates,
        title: format!("地震预警 {place}"),
        description,
        latitude: Some(earthquake.latitude),
        longitude: Some(earthquake.longitude),
        magnitude: Some(earthquake.magnitude),
        depth_km: earthquake.depth,
        affected_regions: Vec::new(),
        radius_km: None,
        level: severity_from_magnitude(earthquake.magnitude),
        occurred_at,
        final_report: false,
        cancel: false,
        training: false,
    })
}

fn severity_from_magnitude(magnitude: f64) -> u8 {
    if magnitude >= 7.0 {
        4
    } else if magnitude >= 6.0 {
        3
    } else if magnitude >= 5.0 {
        2
    } else {
        1
    }
}

fn epoch_millis_to_rfc3339(value: i64) -> Result<String> {
    let seconds = value.div_euclid(1_000);
    let millis = value.rem_euclid(1_000);
    let days = seconds.div_euclid(86_400);
    let day_seconds = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    if !(1970..=9999).contains(&year) {
        bail!("Huania timestamp is outside the supported date range");
    }
    let hour = day_seconds / 3_600;
    let minute = day_seconds.rem_euclid(3_600) / 60;
    let second = day_seconds.rem_euclid(60);
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z"
    ))
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = (if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    })
    .div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524
        - day_of_era / 146_096)
        .div_euclid(365);
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_part = (5 * day_of_year + 2).div_euclid(153);
    let day = day_of_year - (153 * month_part + 2).div_euclid(5) + 1;
    let month = month_part + if month_part < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(updates: u32) -> HuaniaEarthquakeDto {
        report(1_784_343_985, updates, 1_784_353_639_800, 1_784_353_653_622)
    }

    fn report(event_id: i64, updates: u32, start_at: i64, update_at: i64) -> HuaniaEarthquakeDto {
        HuaniaEarthquakeDto {
            event_id,
            updates,
            latitude: 38.738,
            longitude: 75.102,
            depth: None,
            epicenter: "新疆克孜勒苏州阿克陶县".to_string(),
            start_at,
            update_at,
            magnitude: 4.4,
            epi_intensity: Some(5.6),
        }
    }

    #[test]
    fn parses_and_normalizes_huania_response() -> Result<()> {
        let response: HuaniaResponse = serde_json::from_str(
            r#"{
                "code": 0,
                "message": "",
                "data": [{
                    "eventId": 1784343985,
                    "updates": 2,
                    "latitude": 38.738,
                    "longitude": 75.102,
                    "epicenter": "新疆克孜勒苏州阿克陶县",
                    "startAt": 1784353639800,
                    "updateAt": 1784353653622,
                    "magnitude": 4.4,
                    "sourceType": 2,
                    "epiIntensity": 5.6
                }]
            }"#,
        )?;
        let dto = response.data.into_iter().next().context("missing event")?;
        let event = normalize(dto)?;
        anyhow::ensure!(event.source == "huania.earlywarning");
        anyhow::ensure!(event.event_id == "1784343985");
        anyhow::ensure!(event.report_num == 2);
        anyhow::ensure!(event.revision == "1784353653622");
        anyhow::ensure!(event.occurred_at == "2026-07-18T05:47:19.800Z");
        Ok(())
    }

    #[test]
    fn cursor_tracks_revisions_independently_per_event() -> Result<()> {
        let older = report(10, 1, 1_000, 1_010);
        let newer = report(20, 1, 2_000, 2_010);
        let mut cursor = HuaniaCursor::from_initial_snapshot(&[older, newer])?;
        let older_revision = report(10, 2, 1_005, 1_020);

        anyhow::ensure!(cursor.needs_update(&older_revision)?);
        let events = cursor.apply_reports(older_revision.clone(), vec![older_revision])?;

        anyhow::ensure!(events.len() == 1);
        anyhow::ensure!(events[0].event_id == "10");
        anyhow::ensure!(events[0].report_num == 2);
        anyhow::ensure!(cursor.event(10).map(|entry| entry.updates) == Some(2));
        anyhow::ensure!(cursor.event(20).map(|entry| entry.updates) == Some(1));
        Ok(())
    }

    #[test]
    fn invalid_report_does_not_advance_its_cursor() -> Result<()> {
        let baseline = report(10, 1, 1_000, 1_010);
        let mut cursor = HuaniaCursor::from_initial_snapshot(&[baseline])?;
        let before = cursor.clone();
        let mut invalid = report(10, 2, 1_005, 1_020);
        invalid.latitude = 91.0;

        anyhow::ensure!(
            cursor
                .apply_reports(invalid.clone(), vec![invalid])
                .is_err()
        );
        anyhow::ensure!(cursor == before);
        Ok(())
    }

    #[test]
    fn summary_only_change_updates_cursor_without_replaying_report() -> Result<()> {
        let baseline = report(10, 2, 1_000, 1_010);
        let mut cursor = HuaniaCursor::from_initial_snapshot(&[baseline])?;
        let summary = report(10, 2, 1_005, 1_020);
        let detail = report(10, 2, 1_005, 1_010);

        anyhow::ensure!(cursor.needs_update(&summary)?);
        let events = cursor.apply_reports(summary, vec![detail])?;

        anyhow::ensure!(events.is_empty());
        let updated = cursor.event(10).context("missing cursor")?;
        anyhow::ensure!(updated.start_at == 1_005);
        anyhow::ensure!(updated.summary_update_at == 1_020);
        anyhow::ensure!(updated.detail_update_at == 1_010);
        Ok(())
    }

    #[test]
    fn baseline_detail_sync_does_not_replay_then_tracks_corrections() -> Result<()> {
        let baseline = report(10, 2, 1_000, 1_010);
        let mut cursor = HuaniaCursor::from_initial_snapshot(std::slice::from_ref(&baseline))?;
        let detail = report(10, 2, 1_000, 1_020);

        anyhow::ensure!(cursor.needs_update(&baseline)?);
        let initial_events = cursor.apply_reports(baseline, vec![detail])?;
        anyhow::ensure!(initial_events.is_empty());
        anyhow::ensure!(cursor.event(10).is_some_and(|entry| entry.detail_known));

        let summary = report(10, 2, 1_005, 1_025);
        let correction = report(10, 2, 1_005, 1_030);
        anyhow::ensure!(cursor.needs_update(&summary)?);
        let corrected_events = cursor.apply_reports(summary, vec![correction])?;
        anyhow::ensure!(corrected_events.len() == 1);
        anyhow::ensure!(corrected_events[0].revision == "1030");
        Ok(())
    }

    #[test]
    fn cursor_round_trips_each_event_watermark() -> Result<()> {
        let cursor = HuaniaCursor::from_initial_snapshot(&[
            report(10, 1, 1_000, 1_010),
            report(20, 2, 2_000, 2_020),
        ])?;
        anyhow::ensure!(HuaniaCursor::decode(&cursor.encode()?)? == cursor);
        Ok(())
    }

    #[test]
    fn maximum_snapshot_cursor_fits_provider_limit() -> Result<()> {
        let snapshot = (1..=MAX_TRACKED_EVENTS)
            .map(|index| {
                let index = i64::try_from(index).unwrap_or(i64::MAX);
                report(index, 1, 1_000 + index, 2_000 + index)
            })
            .collect::<Vec<_>>();
        let cursor = HuaniaCursor::from_initial_snapshot(&snapshot)?;

        let encoded = cursor.encode()?;
        let _cursor = ProviderCursor::new(HUANIA_CURSOR_STREAM, encoded)?;
        Ok(())
    }

    #[test]
    fn v1_cursor_is_accepted_for_upgrade() -> Result<()> {
        let report = report(10, 2, 1_000, 1_020);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&report.event_id.to_be_bytes());
        bytes.extend_from_slice(&report.start_at.to_be_bytes());
        bytes.extend_from_slice(&report.updates.to_be_bytes());
        let cursor = HuaniaCursor::decode(&format!("v1:{}", URL_SAFE_NO_PAD.encode(bytes)))?;
        anyhow::ensure!(cursor.event(10).map(|entry| entry.updates) == Some(2));
        anyhow::ensure!(cursor.encode()?.starts_with("v2:"));
        Ok(())
    }

    #[test]
    fn decodes_huania_api_url() -> Result<()> {
        let _url = huania_api_url()?;
        Ok(())
    }

    #[test]
    fn converts_epoch_millis_to_utc() -> Result<()> {
        anyhow::ensure!(epoch_millis_to_rfc3339(0)? == "1970-01-01T00:00:00.000Z");
        anyhow::ensure!(epoch_millis_to_rfc3339(1_735_689_600_123)? == "2025-01-01T00:00:00.123Z");
        Ok(())
    }

    #[test]
    fn rejects_invalid_huania_values() {
        let mut event = sample(1);
        event.latitude = 91.0;
        assert!(normalize(event).is_err());
    }
}
