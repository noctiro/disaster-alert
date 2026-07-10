use crate::config::normalize_bark_url;
use crate::db::{Database, StoreErrorKind, SubscriptionStore};
use crate::models::{
    ApiResponse, NotificationBand, SubscribeRequest, Subscription, SubscriptionLocation,
    UnsubscribeRequest, mask_bark_id, validate_bark_level,
};
use crate::services::{BarkNotifier, ReverseGeocodeResult, ReverseGeocoder, RuntimeStatus};
use crate::source_registry::{SourceGroup, groups};
use crate::utils::distance;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

const MAX_LOCATIONS: usize = 3;
const MAX_LOCATION_NAME_CHARS: usize = 80;
const MAX_NOTIFY_BANDS: usize = 3;
const MAX_BAND_LABEL_CHARS: usize = 32;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub bark_notifier: BarkNotifier,
    pub bark_urls: Vec<String>,
    pub runtime_status: RuntimeStatus,
    pub reverse_geocoder: ReverseGeocoder,
}

#[derive(Deserialize)]
pub struct ReverseGeocodeQuery {
    latitude: f64,
    longitude: f64,
}

pub async fn reverse_geocode_handler(
    State(state): State<AppState>,
    Query(query): Query<ReverseGeocodeQuery>,
) -> impl IntoResponse {
    if !distance::validate_coordinates(query.latitude, query.longitude) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<ReverseGeocodeResult>::error("坐标无效")),
        );
    }
    match state
        .reverse_geocoder
        .resolve(query.latitude, query.longitude)
        .await
    {
        Ok(location) => (
            StatusCode::OK,
            Json(ApiResponse::success("区域信息解析成功", Some(location))),
        ),
        Err(error) => {
            tracing::warn!(
                event = "reverse_geocode.failed",
                latitude = query.latitude,
                longitude = query.longitude,
                error = ?error,
                "reverse_geocode.failed"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::<ReverseGeocodeResult>::error(
                    "区域信息暂时无法自动解析，请手动填写",
                )),
            )
        }
    }
}

pub async fn subscribe_handler(
    State(state): State<AppState>,
    Json(payload): Json<SubscribeRequest>,
) -> impl IntoResponse {
    let bark_id = match validate_bark_id(&payload.bark_id) {
        Ok(value) => value,
        Err((status, message)) => {
            return (
                status,
                Json(ApiResponse::<SubscribeResponse>::error(message)),
            );
        }
    };

    let bark_url = match normalize_bark_url(&payload.bark_url) {
        Ok(value) => value,
        Err(_error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<SubscribeResponse>::error("Bark URL 无效")),
            );
        }
    };
    if !state.bark_notifier.allows_bark_url(&bark_url) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<SubscribeResponse>::error(
                "Bark URL 不在允许列表中",
            )),
        );
    }

    let locations = match normalize_locations(&payload) {
        Ok(locations) => locations,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<SubscribeResponse>::error(message)),
            );
        }
    };
    if locations.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<SubscribeResponse>::error(
                "请至少添加一个有效监测地点",
            )),
        );
    }
    let notify_bands = match resolve_notify_bands(&payload) {
        Ok(bands) => bands,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<SubscribeResponse>::error(message)),
            );
        }
    };
    if let Err(message) = payload.disaster_rules.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<SubscribeResponse>::error(message)),
        );
    }
    if payload.source_overrides.len() > 64
        || payload
            .source_overrides
            .keys()
            .any(|source| !known_source(source))
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<SubscribeResponse>::error("灾害来源配置无效")),
        );
    }
    let mut subscription = Subscription::new(bark_id, locations);
    subscription.bark_url = bark_url;
    subscription.notify_bands = notify_bands;
    subscription.disaster_rules = payload.disaster_rules;
    subscription.source_overrides = payload.source_overrides;

    tracing::info!(
        event = "subscription.requested",
        bark_id = %mask_bark_id(&subscription.bark_id),
        location_count = subscription.locations.len(),
        band_count = subscription.notify_bands.len(),
        "subscription.requested"
    );

    if let Err(error) = state
        .bark_notifier
        .send_subscription_confirm(&subscription)
        .await
    {
        tracing::error!(
            event = "subscription.confirm_failed",
            bark_id = %mask_bark_id(&subscription.bark_id),
            error = ?error,
            "subscription.confirm_failed"
        );
        return (
            StatusCode::BAD_GATEWAY,
            Json(ApiResponse::<SubscribeResponse>::error(format!(
                "订阅确认提醒发送失败，订阅未保存: {}",
                error
            ))),
        );
    }

    let store = state.db.subscriptions();
    let subscription_to_store = subscription.clone();
    match run_store(move || store.upsert_subscription(subscription_to_store)).await {
        Ok(_) => {
            tracing::info!(
                event = "subscription.request_completed",
                bark_id = %mask_bark_id(&subscription.bark_id),
                "subscription.request_completed"
            );
            (
                StatusCode::OK,
                Json(ApiResponse::success(
                    "订阅成功",
                    Some(SubscribeResponse::from(subscription)),
                )),
            )
        }
        Err(e) => {
            tracing::error!(
                event = "subscription.request_failed",
                bark_id = %mask_bark_id(&subscription.bark_id),
                error = ?e,
                "subscription.request_failed"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<SubscribeResponse>::error(format!(
                    "订阅失败: {}",
                    e
                ))),
            )
        }
    }
}

fn known_source(source: &str) -> bool {
    crate::source_registry::find(source).is_some()
}

#[derive(Serialize)]
pub struct SubscriptionOptionsResponse {
    pub groups: Vec<SourceGroup>,
    pub defaults: crate::models::DisasterRules,
}

pub async fn subscription_options_handler() -> impl IntoResponse {
    Json(ApiResponse::success(
        "订阅选项获取成功",
        Some(SubscriptionOptionsResponse {
            groups: groups(),
            defaults: crate::models::DisasterRules::default(),
        }),
    ))
}

pub async fn unsubscribe_handler(
    State(state): State<AppState>,
    Json(payload): Json<UnsubscribeRequest>,
) -> impl IntoResponse {
    let bark_id = match validate_bark_id(&payload.bark_id) {
        Ok(value) => value,
        Err((status, message)) => {
            return (status, Json(ApiResponse::<()>::error(message)));
        }
    };

    tracing::info!(
        event = "subscription.delete_requested",
        bark_id = %mask_bark_id(&bark_id),
        "subscription.delete_requested"
    );

    let store = state.db.subscriptions();
    let delete_bark_id = bark_id.clone();
    match run_store(move || store.delete_subscription(&delete_bark_id)).await {
        Ok(_) => {
            tracing::info!(
                event = "subscription.delete_completed",
                bark_id = %mask_bark_id(&bark_id),
                "subscription.delete_completed"
            );
            (
                StatusCode::OK,
                Json(ApiResponse::<()>::success("已取消订阅", None)),
            )
        }
        Err(e) => {
            tracing::error!(
                event = "subscription.delete_failed",
                bark_id = %mask_bark_id(&bark_id),
                error = ?e,
                "subscription.delete_failed"
            );
            let status = match SubscriptionStore::classify_error(&e) {
                StoreErrorKind::NotFound => StatusCode::NOT_FOUND,
                StoreErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                status,
                Json(ApiResponse::<()>::error(format!("取消订阅失败: {}", e))),
            )
        }
    }
}

#[derive(Serialize)]
pub struct SubscribeResponse {
    pub saved: bool,
}

impl From<Subscription> for SubscribeResponse {
    fn from(_sub: Subscription) -> Self {
        Self { saved: true }
    }
}

fn normalize_locations(payload: &SubscribeRequest) -> Result<Vec<SubscriptionLocation>, String> {
    let mut locations = payload.locations.clone();
    if locations.is_empty() {
        return Err("请至少添加一个有效监测地点".to_string());
    }
    if locations.len() > MAX_LOCATIONS {
        return Err(format!("监测地点最多 {MAX_LOCATIONS} 个"));
    }
    if locations
        .iter()
        .any(|item| !distance::validate_coordinates(item.latitude, item.longitude))
    {
        return Err("监测地点坐标无效".to_string());
    }
    for location in &mut locations {
        for (label, value) in [
            ("名称", &mut location.name),
            ("省级行政区", &mut location.province),
            ("城市", &mut location.city),
            ("区县", &mut location.district),
        ] {
            let trimmed = value.trim();
            if trimmed.chars().count() > MAX_LOCATION_NAME_CHARS {
                return Err(format!(
                    "监测地点{label}最多 {MAX_LOCATION_NAME_CHARS} 个字符"
                ));
            }
            *value = trimmed.to_string();
        }
    }
    Ok(locations)
}

fn normalize_notify_bands(payload: &SubscribeRequest) -> Result<Vec<NotificationBand>, String> {
    if payload.notify_bands.is_empty() {
        return Err("请至少添加一条通知级别规则".to_string());
    }
    if payload.notify_bands.len() > MAX_NOTIFY_BANDS {
        return Err(format!("通知级别规则最多 {MAX_NOTIFY_BANDS} 条"));
    }
    let mut bands = payload.notify_bands.clone();
    bands.sort_by_key(|band| band.min);
    let mut levels = std::collections::HashSet::new();
    let mut used = std::collections::HashSet::new();
    for band in &mut bands {
        band.level = band.level.trim().to_ascii_lowercase();
        if !validate_bark_level(&band.level) {
            return Err("通知级别必须是 passive、active 或 critical".to_string());
        }
        if !levels.insert(band.level.clone()) {
            return Err("每个通知级别只能添加一条规则".to_string());
        }
        if band.min > band.max || band.min > 99 || band.max > 99 {
            return Err("通知级别烈度范围无效".to_string());
        }
        if band.level == "critical" && band.max < 7 {
            band.max = 99;
        }
        let trimmed_label = band.label.trim();
        if trimmed_label.chars().count() > MAX_BAND_LABEL_CHARS {
            return Err(format!("通知级别标签最多 {MAX_BAND_LABEL_CHARS} 个字符"));
        }
        band.label = trimmed_label.to_string();
        for value in band.min..=band.max {
            if !used.insert(value) {
                return Err("通知级别烈度范围不能重叠".to_string());
            }
        }
    }
    Ok(bands)
}

fn resolve_notify_bands(payload: &SubscribeRequest) -> Result<Vec<NotificationBand>, String> {
    if !payload.disaster_rules.earthquake_warning && payload.notify_bands.is_empty() {
        Ok(Vec::new())
    } else {
        normalize_notify_bands(payload)
    }
}

fn validate_bark_id(raw: &str) -> std::result::Result<String, (StatusCode, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Bark ID 不能为空".to_string()));
    }
    if trimmed.len() > 64 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Bark ID 过长（最大64字符）".to_string(),
        ));
    }
    if !trimmed.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Bark ID 只能包含字母、数字".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

async fn run_store<F>(operation: F) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(anyhow::Error::from)?
}

#[derive(Serialize)]
pub struct StatsResponse {
    pub total_subscriptions: usize,
}

#[derive(Serialize)]
pub struct BarkUrlsResponse {
    pub bark_urls: Vec<String>,
}

pub async fn bark_urls_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(ApiResponse::success(
        "Bark URL 列表获取成功",
        Some(BarkUrlsResponse {
            bark_urls: state.bark_urls,
        }),
    ))
}

pub async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let store = state.db.subscriptions();
    match tokio::task::spawn_blocking(move || store.get_total_count()).await {
        Ok(Ok(count)) => (
            StatusCode::OK,
            Json(ApiResponse::success(
                "统计成功",
                Some(StatsResponse {
                    total_subscriptions: count,
                }),
            )),
        ),
        Ok(Err(e)) => {
            tracing::error!(event = "stats.load_failed", error = ?e, "stats.load_failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<StatsResponse>::error(format!(
                    "获取统计失败: {}",
                    e
                ))),
            )
        }
        Err(e) => {
            tracing::error!(event = "stats.task_failed", error = ?e, "stats.task_failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<StatsResponse>::error("获取统计失败")),
            )
        }
    }
}

pub async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(ApiResponse::<()>::success("OK", None)))
}

pub async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(ApiResponse::success(
        "运行状态获取成功",
        Some(state.runtime_status.snapshot()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> SubscribeRequest {
        SubscribeRequest {
            bark_id: "abc123".to_string(),
            bark_url: "https://api.day.app".to_string(),
            locations: vec![SubscriptionLocation {
                name: "home".to_string(),
                latitude: 35.0,
                longitude: 105.0,
                province: String::new(),
                city: String::new(),
                district: String::new(),
            }],
            notify_bands: Vec::new(),
            disaster_rules: crate::models::DisasterRules::default(),
            source_overrides: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn weather_only_subscription_accepts_empty_intensity_bands() {
        let mut payload = request();
        payload.disaster_rules.earthquake_warning = false;
        assert!(matches!(resolve_notify_bands(&payload), Ok(bands) if bands.is_empty()));
    }

    #[test]
    fn earthquake_warning_subscription_requires_intensity_bands() {
        assert!(resolve_notify_bands(&request()).is_err());
    }

    #[test]
    fn administrative_fields_obey_location_length_limit() {
        let mut payload = request();
        payload.locations[0].province = "省".repeat(MAX_LOCATION_NAME_CHARS + 1);
        assert!(normalize_locations(&payload).is_err());
    }
}
