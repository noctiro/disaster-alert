use crate::config::normalize_bark_url;
use crate::db::{Database, StoreErrorKind, SubscriptionStore};
use crate::models::{
    ApiResponse, MonitoringTarget, NotificationDestination, SubscribeRequest, Subscription,
    UnsubscribeRequest, mask_device_key,
};
use crate::services::{BarkNotifier, ReverseGeocodeResult, ReverseGeocoder, RuntimeStatus};
use crate::source_registry::{CategoryOption, category_options};
use crate::utils::distance;
use axum::{
    Json,
    extract::{Query, State, rejection::JsonRejection},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

const MAX_LOCATIONS: usize = 3;
const MAX_LOCATION_NAME_CHARS: usize = 80;

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
    payload: Result<Json<SubscribeRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<SubscribeResponse>::error("订阅请求体无效")),
            );
        }
    };
    let device_key = match validate_device_key(payload.destination.bark_device_key()) {
        Ok(value) => value,
        Err((status, message)) => {
            return (
                status,
                Json(ApiResponse::<SubscribeResponse>::error(message)),
            );
        }
    };

    let bark_url = match normalize_bark_url(payload.destination.bark_base_url()) {
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

    let targets = match normalize_targets(payload.targets) {
        Ok(targets) => targets,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<SubscribeResponse>::error(message)),
            );
        }
    };
    let subscription = Subscription::new(
        NotificationDestination::Bark {
            base_url: bark_url,
            device_key,
        },
        targets,
        payload.alerts,
    );
    if let Err(message) = subscription.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<SubscribeResponse>::error(message)),
        );
    }

    tracing::info!(
        event = "subscription.requested",
        device_key = %mask_device_key(subscription.device_key()),
        target_count = subscription.targets.len(),
        alert_count = subscription.alerts.len(),
        "subscription.requested"
    );

    if let Err(error) = state
        .bark_notifier
        .send_subscription_confirm(&subscription)
        .await
    {
        tracing::error!(
            event = "subscription.confirm_failed",
            device_key = %mask_device_key(subscription.device_key()),
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
                device_key = %mask_device_key(subscription.device_key()),
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
                device_key = %mask_device_key(subscription.device_key()),
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

#[derive(Serialize)]
pub struct SubscriptionOptionsResponse {
    pub categories: Vec<CategoryOption>,
}

pub async fn subscription_options_handler() -> impl IntoResponse {
    Json(ApiResponse::success(
        "订阅选项获取成功",
        Some(SubscriptionOptionsResponse {
            categories: category_options(),
        }),
    ))
}

pub async fn unsubscribe_handler(
    State(state): State<AppState>,
    payload: Result<Json<UnsubscribeRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error("取消订阅请求体无效")),
            );
        }
    };
    let device_key = match validate_device_key(payload.destination.bark_device_key()) {
        Ok(value) => value,
        Err((status, message)) => {
            return (status, Json(ApiResponse::<()>::error(message)));
        }
    };
    let base_url = match normalize_bark_url(payload.destination.bark_base_url()) {
        Ok(value) if state.bark_notifier.allows_bark_url(&value) => value,
        Ok(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error("Bark URL 不在允许列表中")),
            );
        }
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<()>::error("Bark URL 无效")),
            );
        }
    };
    let destination_id = crate::models::DestinationId {
        base_url,
        device_key,
    };

    tracing::info!(
        event = "subscription.delete_requested",
        device_key = %mask_device_key(&destination_id.device_key),
        "subscription.delete_requested"
    );

    let store = state.db.subscriptions();
    let destination_to_delete = destination_id.clone();
    match run_store(move || store.delete_subscription(&destination_to_delete)).await {
        Ok(_) => {
            tracing::info!(
                event = "subscription.delete_completed",
                device_key = %mask_device_key(&destination_id.device_key),
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
                device_key = %mask_device_key(&destination_id.device_key),
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

fn normalize_targets(mut targets: Vec<MonitoringTarget>) -> Result<Vec<MonitoringTarget>, String> {
    if targets.is_empty() {
        return Err("请至少添加一个有效监测地点".to_string());
    }
    if targets.len() > MAX_LOCATIONS {
        return Err(format!("监测地点最多 {MAX_LOCATIONS} 个"));
    }
    if targets.iter().any(|target| {
        !distance::validate_coordinates(target.point.latitude, target.point.longitude)
    }) {
        return Err("监测地点坐标无效".to_string());
    }
    for target in &mut targets {
        for (label, value) in [
            ("名称", &mut target.label),
            ("省级行政区", &mut target.region.province),
            ("城市", &mut target.region.city),
            ("区县", &mut target.region.district),
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
    Ok(targets)
}

fn validate_device_key(raw: &str) -> std::result::Result<String, (StatusCode, String)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Bark Key 不能为空".to_string()));
    }
    if trimmed.len() > 64 {
        return Err((
            StatusCode::BAD_REQUEST,
            "Bark Key 过长（最大64字符）".to_string(),
        ));
    }
    if !trimmed.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Bark Key 只能包含字母、数字".to_string(),
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
            destination: NotificationDestination::Bark {
                base_url: "https://api.day.app".to_string(),
                device_key: "abc123".to_string(),
            },
            targets: vec![MonitoringTarget {
                label: "home".to_string(),
                point: crate::models::GeoPoint {
                    latitude: 35.0,
                    longitude: 105.0,
                },
                region: crate::models::AdministrativeRegion::default(),
            }],
            alerts: vec![crate::models::AlertRule::default_for(
                crate::models::DisasterCategory::WeatherWarning,
            )],
        }
    }

    #[test]
    fn weather_only_subscription_does_not_require_intensity_bands() {
        let payload = request();
        let subscription = Subscription::new(payload.destination, payload.targets, payload.alerts);
        assert!(subscription.validate().is_ok());
    }

    #[test]
    fn administrative_fields_obey_location_length_limit() {
        let mut payload = request();
        payload.targets[0].region.province = "省".repeat(MAX_LOCATION_NAME_CHARS + 1);
        assert!(normalize_targets(payload.targets).is_err());
    }
}
