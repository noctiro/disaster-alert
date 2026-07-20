use crate::config::{Config, load_dotenv};
use crate::delivery::{BarkNotifier, BarkPushConfig, NotificationLinkService};
use crate::lifecycle;
use crate::providers::{FanStudioSource, HuaniaSource, WolfxSource};
use crate::routes::{
    AppState, ReverseGeocoder, bark_urls_handler, health_handler, incident_detail_handler,
    index_handler, reverse_geocode_handler, status_handler, subscribe_handler,
    subscription_options_handler, unsubscribe_handler,
};
use crate::runtime::{EventRuntime, RuntimeStatus};
use crate::storage::{RetentionPolicy, Storage};
use crate::subscriptions::SubscriptionConfirmationService;
use anyhow::{Context, Result};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderValue, Method},
    routing::{delete, get, post},
};
use std::net::SocketAddr;
use std::time::Duration;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const SUBSCRIPTION_BODY_LIMIT_BYTES: usize = 32 * 1024;

pub fn run_from_env() -> Result<()> {
    let dotenv_path = load_dotenv().context("failed to load .env configuration")?;

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "disaster_alert=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    if let Some(path) = dotenv_path {
        tracing::info!(event = "config.dotenv_loaded", path = %path.display(), "config.dotenv_loaded");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to create Tokio runtime")?;
    let result = runtime.block_on(run());
    runtime.shutdown_timeout(lifecycle::FORCED_SHUTDOWN_TIMEOUT);
    result
}

async fn run() -> Result<()> {
    let config = Config::from_env().context("failed to load configuration")?;
    tracing::info!(
        event = "config.loaded",
        instance_terms_accepted = config.instance_terms_accepted,
        server_host = %config.server_host,
        server_port = config.server_port,
        db_path = %config.db_path,
        max_concurrent_notifications = config.max_concurrent_notifications,
        http_pool_size = config.http_pool_size,
        "config.loaded"
    );
    if !config.instance_terms_accepted {
        tracing::warn!(
            event = "config.instance_terms_not_accepted",
            variable = "INSTANCE_TERMS_ACCEPTED",
            "config.instance_terms_not_accepted"
        );
    }

    let db_path = config.db_path.clone();
    let storage = tokio::task::spawn_blocking(move || Storage::open(db_path))
        .await
        .context("database open task failed")??;
    tracing::info!(event = "database.opened", db_path = %config.db_path, "database.opened");
    let prune_storage = storage.clone();
    let retention_policy = RetentionPolicy {
        incident_days: config.incident_retention_days,
        delivery_ledger_days: config.delivery_ledger_retention_days,
        operation_days: config.operation_retention_days,
    };
    let prune_stats =
        tokio::task::spawn_blocking(move || prune_storage.prune_retained_data(retention_policy))
            .await
            .context("database pruning task failed")?
            .context("failed to prune retained database records")?;
    if prune_stats.total() > 0 {
        tracing::info!(
            event = "database.records_pruned",
            incidents = prune_stats.incidents,
            delivery_records = prune_stats.delivery_records,
            events = prune_stats.events,
            "database.records_pruned"
        );
    }

    let push_config = BarkPushConfig::new(
        config.bark_sound.clone(),
        config.bark_volume,
        config.bark_group.clone(),
        config.bark_call,
    );
    let bark_notifier = BarkNotifier::new(
        config.bark_url_allowlist.clone(),
        config.http_pool_size,
        config.max_concurrent_notifications,
        push_config,
    )?;

    let runtime_status = RuntimeStatus::default();
    let reverse_geocoder = ReverseGeocoder::new(&config)?;
    let notification_links = NotificationLinkService::new(&config, &storage)?;
    let prune_links = notification_links.clone();
    let context_retention_days = config.notification_context_retention_days;
    let pruned_contexts =
        tokio::task::spawn_blocking(move || prune_links.prune_retained(context_retention_days))
            .await
            .context("notification context pruning task failed")??;
    let subscriptions = storage.subscription_manager();
    let subscription_confirmations =
        SubscriptionConfirmationService::new(subscriptions.clone(), bark_notifier.clone(), 16);
    let state = AppState::new(
        storage.clone(),
        bark_notifier.clone(),
        runtime_status.clone(),
        reverse_geocoder,
        notification_links.clone(),
        subscription_confirmations.clone(),
        config.max_concurrent_notifications,
    )
    .with_instance_terms_accepted(config.instance_terms_accepted);
    if pruned_contexts > 0 {
        tracing::info!(
            event = "database.notification_contexts_pruned",
            notification_contexts = pruned_contexts,
            "database.notification_contexts_pruned"
        );
    }

    let cors = build_cors_layer(&config)?;

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/index.html", get(index_handler))
        .route(
            "/incidents/{incident_id}/notifications/{token}",
            get(incident_detail_handler),
        )
        .route("/health", get(health_handler))
        .route(
            "/api/subscribe",
            post(subscribe_handler).layer(DefaultBodyLimit::max(SUBSCRIPTION_BODY_LIMIT_BYTES)),
        )
        .route("/api/bark-urls", get(bark_urls_handler))
        .route("/api/reverse-geocode", get(reverse_geocode_handler))
        .route(
            "/api/subscription-options",
            get(subscription_options_handler),
        )
        .route(
            "/api/unsubscribe",
            delete(unsubscribe_handler).layer(DefaultBodyLimit::max(SUBSCRIPTION_BODY_LIMIT_BYTES)),
        )
        .route("/api/status", get(status_handler))
        .layer(cors)
        .layer(CompressionLayer::new())
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.server_host, config.server_port)
        .parse()
        .context("failed to parse listen address")?;

    let event_runtime = EventRuntime::new(
        storage.clone(),
        &config,
        bark_notifier.clone(),
        notification_links,
        runtime_status.clone(),
    )?;
    event_runtime
        .recover()
        .await
        .context("failed to recover durable delivery, matching, and event work")?;

    tracing::info!(event = "server.starting", listen_addr = %addr, "server.starting");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("failed to bind HTTP listener")?;
    let wolfx = WolfxSource::new(&config, event_runtime.clone(), runtime_status.clone());
    let fanstudio = FanStudioSource::new(&config, event_runtime.clone(), runtime_status.clone());
    let huania = HuaniaSource::new(&config, event_runtime.clone(), runtime_status.clone())?;
    lifecycle::run_until_shutdown(
        listener,
        app,
        lifecycle::RuntimeServices::new(
            storage,
            event_runtime,
            subscription_confirmations,
            wolfx,
            fanstudio,
            huania,
        ),
        Duration::from_secs(config.shutdown_timeout_seconds),
    )
    .await
}

fn build_cors_layer(config: &Config) -> Result<CorsLayer> {
    let mut origins = Vec::new();
    for origin in &config.allowed_origins {
        origins.push(
            origin
                .parse::<HeaderValue>()
                .with_context(|| format!("invalid ALLOWED_ORIGINS entry {origin:?}"))?,
        );
    }

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ]);

    if origins.is_empty() {
        Ok(cors)
    } else {
        Ok(cors.allow_origin(origins))
    }
}
