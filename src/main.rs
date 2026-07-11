mod config;
mod db;
mod lifecycle;
mod models;
mod providers;
mod routes;
mod services;
mod source_registry;
mod utils;

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderValue, Method},
    routing::{delete, get, post},
};
use config::{Config, load_dotenv};
use db::Database;
use providers::{FanStudioSource, WolfxSource};
use routes::{
    AppState, bark_urls_handler, health_handler, index_handler, reverse_geocode_handler,
    stats_handler, status_handler, subscribe_handler, subscription_options_handler,
    unsubscribe_handler,
};
use services::{
    BarkNotifier, BarkPushConfig, DisasterDispatcher, EventAggregator, ReverseGeocoder,
    RuntimeStatus,
};
use std::net::SocketAddr;
use std::time::Duration;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const SUBSCRIPTION_BODY_LIMIT_BYTES: usize = 32 * 1024;

fn main() -> Result<()> {
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
        server_host = %config.server_host,
        server_port = config.server_port,
        db_path = %config.db_path,
        wolfx_websocket_url = %config.wolfx_websocket_url,
        fanstudio_websocket_url = %config.fanstudio_websocket_url,
        max_concurrent_notifications = config.max_concurrent_notifications,
        http_pool_size = config.http_pool_size,
        "config.loaded"
    );

    let db = Database::open(&config.db_path)?;
    tracing::info!(event = "database.opened", db_path = %config.db_path, "database.opened");

    let push_config = BarkPushConfig {
        sound: config.bark_sound.clone(),
        volume: config.bark_volume,
        group: config.bark_group.clone(),
        call: config.bark_call,
    };
    let bark_notifier = BarkNotifier::new(
        config.bark_url_allowlist.clone(),
        config.http_pool_size,
        config.max_concurrent_notifications,
        db.subscriptions(),
        push_config,
    )?;

    let runtime_status = RuntimeStatus::default();
    let reverse_geocoder = ReverseGeocoder::new(&config)?;
    let state = AppState {
        db: db.clone(),
        bark_notifier: bark_notifier.clone(),
        bark_urls: config.bark_url_allowlist.clone(),
        runtime_status: runtime_status.clone(),
        reverse_geocoder,
    };

    let cors = build_cors_layer(&config)?;

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/index.html", get(index_handler))
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
        .route("/api/stats", get(stats_handler))
        .route("/api/status", get(status_handler))
        .layer(cors)
        .layer(CompressionLayer::new())
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.server_host, config.server_port)
        .parse()
        .context("failed to parse listen address")?;

    tracing::info!(event = "server.starting", listen_addr = %addr, "server.starting");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("failed to bind HTTP listener")?;

    let dedup_keep_seconds = config
        .dedup_keep_minutes
        .checked_mul(60)
        .context("DEDUP_KEEP_MINUTES is too large")?;
    let aggregator = EventAggregator::new(Duration::from_secs(dedup_keep_seconds));
    let dispatcher = DisasterDispatcher::new(
        db.clone(),
        &config,
        bark_notifier.clone(),
        aggregator.clone(),
        runtime_status.clone(),
    );
    let wolfx = WolfxSource::new(&config, dispatcher.clone(), runtime_status.clone());
    let fanstudio = FanStudioSource::new(&config, dispatcher.clone(), runtime_status.clone());
    lifecycle::run_until_shutdown(
        listener,
        app,
        db,
        dispatcher,
        wolfx,
        fanstudio,
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
