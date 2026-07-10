mod config;
mod db;
mod models;
mod providers;
mod routes;
mod services;
mod source_registry;
mod utils;

use anyhow::{Context, Result};
use axum::{
    Router,
    http::{HeaderValue, Method},
    routing::{delete, get, post},
};
use config::Config;
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
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "disaster_alert_backend=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

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
        .route("/api/subscribe", post(subscribe_handler))
        .route("/api/bark-urls", get(bark_urls_handler))
        .route("/api/reverse-geocode", get(reverse_geocode_handler))
        .route(
            "/api/subscription-options",
            get(subscription_options_handler),
        )
        .route("/api/unsubscribe", delete(unsubscribe_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/status", get(status_handler))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.server_host, config.server_port)
        .parse()
        .context("failed to parse listen address")?;

    tracing::info!(event = "server.starting", listen_addr = %addr, "server.starting");

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
    let fanstudio = FanStudioSource::new(&config, dispatcher.clone(), runtime_status);
    let dispatcher_handle = tokio::spawn(async move { dispatcher.run().await });
    let wolfx_handle = tokio::spawn(async move { wolfx.run().await });
    let fanstudio_handle = tokio::spawn(async move { fanstudio.run().await });

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("failed to bind HTTP listener")?;
    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            result.context("HTTP server failed")?;
        }
        result = dispatcher_handle => {
            match result {
                Ok(Ok(())) => tracing::warn!(event = "dispatcher.task_finished", "dispatcher.task_finished"),
                Ok(Err(error)) => return Err(error).context("dispatcher task failed"),
                Err(error) => return Err(error).context("dispatcher task panicked"),
            }
        }
        result = wolfx_handle => {
            match result {
                Ok(Ok(())) => anyhow::bail!("Wolfx provider terminated unexpectedly"),
                Ok(Err(error)) => return Err(error).context("Wolfx provider failed"),
                Err(error) => return Err(error).context("Wolfx provider panicked"),
            }
        }
        result = fanstudio_handle => {
            match result {
                Ok(Ok(())) => anyhow::bail!("Fan Studio provider terminated unexpectedly"),
                Ok(Err(error)) => return Err(error).context("Fan Studio provider failed"),
                Err(error) => return Err(error).context("Fan Studio provider panicked"),
            }
        }
    }

    Ok(())
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
