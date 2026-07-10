mod config;
mod db;
mod models;
mod routes;
mod services;
mod utils;

use anyhow::{Context, Result};
use axum::{
    Router,
    http::{HeaderValue, Method},
    routing::{delete, get, post},
};
use config::Config;
use db::Database;
use routes::{
    AppState, bark_urls_handler, health_handler, index_handler, stats_handler, subscribe_handler,
    unsubscribe_handler,
};
use services::{BarkNotifier, BarkPushConfig, EarthquakeMonitor};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "earthquake_alert_backend=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env().context("failed to load configuration")?;
    tracing::info!(
        event = "config.loaded",
        server_host = %config.server_host,
        server_port = config.server_port,
        db_path = %config.db_path,
        websocket_url = %config.eew_websocket_url,
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
        db.subscriptions(),
        push_config,
    )?;

    let state = AppState {
        db: db.clone(),
        bark_notifier: bark_notifier.clone(),
        bark_urls: config.bark_url_allowlist.clone(),
    };

    let cors = build_cors_layer(&config)?;

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/index.html", get(index_handler))
        .route("/health", get(health_handler))
        .route("/api/subscribe", post(subscribe_handler))
        .route("/api/bark-urls", get(bark_urls_handler))
        .route("/api/unsubscribe", delete(unsubscribe_handler))
        .route("/api/stats", get(stats_handler))
        .layer(cors)
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.server_host, config.server_port)
        .parse()
        .context("failed to parse listen address")?;

    tracing::info!(event = "server.starting", listen_addr = %addr, "server.starting");

    let monitor = EarthquakeMonitor::new(db, config.clone(), bark_notifier)?;
    let monitor_handle = tokio::spawn(async move { monitor.start().await });

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .context("failed to bind HTTP listener")?;
    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            result.context("HTTP server failed")?;
        }
        result = monitor_handle => {
            match result {
                Ok(Ok(())) => tracing::warn!(event = "monitor.task_finished", "monitor.task_finished"),
                Ok(Err(error)) => return Err(error).context("monitor task failed"),
                Err(error) => return Err(error).context("monitor task panicked"),
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
