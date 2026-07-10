use axum::{response::Html, response::IntoResponse};

const INDEX_HTML: &str = include_str!("../../web/index.html");

pub async fn index_handler() -> impl IntoResponse {
    Html(INDEX_HTML)
}
