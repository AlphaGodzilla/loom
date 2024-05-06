use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use axum::routing::{get, post};
use tokio::signal;

use crate::handler_match::{handler_match, TraderMarketWrap};

pub async fn start_http_server(market: TraderMarketWrap) {
    let app = router(Arc::clone(&market));
    serve(app, Arc::clone(&market)).await;
}

async fn handler_ping() -> &'static str {
    "pong"
}

fn router(market: TraderMarketWrap) -> Router {
    // 注册路由
    let ping_handler = Router::new()
        .route("/ping", get(handler_ping));

    let match_handler = Router::new()
        .route("/api/v1/match", post(handler_match))
        .with_state(Arc::clone(&market));

    Router::new()
        .merge(ping_handler)
        .merge(match_handler)
}

async fn serve(app: Router, market: TraderMarketWrap) {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:7001").await.unwrap();
    println!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(market))
        .await.unwrap();
}

async fn shutdown_signal(market: TraderMarketWrap) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    // 关闭引擎
    market.lock().await.shutdown().await;
}


#[derive(Debug)]
pub struct AppError(anyhow::Error);


// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{}", self.0),
        ).into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError where E: Into<anyhow::Error> {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}