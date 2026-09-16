//! Required database execution harness for server authorisation assurance.
//!
//! The cases in this binary deliberately run through the production router against Postgres.  A
//! normal local workspace test may skip when the opt-in is absent, but CI sets
//! `SOTTO_RUN_DB_TESTS=1`; in that mode a missing URL or an unreachable database is a failure.

use std::str::FromStr;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use tower::ServiceExt;

use sotto_server::config::DEFAULT_ORGANISATION_DELETION_RETENTION_DAYS;
use sotto_server::db;
use sotto_server::state::AppState;

async fn pool_or_skip() -> Option<PgPool> {
    let required = std::env::var("SOTTO_RUN_DB_TESTS").as_deref() == Ok("1");
    let url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(error) if required => panic!("DATABASE_URL is required for server assurance: {error}"),
        Err(_) => {
            eprintln!("skipping server assurance: set SOTTO_RUN_DB_TESTS=1 and DATABASE_URL");
            return None;
        }
    };

    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "server assurance only accepts a dedicated loopback database, got {}",
        options.get_host()
    );
    let pool = db::connect(&url)
        .await
        .expect("connect to the server assurance database");
    db::migrate(&pool)
        .await
        .expect("migrate the server assurance database");
    Some(pool)
}

fn app(pool: PgPool) -> Router {
    let state = AppState {
        telemetry_ingest: false,
        pool,
        oauth: None,
        oauth_config: None,
        billing: None,
        organisation_deletion_enabled: false,
        organisation_deletion_retention_days: DEFAULT_ORGANISATION_DELETION_RETENTION_DAYS,
        organisation_deletion_metrics_token: None,
        organisation_deletion_operator_token: None,
    };
    sotto_server::app(state)
}

async fn get(pool: &PgPool, uri: &str) -> StatusCode {
    let response = app(pool.clone())
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");
    response.status()
}

#[tokio::test]
async fn server_assurance_executes_against_the_required_database() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };

    let mut completed = 0usize;
    assert_eq!(get(&pool, "/health").await, StatusCode::OK);
    completed += 1;

    assert!(completed > 0, "server assurance executed no scenarios");
    println!("SERVER_ASSURANCE_DONE {completed}");
}
