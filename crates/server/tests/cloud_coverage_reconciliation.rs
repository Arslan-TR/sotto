use std::str::FromStr;

use sotto_server::cloud_coverage::ConfirmedPaidInterval;
use sotto_server::cloud_coverage_reconciliation::{
    begin_collection, finish_collection, register_source, CollectionStatus, ReconciliationError,
    RegistrationOutcome, SourceBinding, SourceObservation,
};
use sotto_server::cloud_coverage_store::{load, StoreError, UnavailableReason};
use sotto_server::db;
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use uuid::Uuid;

struct Fixture {
    pool: PgPool,
    beneficiary_id: String,
}

impl Fixture {
    async fn create() -> Option<Self> {
        if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
            return None;
        }
        let database_url = std::env::var("DATABASE_URL").ok()?;
        let options = PgConnectOptions::from_str(&database_url).expect("parse DATABASE_URL");
        assert!(
            matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
            "refusing reconciliation tests against non-local host: {}",
            options.get_host()
        );
        let pool = db::connect(&database_url).await.expect("connect");
        db::migrate(&pool).await.expect("migrate");
        let beneficiary_id = format!("coverage-reconciliation-test-{}", Uuid::new_v4());
        sqlx::query(
            "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'reconciliation-test', $2)",
        )
        .bind(&beneficiary_id)
        .bind(&beneficiary_id)
        .execute(&pool)
        .await
        .expect("insert reconciliation test user");
        Some(Self {
            pool,
            beneficiary_id,
        })
    }
}

async fn cleanup(fixture: &Fixture) {
    sqlx::query("DELETE FROM cloud_coverage_heads WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage head");
    sqlx::query("DELETE FROM cloud_coverage_revision_facts WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage facts");
    sqlx::query("DELETE FROM cloud_coverage_revisions WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage revisions");
    sqlx::query("DELETE FROM cloud_coverage_collection_attempts WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete collection attempts");
    sqlx::query("DELETE FROM cloud_coverage_sources WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage sources");
    sqlx::query("DELETE FROM cloud_coverage_coordinators WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage coordinator");
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete reconciliation test user");
}

fn binding(fixture: &Fixture, source_id: &str, external: &str) -> SourceBinding {
    SourceBinding {
        beneficiary_id: fixture.beneficiary_id.clone(),
        source_id: source_id.into(),
        provider_namespace: "stripe:test".into(),
        external_allocation_reference: external.into(),
        ownership_evidence_reference: format!("evidence:{external}"),
    }
}

#[tokio::test]
async fn first_registration_is_unavailable_and_exact_replay_is_idempotent() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source-1", "allocation-1");
    let mut tx = fixture.pool.begin().await.expect("begin registration");
    let first = register_source(&mut tx, "registration-1", &source)
        .await
        .expect("register source");
    tx.commit().await.expect("commit registration");
    assert_eq!(first.source_set_generation, 1);
    assert_eq!(first.projection_revision, Some(1));
    assert_eq!(first.outcome, RegistrationOutcome::Applied);
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::NeedsReconciliation
        ))
    ));

    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin registration replay");
    let replay = register_source(&mut tx, "registration-1", &source)
        .await
        .expect("replay source registration");
    tx.commit().await.expect("commit registration replay");
    assert_eq!(replay.outcome, RegistrationOutcome::AlreadyApplied);
    assert_eq!(replay.source_set_generation, 1);
    assert_eq!(replay.projection_revision, Some(1));

    cleanup(&fixture).await;
}

#[tokio::test]
async fn adding_a_source_invalidates_the_previous_completeness_claim() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let first_source = binding(&fixture, "source-1", "allocation-1");
    let second_source = binding(&fixture, "source-2", "allocation-2");
    for (operation, source) in [
        ("registration-1", first_source),
        ("registration-2", second_source),
    ] {
        let mut tx = fixture
            .pool
            .begin()
            .await
            .expect("begin source registration");
        let receipt = register_source(&mut tx, operation, &source)
            .await
            .expect("register source");
        tx.commit().await.expect("commit source registration");
        assert_eq!(receipt.outcome, RegistrationOutcome::Applied);
    }
    let generation: i64 = sqlx::query_scalar(
        "SELECT source_set_generation FROM cloud_coverage_coordinators WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read source generation");
    assert_eq!(generation, 2);
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::NeedsReconciliation
        ))
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn registration_rollback_leaves_no_source_or_projection() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source-1", "allocation-1");
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin rolled-back registration");
    register_source(&mut tx, "registration-1", &source)
        .await
        .expect("register source before rollback");
    tx.rollback().await.expect("rollback registration");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionMissing)
    ));
    let source_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_coverage_sources WHERE beneficiary_id = $1")
            .bind(&fixture.beneficiary_id)
            .fetch_one(&fixture.pool)
            .await
            .expect("count rolled-back sources");
    assert_eq!(source_count, 0);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn registration_rejects_reusing_operation_for_changed_binding() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source-1", "allocation-1");
    let changed = binding(&fixture, "source-2", "allocation-2");
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin source registration");
    register_source(&mut tx, "registration-1", &source)
        .await
        .expect("register source");
    tx.commit().await.expect("commit source registration");

    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin conflicting registration");
    let result = register_source(&mut tx, "registration-1", &changed).await;
    tx.rollback()
        .await
        .expect("rollback conflicting registration");
    assert!(matches!(
        result,
        Err(ReconciliationError::RegistrationConflict)
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn complete_collection_replaces_unavailable_projection_and_replays() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source-1", "allocation-1");
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin source registration");
    register_source(&mut tx, "registration-1", &source)
        .await
        .expect("register source");
    tx.commit().await.expect("commit source registration");

    let ticket = {
        let mut tx = fixture.pool.begin().await.expect("begin collection");
        let ticket = begin_collection(&mut tx, &fixture.beneficiary_id, "collection-1")
            .await
            .expect("begin collection");
        tx.commit().await.expect("commit collection begin");
        ticket
    };
    assert_eq!(ticket.status, CollectionStatus::Pending);
    let observations = vec![SourceObservation::Complete {
        source_id: source.source_id.clone(),
        evidence_reference: "source-evidence-1".into(),
        paid_intervals: vec![ConfirmedPaidInterval {
            coverage_id: "coverage-1".into(),
            source_id: source.source_id.clone(),
            starts_at: 0,
            paid_until: 30 * 24 * 60 * 60,
            failed_renewal_id: None,
        }],
    }];
    let receipt = {
        let mut tx = fixture.pool.begin().await.expect("begin collection finish");
        let receipt = finish_collection(&mut tx, &ticket, "collection-evidence-1", &observations)
            .await
            .expect("finish collection");
        tx.commit().await.expect("commit collection finish");
        receipt
    };
    assert_eq!(receipt.revision, 2);
    let loaded = load(&fixture.pool, &fixture.beneficiary_id)
        .await
        .expect("load completed collection");
    assert_eq!(loaded.revision, 2);
    assert_eq!(loaded.coverage.paid_intervals.len(), 1);

    let replay = {
        let mut tx = fixture.pool.begin().await.expect("begin collection replay");
        let replay = finish_collection(&mut tx, &ticket, "collection-evidence-1", &observations)
            .await
            .expect("replay completed collection");
        tx.commit().await.expect("commit collection replay");
        replay
    };
    assert_eq!(
        replay.outcome,
        sotto_server::cloud_coverage_store::PublicationOutcome::AlreadyApplied
    );
    assert_eq!(replay.revision, receipt.revision);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn a_new_collection_supersedes_an_older_pending_attempt() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source-1", "allocation-1");
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin source registration");
    register_source(&mut tx, "registration-1", &source)
        .await
        .expect("register source");
    tx.commit().await.expect("commit source registration");

    let first = {
        let mut tx = fixture.pool.begin().await.expect("begin first collection");
        let ticket = begin_collection(&mut tx, &fixture.beneficiary_id, "collection-1")
            .await
            .expect("begin first collection");
        tx.commit().await.expect("commit first collection");
        ticket
    };
    let second = {
        let mut tx = fixture.pool.begin().await.expect("begin second collection");
        let ticket = begin_collection(&mut tx, &fixture.beneficiary_id, "collection-2")
            .await
            .expect("begin second collection");
        tx.commit().await.expect("commit second collection");
        ticket
    };
    assert_eq!(second.collection_epoch, first.collection_epoch + 1);
    assert_eq!(first.status, CollectionStatus::Pending);
    let observation = SourceObservation::Unavailable {
        source_id: source.source_id,
        evidence_reference: "source-evidence".into(),
        reason: UnavailableReason::NeedsReconciliation,
    };
    let mut tx = fixture.pool.begin().await.expect("begin stale collection");
    let result = finish_collection(&mut tx, &first, "collection-evidence", &[observation]).await;
    tx.rollback().await.expect("rollback stale collection");
    assert!(matches!(
        result,
        Err(ReconciliationError::AttemptSuperseded)
    ));
    cleanup(&fixture).await;
}
