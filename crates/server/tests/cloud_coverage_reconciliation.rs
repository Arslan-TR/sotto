use std::{str::FromStr, sync::Arc};

use sotto_server::cloud_coverage::ConfirmedPaidInterval;
use sotto_server::cloud_coverage_reconciliation::{
    begin_collection, finish_collection, register_source, CollectionStatus, CorruptAttemptReason,
    ReconciliationError, RegistrationOutcome, SourceBinding, SourceObservation,
};
use sotto_server::cloud_coverage_store::{
    load, publish, CoverageProjection, PublicationOutcome, StoreError, UnavailableReason,
};
use sotto_server::db;
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::{oneshot, Barrier, Notify};
use tokio::time::{sleep, Duration, Instant};
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
        let database_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL is required when SOTTO_RUN_DB_TESTS=1");
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

    async fn add_beneficiary(&self) -> Self {
        let beneficiary_id = format!("coverage-reconciliation-test-{}", Uuid::new_v4());
        sqlx::query(
            "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'reconciliation-test', $2)",
        )
        .bind(&beneficiary_id)
        .bind(&beneficiary_id)
        .execute(&self.pool)
        .await
        .expect("insert second reconciliation test user");
        Self {
            pool: self.pool.clone(),
            beneficiary_id,
        }
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
    sqlx::query(
        "UPDATE cloud_coverage_coordinators SET current_attempt_id = NULL WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .execute(&fixture.pool)
    .await
    .expect("clear current collection attempt");
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
    sqlx::query("DELETE FROM cloud_coverage_revisions WHERE beneficiary_id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage revisions");
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
        source_id: format!("{}:{source_id}", fixture.beneficiary_id),
        provider_namespace: format!("stripe:test:{}", fixture.beneficiary_id),
        external_allocation_reference: external.into(),
        ownership_evidence_reference: format!("evidence:{external}"),
    }
}

fn attempt_id(fixture: &Fixture, suffix: &str) -> String {
    format!("{}:{suffix}", fixture.beneficiary_id)
}

async fn register(fixture: &Fixture, source: &SourceBinding, operation_id: &str) {
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin source registration");
    register_source(&mut tx, operation_id, source)
        .await
        .expect("register source");
    tx.commit().await.expect("commit source registration");
}

async fn begin(
    fixture: &Fixture,
    attempt_id: &str,
) -> sotto_server::cloud_coverage_reconciliation::CollectionTicket {
    let mut tx = fixture.pool.begin().await.expect("begin collection");
    let ticket = begin_collection(&mut tx, &fixture.beneficiary_id, attempt_id)
        .await
        .expect("begin collection");
    tx.commit().await.expect("commit collection");
    ticket
}

async fn corrupt_bindings(
    fixture: &Fixture,
    ticket: &sotto_server::cloud_coverage_reconciliation::CollectionTicket,
    bindings: serde_json::Value,
) {
    sqlx::query(
        "UPDATE cloud_coverage_collection_attempts SET source_bindings = $3::jsonb \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&ticket.attempt_id)
    .bind(bindings.to_string())
    .execute(&fixture.pool)
    .await
    .expect("corrupt stored source bindings");
}

async fn corrupt_result(
    fixture: &Fixture,
    ticket: &sotto_server::cloud_coverage_reconciliation::CollectionTicket,
    result: serde_json::Value,
) {
    sqlx::query(
        "UPDATE cloud_coverage_collection_attempts SET canonical_result = $3::jsonb \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&ticket.attempt_id)
    .bind(result.to_string())
    .execute(&fixture.pool)
    .await
    .expect("corrupt stored collection result");
}

async fn corrupt_generation(
    fixture: &Fixture,
    ticket: &sotto_server::cloud_coverage_reconciliation::CollectionTicket,
    generation: i64,
) {
    sqlx::query(
        "UPDATE cloud_coverage_collection_attempts SET source_set_generation = $3 \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&ticket.attempt_id)
    .bind(generation)
    .execute(&fixture.pool)
    .await
    .expect("corrupt stored source generation");
}

async fn head_revision(fixture: &Fixture) -> i64 {
    sqlx::query_scalar(
        "SELECT current_revision FROM cloud_coverage_heads WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read coverage head revision")
}

async fn transaction_pid(tx: &mut Transaction<'_, Postgres>) -> i32 {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut **tx)
        .await
        .expect("read transaction backend pid")
}

async fn wait_for_specific_block(pool: &PgPool, waiter_pid: i32, holder_pid: i32) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_stat_activity
                 WHERE pid = $1 AND $2 = ANY(pg_blocking_pids(pid))
             )",
        )
        .bind(waiter_pid)
        .bind(holder_pid)
        .fetch_one(pool)
        .await
        .expect("inspect coverage transaction blocking");
        if blocked {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for backend {waiter_pid} to block on {holder_pid}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn held_registration(
    pool: PgPool,
    operation_id: String,
    source: SourceBinding,
    ready: oneshot::Sender<i32>,
    release: Arc<Notify>,
) -> Result<sotto_server::cloud_coverage_reconciliation::RegistrationReceipt, ReconciliationError> {
    let mut tx = pool.begin().await.expect("begin held registration");
    let pid = transaction_pid(&mut tx).await;
    let result = register_source(&mut tx, &operation_id, &source).await;
    ready.send(pid).expect("signal held registration");
    release.notified().await;
    match result {
        Ok(receipt) => {
            tx.commit().await.expect("commit held registration");
            Ok(receipt)
        }
        Err(error) => {
            tx.rollback().await.expect("rollback held registration");
            Err(error)
        }
    }
}

#[tokio::test]
async fn malformed_stored_bindings_fail_closed_without_writes() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source", "allocation");
    register(&fixture, &source, "registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "corrupt-bindings")).await;
    let original_head = head_revision(&fixture).await;
    corrupt_bindings(&fixture, &ticket, serde_json::json!([])).await;

    let mut begin_tx = fixture.pool.begin().await.expect("begin corrupt replay");
    let replay = begin_collection(&mut begin_tx, &fixture.beneficiary_id, &ticket.attempt_id).await;
    begin_tx.commit().await.expect("commit corrupt replay");
    assert!(matches!(
        replay,
        Err(ReconciliationError::CorruptAttempt(
            CorruptAttemptReason::BindingShape
        ))
    ));

    let observation = SourceObservation::Complete {
        source_id: source.source_id,
        evidence_reference: "source-evidence".into(),
        paid_intervals: vec![],
    };
    let mut finish_tx = fixture.pool.begin().await.expect("begin corrupt finish");
    let finish = finish_collection(
        &mut finish_tx,
        &ticket,
        "aggregate-evidence",
        &[observation],
    )
    .await;
    finish_tx.commit().await.expect("commit corrupt finish");
    assert!(matches!(
        finish,
        Err(ReconciliationError::CorruptAttempt(
            CorruptAttemptReason::BindingShape
        ))
    ));
    assert_eq!(head_revision(&fixture).await, original_head);
    let status: String = sqlx::query_scalar(
        "SELECT status FROM cloud_coverage_collection_attempts \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&ticket.attempt_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read corrupt attempt status");
    assert_eq!(status, "pending");
    cleanup(&fixture).await;
}

#[tokio::test]
async fn changed_stored_binding_fails_without_disclosing_as_a_ticket_conflict() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source", "allocation");
    register(&fixture, &source, "registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "changed-binding")).await;
    let mut changed = source.clone();
    changed.ownership_evidence_reference = "different-evidence".into();
    corrupt_bindings(&fixture, &ticket, serde_json::json!([changed])).await;

    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin changed binding replay");
    let replay = begin_collection(&mut tx, &fixture.beneficiary_id, &ticket.attempt_id).await;
    tx.commit().await.expect("commit changed binding replay");
    assert!(matches!(
        replay,
        Err(ReconciliationError::CorruptAttempt(
            CorruptAttemptReason::BindingSourceSet
        ))
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn stored_attempt_requires_an_exact_source_generation_snapshot() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source", "allocation");
    register(&fixture, &source, "registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "missing-generation")).await;
    corrupt_generation(&fixture, &ticket, ticket.source_set_generation + 1).await;

    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin corrupt generation replay");
    let replay = begin_collection(&mut tx, &fixture.beneficiary_id, &ticket.attempt_id).await;
    tx.commit().await.expect("commit corrupt generation replay");
    assert!(matches!(
        replay,
        Err(ReconciliationError::CorruptAttempt(
            CorruptAttemptReason::BindingSourceSet
        ))
    ));
    let coordinator_generation: i64 = sqlx::query_scalar(
        "SELECT source_set_generation FROM cloud_coverage_coordinators WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read source generation after corrupt replay");
    assert_eq!(coordinator_generation, ticket.source_set_generation);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn corrupt_completed_result_fails_before_classifying_a_changed_replay() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source", "allocation");
    register(&fixture, &source, "registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "corrupt-result")).await;
    let observation = SourceObservation::Complete {
        source_id: source.source_id.clone(),
        evidence_reference: "source-evidence".into(),
        paid_intervals: vec![],
    };
    let mut complete_tx = fixture.pool.begin().await.expect("begin completion");
    finish_collection(
        &mut complete_tx,
        &ticket,
        "aggregate-evidence",
        std::slice::from_ref(&observation),
    )
    .await
    .expect("complete collection");
    complete_tx.commit().await.expect("commit completion");
    let original_head = head_revision(&fixture).await;
    corrupt_result(
        &fixture,
        &ticket,
        serde_json::json!({
            "aggregate_evidence_reference": "aggregate-evidence",
            "sources": []
        }),
    )
    .await;

    let mut finish_tx = fixture.pool.begin().await.expect("begin corrupt replay");
    let changed = finish_collection(
        &mut finish_tx,
        &ticket,
        "different-evidence",
        &[SourceObservation::Unavailable {
            source_id: source.source_id,
            evidence_reference: "different-source-evidence".into(),
            reason: UnavailableReason::ConflictingEvidence,
        }],
    )
    .await;
    finish_tx.commit().await.expect("commit corrupt replay");
    assert!(matches!(
        changed,
        Err(ReconciliationError::CorruptAttempt(
            CorruptAttemptReason::ResultCanonical
        ))
    ));

    let mut begin_tx = fixture
        .pool
        .begin()
        .await
        .expect("begin corrupt ticket replay");
    let replay = begin_collection(&mut begin_tx, &fixture.beneficiary_id, &ticket.attempt_id).await;
    begin_tx
        .commit()
        .await
        .expect("commit corrupt ticket replay");
    assert!(matches!(
        replay,
        Err(ReconciliationError::CorruptAttempt(
            CorruptAttemptReason::ResultCanonical
        ))
    ));
    assert_eq!(head_revision(&fixture).await, original_head);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn beneficiaries_can_independently_use_the_same_attempt_id() {
    let Some(first) = Fixture::create().await else {
        return;
    };
    let second = first.add_beneficiary().await;
    let first_source = binding(&first, "source", "allocation");
    let second_source = binding(&second, "source", "allocation");
    register(&first, &first_source, "registration").await;
    register(&second, &second_source, "registration").await;

    let first_ticket = begin(&first, "attempt").await;
    let second_ticket = begin(&second, "attempt").await;
    assert_eq!(first_ticket.attempt_id, second_ticket.attempt_id);
    assert_ne!(first_ticket.beneficiary_id, second_ticket.beneficiary_id);

    for (fixture, ticket, source) in [
        (&first, first_ticket, first_source),
        (&second, second_ticket, second_source),
    ] {
        let observation = SourceObservation::Complete {
            source_id: source.source_id,
            evidence_reference: "source-evidence".into(),
            paid_intervals: vec![],
        };
        let mut tx = fixture.pool.begin().await.expect("begin collection finish");
        finish_collection(&mut tx, &ticket, "collection-evidence", &[observation])
            .await
            .expect("finish collection");
        tx.commit().await.expect("commit collection finish");
        assert!(load(&fixture.pool, &fixture.beneficiary_id).await.is_ok());
    }

    cleanup(&second).await;
    cleanup(&first).await;
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
        ("registration-1", first_source.clone()),
        ("registration-2", second_source.clone()),
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
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin registration replay");
    let replay = register_source(&mut tx, "registration-1", &first_source)
        .await
        .expect("replay first source registration");
    tx.commit().await.expect("commit registration replay");
    assert_eq!(replay.outcome, RegistrationOutcome::AlreadyApplied);
    assert_eq!(replay.source_set_generation, 1);
    assert_eq!(replay.projection_revision, Some(1));
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
async fn a_source_cannot_bind_to_two_beneficiaries() {
    let Some(first) = Fixture::create().await else {
        return;
    };
    let second = first.add_beneficiary().await;
    let source = binding(&first, "source", "allocation");
    register(&first, &source, "registration").await;

    let rebound = SourceBinding {
        beneficiary_id: second.beneficiary_id.clone(),
        source_id: source.source_id.clone(),
        provider_namespace: source.provider_namespace.clone(),
        external_allocation_reference: source.external_allocation_reference.clone(),
        ownership_evidence_reference: source.ownership_evidence_reference.clone(),
    };
    let mut tx = second
        .pool
        .begin()
        .await
        .expect("begin conflicting registration");
    let result = register_source(&mut tx, "registration", &rebound).await;
    tx.rollback()
        .await
        .expect("rollback conflicting registration");
    assert!(matches!(
        result,
        Err(ReconciliationError::SourceBindingConflict)
    ));
    assert!(matches!(
        load(&second.pool, &second.beneficiary_id).await,
        Err(StoreError::ProjectionMissing)
    ));

    cleanup(&second).await;
    cleanup(&first).await;
}

#[tokio::test]
async fn registration_rejects_adopting_an_unmanaged_projection() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin unmanaged publication");
    publish(
        &mut tx,
        &fixture.beneficiary_id,
        None,
        "unmanaged-publication",
        "unmanaged-evidence",
        &CoverageProjection::Complete {
            paid_intervals: vec![],
        },
    )
    .await
    .expect("publish unmanaged projection");
    tx.commit().await.expect("commit unmanaged projection");

    let source = binding(&fixture, "source-1", "allocation-1");
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin source registration");
    let result = register_source(&mut tx, "registration-1", &source).await;
    tx.rollback()
        .await
        .expect("rollback unmanaged registration");
    assert!(matches!(
        result,
        Err(ReconciliationError::BootstrapConflict)
    ));
    assert!(load(&fixture.pool, &fixture.beneficiary_id).await.is_ok());
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
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-1"),
        )
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

    let mut tx = fixture.pool.begin().await.expect("begin later publication");
    publish(
        &mut tx,
        &fixture.beneficiary_id,
        Some(receipt.revision),
        "later-publication",
        "later-evidence",
        &CoverageProjection::Complete {
            paid_intervals: vec![],
        },
    )
    .await
    .expect("publish later projection");
    tx.commit().await.expect("commit later publication");

    let mut tx = fixture.pool.begin().await.expect("begin stale replay");
    let stale_replay = finish_collection(&mut tx, &ticket, "collection-evidence-1", &observations)
        .await
        .expect("replay original collection after a later revision");
    tx.commit().await.expect("commit stale replay");
    assert_eq!(stale_replay.revision, receipt.revision);
    assert_eq!(
        load(&fixture.pool, &fixture.beneficiary_id)
            .await
            .expect("load later projection")
            .revision,
        receipt.revision + 1
    );

    let later_source = binding(&fixture, "source-2", "allocation-2");
    register(&fixture, &later_source, "registration-2").await;
    let source_generation: i64 = sqlx::query_scalar(
        "SELECT source_set_generation FROM cloud_coverage_coordinators WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read later source generation");
    assert_eq!(source_generation, 2);
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin historical source replay");
    let historical_replay =
        finish_collection(&mut tx, &ticket, "collection-evidence-1", &observations)
            .await
            .expect("replay collection after source registration");
    tx.commit().await.expect("commit historical source replay");
    assert_eq!(historical_replay.revision, receipt.revision);
    assert_eq!(head_revision(&fixture).await, receipt.revision + 2);

    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin malformed collection replay");
    let changed = finish_collection(&mut tx, &ticket, "collection-evidence-1", &[]).await;
    tx.rollback()
        .await
        .expect("rollback malformed collection replay");
    assert!(matches!(
        changed,
        Err(ReconciliationError::OperationConflict)
    ));
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
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-1"),
        )
        .await
        .expect("begin first collection");
        tx.commit().await.expect("commit first collection");
        ticket
    };
    let second = {
        let mut tx = fixture.pool.begin().await.expect("begin second collection");
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-2"),
        )
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
    let status: String = sqlx::query_scalar(
        "SELECT status FROM cloud_coverage_collection_attempts \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&first.attempt_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read superseded attempt status");
    assert_eq!(status, "superseded");
    cleanup(&fixture).await;
}

#[tokio::test]
async fn a_new_source_supersedes_a_pending_collection() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let first_source = binding(&fixture, "first", "allocation-first");
    let second_source = binding(&fixture, "second", "allocation-second");
    register(&fixture, &first_source, "registration-first").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "pending")).await;

    register(&fixture, &second_source, "registration-second").await;
    let status: String = sqlx::query_scalar(
        "SELECT status FROM cloud_coverage_collection_attempts \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&ticket.attempt_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read source invalidated attempt");
    assert_eq!(status, "superseded");

    let observation = SourceObservation::Unavailable {
        source_id: first_source.source_id,
        evidence_reference: "source-evidence".into(),
        reason: UnavailableReason::NeedsReconciliation,
    };
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin stale source finish");
    let result = finish_collection(&mut tx, &ticket, "aggregate-evidence", &[observation]).await;
    tx.rollback().await.expect("rollback stale source finish");
    assert!(matches!(
        result,
        Err(ReconciliationError::AttemptSuperseded)
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn competing_source_claims_preserve_provider_allocation_ownership() {
    let Some(first) = Fixture::create().await else {
        return;
    };
    let second = first.add_beneficiary().await;
    let first_source = binding(&first, "owner-a", "shared-allocation");
    let mut second_source = binding(&second, "owner-b", "shared-allocation");
    second_source.provider_namespace = first_source.provider_namespace.clone();

    let release = Arc::new(Notify::new());
    let (holder_ready, holder_ready_rx) = oneshot::channel();
    let holder = tokio::spawn(held_registration(
        first.pool.clone(),
        "owner-a-registration".into(),
        first_source.clone(),
        holder_ready,
        release.clone(),
    ));
    let holder_pid = holder_ready_rx
        .await
        .expect("receive allocation holder pid");

    let (waiter_ready, waiter_ready_rx) = oneshot::channel();
    let waiter_pool = second.pool.clone();
    let waiter_source = second_source.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool.begin().await.expect("begin allocation waiter");
        let pid = transaction_pid(&mut tx).await;
        waiter_ready.send(pid).expect("signal allocation waiter");
        let result = register_source(&mut tx, "owner-b-registration", &waiter_source).await;
        tx.rollback().await.expect("rollback allocation waiter");
        result
    });
    let waiter_pid = waiter_ready_rx
        .await
        .expect("receive allocation waiter pid");
    wait_for_specific_block(&first.pool, waiter_pid, holder_pid).await;
    release.notify_one();

    let first_receipt = tokio::time::timeout(Duration::from_secs(10), holder)
        .await
        .expect("allocation holder finished")
        .expect("join allocation holder")
        .expect("first allocation claim applied");
    let second_result = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("allocation waiter finished")
        .expect("join allocation waiter");
    assert_eq!(first_receipt.outcome, RegistrationOutcome::Applied);
    assert!(matches!(
        second_result,
        Err(ReconciliationError::SourceBindingConflict)
    ));
    let second_source_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_coverage_sources WHERE beneficiary_id = $1")
            .bind(&second.beneficiary_id)
            .fetch_one(&second.pool)
            .await
            .expect("count losing allocation sources");
    assert_eq!(second_source_count, 0);

    let mut replay_tx = first.pool.begin().await.expect("begin allocation replay");
    let replay = register_source(&mut replay_tx, "owner-a-registration", &first_source)
        .await
        .expect("replay winning allocation claim");
    replay_tx.commit().await.expect("commit allocation replay");
    assert_eq!(replay.outcome, RegistrationOutcome::AlreadyApplied);
    assert_eq!(
        replay.projection_revision,
        first_receipt.projection_revision
    );
    cleanup(&second).await;
    cleanup(&first).await;
}

#[tokio::test]
async fn competing_source_claims_preserve_global_source_identity() {
    let Some(first) = Fixture::create().await else {
        return;
    };
    let second = first.add_beneficiary().await;
    let first_source = binding(&first, "shared-source", "allocation-a");
    let mut second_source = binding(&second, "different-source", "allocation-b");
    second_source.source_id = first_source.source_id.clone();

    let release = Arc::new(Notify::new());
    let (holder_ready, holder_ready_rx) = oneshot::channel();
    let holder = tokio::spawn(held_registration(
        first.pool.clone(),
        "identity-a-registration".into(),
        first_source.clone(),
        holder_ready,
        release.clone(),
    ));
    let holder_pid = holder_ready_rx.await.expect("receive identity holder pid");

    let (waiter_ready, waiter_ready_rx) = oneshot::channel();
    let waiter_pool = second.pool.clone();
    let waiter_source = second_source.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool.begin().await.expect("begin identity waiter");
        let pid = transaction_pid(&mut tx).await;
        waiter_ready.send(pid).expect("signal identity waiter");
        let result = register_source(&mut tx, "identity-b-registration", &waiter_source).await;
        tx.rollback().await.expect("rollback identity waiter");
        result
    });
    let waiter_pid = waiter_ready_rx.await.expect("receive identity waiter pid");
    wait_for_specific_block(&first.pool, waiter_pid, holder_pid).await;
    release.notify_one();

    let first_receipt = tokio::time::timeout(Duration::from_secs(10), holder)
        .await
        .expect("identity holder finished")
        .expect("join identity holder")
        .expect("first identity claim applied");
    let second_result = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("identity waiter finished")
        .expect("join identity waiter");
    assert_eq!(first_receipt.outcome, RegistrationOutcome::Applied);
    assert!(matches!(
        second_result,
        Err(ReconciliationError::SourceBindingConflict)
    ));
    let second_source_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cloud_coverage_sources WHERE beneficiary_id = $1")
            .bind(&second.beneficiary_id)
            .fetch_one(&second.pool)
            .await
            .expect("count losing identity sources");
    assert_eq!(second_source_count, 0);
    cleanup(&second).await;
    cleanup(&first).await;
}

#[tokio::test]
async fn registration_first_supersedes_a_completion_waiting_on_the_coordinator() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let first_source = binding(
        &fixture,
        "registration-race-first",
        "registration-race-allocation-first",
    );
    let second_source = binding(
        &fixture,
        "registration-race-second",
        "registration-race-allocation-second",
    );
    register(&fixture, &first_source, "registration-race-first-op").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "registration-race-pending")).await;
    let observation = SourceObservation::Complete {
        source_id: first_source.source_id.clone(),
        evidence_reference: "registration-race-evidence".into(),
        paid_intervals: vec![],
    };

    let release = Arc::new(Notify::new());
    let (holder_ready, holder_ready_rx) = oneshot::channel();
    let holder = tokio::spawn(held_registration(
        fixture.pool.clone(),
        "registration-race-second-op".into(),
        second_source,
        holder_ready,
        release.clone(),
    ));
    let holder_pid = holder_ready_rx
        .await
        .expect("receive registration holder pid");

    let (waiter_ready, waiter_ready_rx) = oneshot::channel();
    let waiter_pool = fixture.pool.clone();
    let waiter_ticket = ticket.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool.begin().await.expect("begin superseded finish");
        let pid = transaction_pid(&mut tx).await;
        waiter_ready.send(pid).expect("signal superseded finish");
        let result = finish_collection(
            &mut tx,
            &waiter_ticket,
            "registration-race-aggregate",
            &[observation],
        )
        .await;
        tx.rollback().await.expect("rollback superseded finish");
        result
    });
    let waiter_pid = waiter_ready_rx
        .await
        .expect("receive superseded finish pid");
    wait_for_specific_block(&fixture.pool, waiter_pid, holder_pid).await;
    release.notify_one();

    let registration = tokio::time::timeout(Duration::from_secs(10), holder)
        .await
        .expect("registration holder finished")
        .expect("join registration holder")
        .expect("second registration applied");
    let finish = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("superseded finish finished")
        .expect("join superseded finish");
    assert_eq!(registration.source_set_generation, 2);
    assert!(matches!(
        finish,
        Err(ReconciliationError::AttemptSuperseded)
    ));
    let status: String = sqlx::query_scalar(
        "SELECT status FROM cloud_coverage_collection_attempts \
         WHERE beneficiary_id = $1 AND attempt_id = $2",
    )
    .bind(&fixture.beneficiary_id)
    .bind(&ticket.attempt_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read superseded registration race status");
    assert_eq!(status, "superseded");
    assert_eq!(head_revision(&fixture).await, 2);
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::NeedsReconciliation
        ))
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn completion_first_allows_registration_and_preserves_historical_replay() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let first_source = binding(
        &fixture,
        "completion-race-first",
        "completion-race-allocation-first",
    );
    let second_source = binding(
        &fixture,
        "completion-race-second",
        "completion-race-allocation-second",
    );
    register(&fixture, &first_source, "completion-race-first-op").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "completion-race-pending")).await;
    let observations = vec![SourceObservation::Complete {
        source_id: first_source.source_id.clone(),
        evidence_reference: "completion-race-evidence".into(),
        paid_intervals: vec![ConfirmedPaidInterval {
            coverage_id: "completion-race-coverage".into(),
            source_id: first_source.source_id.clone(),
            starts_at: 0,
            paid_until: 100,
            failed_renewal_id: None,
        }],
    }];

    let release = Arc::new(Notify::new());
    let (holder_ready, holder_ready_rx) = oneshot::channel();
    let holder_pool = fixture.pool.clone();
    let holder_ticket = ticket.clone();
    let holder_observations = observations.clone();
    let holder_release = release.clone();
    let holder = tokio::spawn(async move {
        let mut tx = holder_pool
            .begin()
            .await
            .expect("begin held completion race");
        let pid = transaction_pid(&mut tx).await;
        let result = finish_collection(
            &mut tx,
            &holder_ticket,
            "completion-race-aggregate",
            &holder_observations,
        )
        .await;
        holder_ready.send(pid).expect("signal held completion race");
        holder_release.notified().await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit held completion race");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback held completion race");
                Err(error)
            }
        }
    });
    let holder_pid = holder_ready_rx
        .await
        .expect("receive completion holder pid");

    let (waiter_ready, waiter_ready_rx) = oneshot::channel();
    let waiter_pool = fixture.pool.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool
            .begin()
            .await
            .expect("begin blocked registration race");
        let pid = transaction_pid(&mut tx).await;
        waiter_ready
            .send(pid)
            .expect("signal blocked registration race");
        let result = register_source(&mut tx, "completion-race-second-op", &second_source).await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit blocked registration race");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback()
                    .await
                    .expect("rollback blocked registration race");
                Err(error)
            }
        }
    });
    let waiter_pid = waiter_ready_rx
        .await
        .expect("receive registration waiter pid");
    wait_for_specific_block(&fixture.pool, waiter_pid, holder_pid).await;
    release.notify_one();

    let completion = tokio::time::timeout(Duration::from_secs(10), holder)
        .await
        .expect("held completion race finished")
        .expect("join held completion race")
        .expect("completion race applied");
    let registration = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("registration race finished")
        .expect("join registration race")
        .expect("registration race applied");
    assert_eq!(completion.revision, 2);
    assert_eq!(completion.outcome, PublicationOutcome::Applied);
    assert_eq!(registration.source_set_generation, 2);
    assert_eq!(registration.projection_revision, Some(3));
    assert_eq!(head_revision(&fixture).await, 3);
    let mut replay_tx = fixture.pool.begin().await.expect("begin completed replay");
    let replay = finish_collection(
        &mut replay_tx,
        &ticket,
        "completion-race-aggregate",
        &observations,
    )
    .await
    .expect("replay completed historical collection");
    replay_tx.commit().await.expect("commit completed replay");
    assert_eq!(replay.revision, completion.revision);
    assert_eq!(replay.outcome, PublicationOutcome::AlreadyApplied);
    assert_eq!(head_revision(&fixture).await, 3);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn identical_concurrent_completions_apply_once_and_replay_once() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "source", "allocation");
    register(&fixture, &source, "registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "concurrent")).await;
    let observations = vec![SourceObservation::Complete {
        source_id: source.source_id,
        evidence_reference: "source-evidence".into(),
        paid_intervals: vec![],
    }];
    let barrier = std::sync::Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let pool = fixture.pool.clone();
        let ticket = ticket.clone();
        let observations = observations.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut tx = pool.begin().await.expect("begin concurrent finish");
            let result = finish_collection(&mut tx, &ticket, "aggregate-evidence", &observations)
                .await
                .expect("finish concurrent collection");
            tx.commit().await.expect("commit concurrent finish");
            result
        }));
    }
    let first = tasks.remove(0).await.expect("join first completion");
    let second = tasks.remove(0).await.expect("join second completion");
    assert_eq!(first.revision, second.revision);
    assert!(matches!(
        (first.outcome, second.outcome),
        (
            sotto_server::cloud_coverage_store::PublicationOutcome::Applied,
            sotto_server::cloud_coverage_store::PublicationOutcome::AlreadyApplied
        ) | (
            sotto_server::cloud_coverage_store::PublicationOutcome::AlreadyApplied,
            sotto_server::cloud_coverage_store::PublicationOutcome::Applied
        )
    ));
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_coverage_collection_attempts \
         WHERE beneficiary_id = $1 AND status = 'completed'",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("count completed attempts");
    assert_eq!(completed_count, 1);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn identical_completion_waits_for_the_winner_and_replays_exactly() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "serialised-source", "serialised-allocation");
    register(&fixture, &source, "serialised-registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "serialised-completion")).await;
    let observations = vec![SourceObservation::Complete {
        source_id: source.source_id.clone(),
        evidence_reference: "serialised-source-evidence".into(),
        paid_intervals: vec![ConfirmedPaidInterval {
            coverage_id: "serialised-coverage".into(),
            source_id: source.source_id.clone(),
            starts_at: 0,
            paid_until: 100,
            failed_renewal_id: None,
        }],
    }];

    let release = Arc::new(Notify::new());
    let (holder_ready, holder_ready_rx) = oneshot::channel();
    let holder_pool = fixture.pool.clone();
    let holder_ticket = ticket.clone();
    let holder_observations = observations.clone();
    let holder_release = release.clone();
    let holder = tokio::spawn(async move {
        let mut tx = holder_pool.begin().await.expect("begin held completion");
        let pid = transaction_pid(&mut tx).await;
        let result = finish_collection(
            &mut tx,
            &holder_ticket,
            "serialised-aggregate-evidence",
            &holder_observations,
        )
        .await;
        holder_ready.send(pid).expect("signal held completion");
        holder_release.notified().await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit held completion");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback held completion");
                Err(error)
            }
        }
    });
    let holder_pid = holder_ready_rx.await.expect("receive holder pid");

    let (waiter_ready, waiter_ready_rx) = oneshot::channel();
    let waiter_pool = fixture.pool.clone();
    let waiter_ticket = ticket.clone();
    let waiter_observations = observations.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool.begin().await.expect("begin waiting completion");
        let pid = transaction_pid(&mut tx).await;
        waiter_ready.send(pid).expect("signal waiting completion");
        let result = finish_collection(
            &mut tx,
            &waiter_ticket,
            "serialised-aggregate-evidence",
            &waiter_observations,
        )
        .await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit waiting completion");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback waiting completion");
                Err(error)
            }
        }
    });
    let waiter_pid = waiter_ready_rx.await.expect("receive waiter pid");
    wait_for_specific_block(&fixture.pool, waiter_pid, holder_pid).await;
    release.notify_one();

    let applied = tokio::time::timeout(Duration::from_secs(10), holder)
        .await
        .expect("held completion finished")
        .expect("join held completion")
        .expect("held completion applied");
    let replay = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("waiting completion finished")
        .expect("join waiting completion")
        .expect("waiting completion replayed");
    assert_eq!(applied.revision, replay.revision);
    assert_eq!(applied.outcome, PublicationOutcome::Applied);
    assert_eq!(replay.outcome, PublicationOutcome::AlreadyApplied);
    assert_eq!(head_revision(&fixture).await, applied.revision);
    let revision_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_coverage_revisions WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("count serialised revisions");
    assert_eq!(revision_count, 2);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn conflicting_completion_waits_then_rolls_back_without_a_loser_revision() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let source = binding(&fixture, "conflicting-source", "conflicting-allocation");
    register(&fixture, &source, "conflicting-registration").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "conflicting-completion")).await;
    let winning_observations = vec![SourceObservation::Complete {
        source_id: source.source_id.clone(),
        evidence_reference: "winning-source-evidence".into(),
        paid_intervals: vec![ConfirmedPaidInterval {
            coverage_id: "winning-coverage".into(),
            source_id: source.source_id.clone(),
            starts_at: 0,
            paid_until: 100,
            failed_renewal_id: None,
        }],
    }];
    let losing_observations = vec![SourceObservation::Complete {
        source_id: source.source_id.clone(),
        evidence_reference: "losing-source-evidence".into(),
        paid_intervals: vec![ConfirmedPaidInterval {
            coverage_id: "losing-coverage".into(),
            source_id: source.source_id.clone(),
            starts_at: 0,
            paid_until: 200,
            failed_renewal_id: None,
        }],
    }];

    let release = Arc::new(Notify::new());
    let (holder_ready, holder_ready_rx) = oneshot::channel();
    let holder_pool = fixture.pool.clone();
    let holder_ticket = ticket.clone();
    let holder_observations = winning_observations.clone();
    let holder_release = release.clone();
    let holder = tokio::spawn(async move {
        let mut tx = holder_pool
            .begin()
            .await
            .expect("begin held winning completion");
        let pid = transaction_pid(&mut tx).await;
        let result = finish_collection(
            &mut tx,
            &holder_ticket,
            "winning-aggregate-evidence",
            &holder_observations,
        )
        .await;
        holder_ready
            .send(pid)
            .expect("signal held winning completion");
        holder_release.notified().await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit held winning completion");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback()
                    .await
                    .expect("rollback held winning completion");
                Err(error)
            }
        }
    });
    let holder_pid = holder_ready_rx.await.expect("receive winning holder pid");

    let (waiter_ready, waiter_ready_rx) = oneshot::channel();
    let waiter_pool = fixture.pool.clone();
    let waiter_ticket = ticket.clone();
    let waiter = tokio::spawn(async move {
        let mut tx = waiter_pool.begin().await.expect("begin losing completion");
        let pid = transaction_pid(&mut tx).await;
        waiter_ready.send(pid).expect("signal losing completion");
        let result = finish_collection(
            &mut tx,
            &waiter_ticket,
            "losing-aggregate-evidence",
            &losing_observations,
        )
        .await;
        tx.rollback().await.expect("rollback losing completion");
        result
    });
    let waiter_pid = waiter_ready_rx.await.expect("receive losing waiter pid");
    wait_for_specific_block(&fixture.pool, waiter_pid, holder_pid).await;
    release.notify_one();

    let winner = tokio::time::timeout(Duration::from_secs(10), holder)
        .await
        .expect("winning completion finished")
        .expect("join winning completion")
        .expect("winning completion applied");
    let loser = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("losing completion finished")
        .expect("join losing completion");
    assert_eq!(winner.outcome, PublicationOutcome::Applied);
    assert!(matches!(loser, Err(ReconciliationError::OperationConflict)));
    assert_eq!(head_revision(&fixture).await, winner.revision);
    let loser_fact_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_coverage_revision_facts \
         WHERE beneficiary_id = $1 AND coverage_id = 'losing-coverage'",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("count losing facts");
    assert_eq!(loser_fact_count, 0);
    let revision_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cloud_coverage_revisions WHERE beneficiary_id = $1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("count conflicting revisions");
    assert_eq!(revision_count, 2);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn collection_combines_all_sources_in_canonical_order() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let first_source = binding(&fixture, "source-B", "allocation-B");
    let second_source = binding(&fixture, "source-a", "allocation-a");
    for (operation, source) in [
        ("registration-B", first_source.clone()),
        ("registration-a", second_source.clone()),
    ] {
        let mut tx = fixture
            .pool
            .begin()
            .await
            .expect("begin source registration");
        register_source(&mut tx, operation, &source)
            .await
            .expect("register source");
        tx.commit().await.expect("commit source registration");
    }
    let ticket = {
        let mut tx = fixture.pool.begin().await.expect("begin collection");
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-all"),
        )
        .await
        .expect("begin collection");
        tx.commit().await.expect("commit collection begin");
        ticket
    };
    let observations = vec![
        SourceObservation::Complete {
            source_id: second_source.source_id.clone(),
            evidence_reference: "evidence-a".into(),
            paid_intervals: vec![ConfirmedPaidInterval {
                coverage_id: "a".into(),
                source_id: second_source.source_id.clone(),
                starts_at: 30,
                paid_until: 60,
                failed_renewal_id: None,
            }],
        },
        SourceObservation::Complete {
            source_id: first_source.source_id.clone(),
            evidence_reference: "evidence-B".into(),
            paid_intervals: vec![ConfirmedPaidInterval {
                coverage_id: "B".into(),
                source_id: first_source.source_id.clone(),
                starts_at: 0,
                paid_until: 30,
                failed_renewal_id: None,
            }],
        },
    ];
    let wrong_provenance = vec![
        SourceObservation::Complete {
            source_id: second_source.source_id.clone(),
            evidence_reference: "evidence-a".into(),
            paid_intervals: vec![ConfirmedPaidInterval {
                coverage_id: "a".into(),
                source_id: "unregistered-source".into(),
                starts_at: 30,
                paid_until: 60,
                failed_renewal_id: None,
            }],
        },
        observations[1].clone(),
    ];
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin wrong provenance finish");
    let result = finish_collection(
        &mut tx,
        &ticket,
        "evidence-wrong-provenance",
        &wrong_provenance,
    )
    .await;
    tx.rollback()
        .await
        .expect("rollback wrong provenance finish");
    assert!(matches!(
        result,
        Err(ReconciliationError::SourceObservationConflict(_))
    ));
    let conflicting_renewal = vec![
        SourceObservation::Complete {
            source_id: second_source.source_id.clone(),
            evidence_reference: "evidence-a-renewal".into(),
            paid_intervals: vec![ConfirmedPaidInterval {
                coverage_id: "a-renewal".into(),
                source_id: second_source.source_id.clone(),
                starts_at: 30,
                paid_until: 60,
                failed_renewal_id: Some("renewal-shared".into()),
            }],
        },
        SourceObservation::Complete {
            source_id: first_source.source_id.clone(),
            evidence_reference: "evidence-B-renewal".into(),
            paid_intervals: vec![ConfirmedPaidInterval {
                coverage_id: "B-renewal".into(),
                source_id: first_source.source_id.clone(),
                starts_at: 0,
                paid_until: 30,
                failed_renewal_id: Some("renewal-shared".into()),
            }],
        },
    ];
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin conflicting renewal finish");
    let result = finish_collection(
        &mut tx,
        &ticket,
        "evidence-conflicting-renewal",
        &conflicting_renewal,
    )
    .await;
    tx.rollback()
        .await
        .expect("rollback conflicting renewal finish");
    assert!(matches!(
        result,
        Err(ReconciliationError::SourceObservationConflict(_))
    ));
    let mut tx = fixture.pool.begin().await.expect("begin collection finish");
    finish_collection(&mut tx, &ticket, "evidence-all", &observations)
        .await
        .expect("finish collection");
    tx.commit().await.expect("commit collection finish");
    let loaded = load(&fixture.pool, &fixture.beneficiary_id)
        .await
        .expect("load combined collection");
    assert_eq!(
        loaded
            .coverage
            .paid_intervals
            .iter()
            .map(|interval| interval.coverage_id.as_str())
            .collect::<Vec<_>>(),
        ["B", "a"]
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn direct_projection_change_rejects_a_stale_collection() {
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
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-stale"),
        )
        .await
        .expect("begin collection");
        tx.commit().await.expect("commit collection begin");
        ticket
    };

    let mut direct = fixture
        .pool
        .begin()
        .await
        .expect("begin direct publication");
    publish(
        &mut direct,
        &fixture.beneficiary_id,
        ticket.expected_projection_revision,
        "direct-publication",
        "direct-evidence",
        &CoverageProjection::Complete {
            paid_intervals: vec![],
        },
    )
    .await
    .expect("publish direct correction");
    direct.commit().await.expect("commit direct publication");

    let observation = SourceObservation::Unavailable {
        source_id: source.source_id,
        evidence_reference: "source-evidence".into(),
        reason: UnavailableReason::NeedsReconciliation,
    };
    let mut tx = fixture.pool.begin().await.expect("begin stale finish");
    let result = finish_collection(&mut tx, &ticket, "collection-evidence", &[observation]).await;
    tx.rollback().await.expect("rollback stale finish");
    assert!(matches!(
        result,
        Err(ReconciliationError::Store(
            StoreError::RevisionConflict { .. }
        ))
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn incomplete_collection_does_not_publish_and_conflicting_evidence_wins() {
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
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-errors"),
        )
        .await
        .expect("begin collection");
        tx.commit().await.expect("commit collection begin");
        ticket
    };
    let mut tx = fixture.pool.begin().await.expect("begin incomplete finish");
    let result = finish_collection(&mut tx, &ticket, "evidence-incomplete", &[]).await;
    tx.rollback().await.expect("rollback incomplete finish");
    assert!(matches!(
        result,
        Err(ReconciliationError::SourceBatchMismatch)
    ));
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::NeedsReconciliation
        ))
    ));

    let observation = SourceObservation::Unavailable {
        source_id: source.source_id,
        evidence_reference: "evidence-conflict".into(),
        reason: UnavailableReason::ConflictingEvidence,
    };
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin unavailable finish");
    finish_collection(&mut tx, &ticket, "evidence-conflict", &[observation])
        .await
        .expect("finish conflicting collection");
    tx.commit().await.expect("commit unavailable finish");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::ConflictingEvidence
        ))
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn mixed_source_results_publish_no_known_subset_and_prefer_conflicting_evidence() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let complete_source = binding(&fixture, "complete", "allocation-complete");
    let unavailable_source = binding(&fixture, "unavailable", "allocation-unavailable");
    register(&fixture, &complete_source, "registration-complete").await;
    register(&fixture, &unavailable_source, "registration-unavailable").await;
    let ticket = begin(&fixture, &attempt_id(&fixture, "mixed")).await;
    let observations = [
        SourceObservation::Complete {
            source_id: complete_source.source_id.clone(),
            evidence_reference: "complete-evidence".into(),
            paid_intervals: vec![ConfirmedPaidInterval {
                coverage_id: "known".into(),
                source_id: complete_source.source_id.clone(),
                starts_at: 0,
                paid_until: 100,
                failed_renewal_id: None,
            }],
        },
        SourceObservation::Unavailable {
            source_id: unavailable_source.source_id,
            evidence_reference: "unavailable-evidence".into(),
            reason: UnavailableReason::ConflictingEvidence,
        },
    ];
    let mut tx = fixture.pool.begin().await.expect("begin mixed finish");
    finish_collection(&mut tx, &ticket, "aggregate-evidence", &observations)
        .await
        .expect("finish mixed collection");
    tx.commit().await.expect("commit mixed finish");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::ConflictingEvidence
        ))
    ));
    let fact_count: i64 = sqlx::query_scalar(
        "SELECT fact_count FROM cloud_coverage_revisions \
         WHERE beneficiary_id = $1 ORDER BY revision DESC LIMIT 1",
    )
    .bind(&fixture.beneficiary_id)
    .fetch_one(&fixture.pool)
    .await
    .expect("read unavailable fact count");
    assert_eq!(fact_count, 0);
    cleanup(&fixture).await;
}

#[tokio::test]
async fn rolling_back_a_finish_keeps_the_ticket_retryable() {
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
        let ticket = begin_collection(
            &mut tx,
            &fixture.beneficiary_id,
            &attempt_id(&fixture, "collection-rollback"),
        )
        .await
        .expect("begin collection");
        tx.commit().await.expect("commit collection begin");
        ticket
    };
    let observation = SourceObservation::Complete {
        source_id: source.source_id,
        evidence_reference: "source-evidence".into(),
        paid_intervals: vec![],
    };
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin rolled-back finish");
    finish_collection(
        &mut tx,
        &ticket,
        "evidence-rollback",
        std::slice::from_ref(&observation),
    )
    .await
    .expect("finish before rollback");
    tx.rollback().await.expect("rollback finish");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::NeedsReconciliation
        ))
    ));

    let mut tx = fixture.pool.begin().await.expect("begin retried finish");
    let receipt = finish_collection(&mut tx, &ticket, "evidence-rollback", &[observation])
        .await
        .expect("retry finish after rollback");
    tx.commit().await.expect("commit retried finish");
    assert_eq!(receipt.revision, 2);
    cleanup(&fixture).await;
}
