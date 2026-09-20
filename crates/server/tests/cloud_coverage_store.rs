use std::{str::FromStr, sync::Arc};

use sotto_server::cloud_coverage::{evaluate, ConfirmedPaidInterval, CoverageState};
use sotto_server::cloud_coverage_store::{
    load, publish, CoverageProjection, PublicationOutcome, StoreError, UnavailableReason,
};
use sotto_server::db;
use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use tokio::sync::Barrier;
use uuid::Uuid;

const DAY: i64 = 24 * 60 * 60;

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
            "refusing coverage store tests against non-local host: {}",
            options.get_host()
        );
        let pool = db::connect(&database_url).await.expect("connect");
        db::migrate(&pool).await.expect("migrate");
        let beneficiary_id = format!("coverage-store-test-{}", Uuid::new_v4());
        sqlx::query(
            "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'coverage-test', $2)",
        )
        .bind(&beneficiary_id)
        .bind(&beneficiary_id)
        .execute(&pool)
        .await
        .expect("insert coverage test user");
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
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(&fixture.beneficiary_id)
        .execute(&fixture.pool)
        .await
        .expect("delete coverage test user");
}

fn paid(id: &str, source: &str, starts_at: i64, paid_until: i64) -> ConfirmedPaidInterval {
    ConfirmedPaidInterval {
        coverage_id: id.into(),
        source_id: source.into(),
        starts_at,
        paid_until,
        failed_renewal_id: None,
    }
}

fn recovery(
    id: &str,
    source: &str,
    starts_at: i64,
    paid_until: i64,
    renewal: &str,
) -> ConfirmedPaidInterval {
    ConfirmedPaidInterval {
        failed_renewal_id: Some(renewal.into()),
        ..paid(id, source, starts_at, paid_until)
    }
}

async fn committed_publish(
    fixture: &Fixture,
    expected_revision: Option<i64>,
    operation_id: &str,
    evidence_reference: &str,
    projection: &CoverageProjection,
) -> Result<sotto_server::cloud_coverage_store::PublicationReceipt, StoreError> {
    let mut tx = fixture.pool.begin().await.expect("begin publication");
    let receipt = publish(
        &mut tx,
        &fixture.beneficiary_id,
        expected_revision,
        operation_id,
        evidence_reference,
        projection,
    )
    .await?;
    tx.commit().await.expect("commit publication");
    Ok(receipt)
}

#[tokio::test]
async fn missing_and_complete_empty_projection_are_distinct() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionMissing)
    ));

    let receipt = committed_publish(
        &fixture,
        None,
        "empty-op",
        "empty-evidence",
        &CoverageProjection::Complete {
            paid_intervals: vec![],
        },
    )
    .await
    .expect("publish empty projection");
    assert_eq!(receipt.revision, 1);
    let loaded = load(&fixture.pool, &fixture.beneficiary_id)
        .await
        .expect("load empty projection");
    assert_eq!(loaded.coverage.paid_intervals, Vec::new());
    assert_eq!(
        evaluate(&loaded.coverage, 5 * DAY).unwrap().state,
        CoverageState::Free
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn complete_facts_round_trip_and_exact_replay_is_idempotent() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let facts = vec![
        recovery("renewal", "personal", 0, 30 * DAY, "renewal-1"),
        paid("future", "sponsor", 40 * DAY, 70 * DAY),
    ];
    let first = committed_publish(
        &fixture,
        None,
        "operation-1",
        "evidence-1",
        &CoverageProjection::Complete {
            paid_intervals: facts.clone(),
        },
    )
    .await
    .expect("publish complete projection");
    assert_eq!(first.outcome, PublicationOutcome::Applied);

    let mut reordered = facts;
    reordered.reverse();
    reordered.push(reordered[0].clone());
    let replay = committed_publish(
        &fixture,
        None,
        "operation-1",
        "evidence-1",
        &CoverageProjection::Complete {
            paid_intervals: reordered,
        },
    )
    .await
    .expect("replay complete projection");
    assert_eq!(replay.revision, first.revision);
    assert_eq!(replay.outcome, PublicationOutcome::AlreadyApplied);

    let loaded = load(&fixture.pool, &fixture.beneficiary_id)
        .await
        .expect("load complete projection");
    assert_eq!(loaded.revision, 1);
    assert_eq!(loaded.coverage.paid_intervals.len(), 2);
    assert_eq!(loaded.coverage.paid_intervals[0].coverage_id, "future");
    cleanup(&fixture).await;
}

#[tokio::test]
async fn operation_conflict_and_stale_replay_cannot_rewind_head() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let first_projection = CoverageProjection::Complete {
        paid_intervals: vec![paid("first", "personal", 0, 30 * DAY)],
    };
    committed_publish(
        &fixture,
        None,
        "operation-1",
        "evidence-1",
        &first_projection,
    )
    .await
    .expect("publish first projection");
    committed_publish(
        &fixture,
        Some(1),
        "operation-2",
        "evidence-2",
        &CoverageProjection::Complete {
            paid_intervals: vec![paid("second", "personal", 30 * DAY, 60 * DAY)],
        },
    )
    .await
    .expect("publish correction");
    assert!(matches!(
        committed_publish(
            &fixture,
            Some(1),
            "competing-correction",
            "evidence-competing",
            &CoverageProjection::Complete {
                paid_intervals: vec![paid("competing", "personal", 30 * DAY, 90 * DAY)],
            },
        )
        .await,
        Err(StoreError::RevisionConflict {
            expected: Some(1),
            actual: Some(2),
        })
    ));

    let replay = committed_publish(
        &fixture,
        None,
        "operation-1",
        "evidence-1",
        &first_projection,
    )
    .await
    .expect("replay old operation");
    assert_eq!(replay.outcome, PublicationOutcome::AlreadyApplied);
    assert_eq!(replay.revision, 1);
    assert!(matches!(
        committed_publish(
            &fixture,
            None,
            "operation-1",
            "changed-evidence",
            &first_projection,
        )
        .await,
        Err(StoreError::OperationConflict)
    ));
    assert!(matches!(
        committed_publish(
            &fixture,
            None,
            "stale-operation",
            "stale-evidence",
            &first_projection,
        )
        .await,
        Err(StoreError::RevisionConflict {
            expected: None,
            actual: Some(2),
        })
    ));
    assert_eq!(
        load(&fixture.pool, &fixture.beneficiary_id)
            .await
            .unwrap()
            .revision,
        2
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn invalid_publication_does_not_create_a_head() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin invalid publication");
    assert!(matches!(
        publish(
            &mut tx,
            &fixture.beneficiary_id,
            None,
            "invalid",
            "evidence-invalid",
            &CoverageProjection::Complete {
                paid_intervals: vec![paid("invalid", "personal", 10, 10)],
            },
        )
        .await,
        Err(StoreError::InvalidCoverage(
            sotto_server::cloud_coverage::InvalidCoverage::InvalidInterval
        ))
    ));
    tx.rollback().await.expect("rollback invalid publication");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionMissing)
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn first_revision_conflict_does_not_leave_an_empty_head() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let mut tx = fixture
        .pool
        .begin()
        .await
        .expect("begin first revision conflict");
    assert!(matches!(
        publish(
            &mut tx,
            &fixture.beneficiary_id,
            Some(1),
            "wrong-first-revision",
            "evidence-wrong-first-revision",
            &CoverageProjection::Complete {
                paid_intervals: vec![],
            },
        )
        .await,
        Err(StoreError::RevisionConflict {
            expected: Some(1),
            actual: None,
        })
    ));
    tx.commit()
        .await
        .expect("commit unrelated work after conflict");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionMissing)
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn simultaneous_first_publications_have_one_winner() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let barrier = Arc::new(Barrier::new(2));
    let first_pool = fixture.pool.clone();
    let second_pool = fixture.pool.clone();
    let beneficiary = fixture.beneficiary_id.clone();
    let first_barrier = barrier.clone();
    let second_barrier = barrier;
    let publish_one = async move {
        let mut tx = first_pool.begin().await.expect("begin first race");
        first_barrier.wait().await;
        let result = publish(
            &mut tx,
            &beneficiary,
            None,
            "race-one",
            "race-evidence-one",
            &CoverageProjection::Complete {
                paid_intervals: vec![paid("one", "personal", 0, 30 * DAY)],
            },
        )
        .await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit first race");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback first race");
                Err(error)
            }
        }
    };
    let beneficiary = fixture.beneficiary_id.clone();
    let publish_two = async move {
        let mut tx = second_pool.begin().await.expect("begin second race");
        second_barrier.wait().await;
        let result = publish(
            &mut tx,
            &beneficiary,
            None,
            "race-two",
            "race-evidence-two",
            &CoverageProjection::Complete {
                paid_intervals: vec![paid("two", "personal", 0, 30 * DAY)],
            },
        )
        .await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit second race");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback second race");
                Err(error)
            }
        }
    };
    let (first, second) = tokio::join!(publish_one, publish_two);
    let results = [first, second];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(receipt) if receipt.outcome == PublicationOutcome::Applied))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StoreError::RevisionConflict { .. })))
            .count(),
        1
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn simultaneous_replays_of_one_operation_have_one_application() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let barrier = Arc::new(Barrier::new(2));
    let first_pool = fixture.pool.clone();
    let second_pool = fixture.pool.clone();
    let first_beneficiary = fixture.beneficiary_id.clone();
    let second_beneficiary = fixture.beneficiary_id.clone();
    let first_barrier = barrier.clone();
    let second_barrier = barrier;
    let publish_one = async move {
        let mut tx = first_pool.begin().await.expect("begin first replay race");
        first_barrier.wait().await;
        let result = publish(
            &mut tx,
            &first_beneficiary,
            None,
            "same-operation",
            "same-evidence",
            &CoverageProjection::Complete {
                paid_intervals: vec![paid("same", "personal", 0, 30 * DAY)],
            },
        )
        .await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit first replay race");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback first replay race");
                Err(error)
            }
        }
    };
    let publish_two = async move {
        let mut tx = second_pool.begin().await.expect("begin second replay race");
        second_barrier.wait().await;
        let result = publish(
            &mut tx,
            &second_beneficiary,
            None,
            "same-operation",
            "same-evidence",
            &CoverageProjection::Complete {
                paid_intervals: vec![paid("same", "personal", 0, 30 * DAY)],
            },
        )
        .await;
        match result {
            Ok(receipt) => {
                tx.commit().await.expect("commit second replay race");
                Ok(receipt)
            }
            Err(error) => {
                tx.rollback().await.expect("rollback second replay race");
                Err(error)
            }
        }
    };
    let (first, second) = tokio::join!(publish_one, publish_two);
    let results = [first, second];
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(receipt) if receipt.outcome == PublicationOutcome::Applied))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Ok(receipt) if receipt.outcome == PublicationOutcome::AlreadyApplied))
            .count(),
        1
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn unavailable_projection_blocks_load_until_complete_revision() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    committed_publish(
        &fixture,
        None,
        "unavailable-op",
        "reconcile-1",
        &CoverageProjection::Unavailable {
            reason: UnavailableReason::NeedsReconciliation,
        },
    )
    .await
    .expect("publish unavailable projection");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionUnavailable(
            UnavailableReason::NeedsReconciliation
        ))
    ));

    committed_publish(
        &fixture,
        Some(1),
        "complete-op",
        "reconcile-2",
        &CoverageProjection::Complete {
            paid_intervals: vec![paid("paid", "personal", 0, 30 * DAY)],
        },
    )
    .await
    .expect("publish recovered projection");
    assert_eq!(
        load(&fixture.pool, &fixture.beneficiary_id)
            .await
            .unwrap()
            .revision,
        2
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn rollback_leaves_missing_projection_and_retry_can_apply() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let mut tx = fixture.pool.begin().await.expect("begin rollback");
    publish(
        &mut tx,
        &fixture.beneficiary_id,
        None,
        "rolled-back",
        "evidence",
        &CoverageProjection::Complete {
            paid_intervals: vec![paid("paid", "personal", 0, 30 * DAY)],
        },
    )
    .await
    .expect("publish before rollback");
    tx.rollback().await.expect("rollback publication");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::ProjectionMissing)
    ));
    committed_publish(
        &fixture,
        None,
        "rolled-back",
        "evidence",
        &CoverageProjection::Complete {
            paid_intervals: vec![paid("paid", "personal", 0, 30 * DAY)],
        },
    )
    .await
    .expect("retry after rollback");
    cleanup(&fixture).await;
}

#[tokio::test]
async fn renewal_correction_replaces_recovery_without_rewriting_old_revision() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let old = CoverageProjection::Complete {
        paid_intervals: vec![recovery("paid", "personal", 0, 30 * DAY, "renewal-1")],
    };
    committed_publish(&fixture, None, "old", "evidence-old", &old)
        .await
        .expect("publish old recovery");
    let renewed = CoverageProjection::Complete {
        paid_intervals: vec![
            paid("paid", "personal", 0, 30 * DAY),
            paid("renewed", "personal", 30 * DAY, 60 * DAY),
        ],
    };
    let mut pending = fixture.pool.begin().await.expect("begin confirmed renewal");
    publish(
        &mut pending,
        &fixture.beneficiary_id,
        Some(1),
        "renewed",
        "evidence-renewed",
        &renewed,
    )
    .await
    .expect("publish confirmed renewal");
    assert_eq!(
        load(&fixture.pool, &fixture.beneficiary_id)
            .await
            .expect("read old committed revision")
            .revision,
        1
    );
    pending.commit().await.expect("commit confirmed renewal");
    let loaded = load(&fixture.pool, &fixture.beneficiary_id)
        .await
        .expect("load renewed coverage");
    let decision = evaluate(&loaded.coverage, 31 * DAY).unwrap();
    assert_eq!(decision.state, CoverageState::Paid);
    assert_eq!(decision.active_until, Some(60 * DAY));
    assert_eq!(
        evaluate(&loaded.coverage, 60 * DAY).unwrap().export_until,
        Some(90 * DAY)
    );

    let replay = committed_publish(&fixture, None, "old", "evidence-old", &old)
        .await
        .expect("replay old recovery");
    assert_eq!(replay.outcome, PublicationOutcome::AlreadyApplied);
    assert_eq!(
        load(&fixture.pool, &fixture.beneficiary_id)
            .await
            .unwrap()
            .revision,
        2
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn future_max_timestamp_can_be_stored_without_eager_export_overflow() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    committed_publish(
        &fixture,
        None,
        "future-max",
        "evidence-max",
        &CoverageProjection::Complete {
            paid_intervals: vec![paid("future", "personal", i64::MAX - 1, i64::MAX)],
        },
    )
    .await
    .expect("publish future max interval");
    let loaded = load(&fixture.pool, &fixture.beneficiary_id)
        .await
        .expect("load future max interval");
    assert_eq!(
        evaluate(&loaded.coverage, 5).unwrap().state,
        CoverageState::Free
    );
    cleanup(&fixture).await;
}

#[tokio::test]
async fn stored_fact_count_mismatch_fails_closed() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    committed_publish(
        &fixture,
        None,
        "corrupt-count",
        "evidence-corrupt",
        &CoverageProjection::Complete {
            paid_intervals: vec![paid("paid", "personal", 0, 30 * DAY)],
        },
    )
    .await
    .expect("publish projection to corrupt");
    sqlx::query(
        "UPDATE cloud_coverage_revisions SET fact_count = 0 \
         WHERE beneficiary_id = $1 AND revision = 1",
    )
    .bind(&fixture.beneficiary_id)
    .execute(&fixture.pool)
    .await
    .expect("corrupt stored fact count");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::CorruptProjection(_))
    ));
    cleanup(&fixture).await;
}

#[tokio::test]
async fn unavailable_projection_with_stored_facts_fails_closed() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    committed_publish(
        &fixture,
        None,
        "corrupt-unavailable",
        "evidence-corrupt-unavailable",
        &CoverageProjection::Unavailable {
            reason: UnavailableReason::NeedsReconciliation,
        },
    )
    .await
    .expect("publish unavailable projection to corrupt");
    sqlx::query(
        "INSERT INTO cloud_coverage_revision_facts \
         (beneficiary_id, revision, coverage_id, source_id, starts_at, paid_until) \
         VALUES ($1, 1, 'unexpected', 'source', 0, 10)",
    )
    .bind(&fixture.beneficiary_id)
    .execute(&fixture.pool)
    .await
    .expect("insert unexpected unavailable fact");
    assert!(matches!(
        load(&fixture.pool, &fixture.beneficiary_id).await,
        Err(StoreError::CorruptProjection(_))
    ));
    cleanup(&fixture).await;
}
