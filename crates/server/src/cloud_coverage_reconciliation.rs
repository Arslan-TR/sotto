//! Coordination for trusted person-level Cloud coverage sources.
//!
//! This module records which external allocations belong to a beneficiary and serialises later
//! complete collections. It does not call a provider or decide whether an allocation is paid.
//! Callers provide evidence that has already passed the provider-specific checks.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Row, Transaction};
use thiserror::Error;

use crate::cloud_coverage::{
    normalise_confirmed_intervals, ConfirmedPaidInterval, InvalidCoverage, PersonCoverage,
};
use crate::cloud_coverage_store::{
    current_revision, publish, CoverageProjection, PublicationOutcome, StoreError,
    UnavailableReason,
};

/// The immutable identity that binds one external allocation to a beneficiary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBinding {
    pub beneficiary_id: String,
    pub source_id: String,
    pub provider_namespace: String,
    pub external_allocation_reference: String,
    pub ownership_evidence_reference: String,
}

/// Whether a source registration was newly applied or exactly replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    Applied,
    AlreadyApplied,
}

/// The durable result of registering one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationReceipt {
    pub source_id: String,
    pub source_set_generation: i64,
    pub projection_revision: Option<i64>,
    pub outcome: RegistrationOutcome,
}

/// The lifecycle state of a durable collection attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionStatus {
    Pending,
    Superseded,
    Completed,
}

/// A source set snapshot and projection revision captured before provider collection begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionTicket {
    pub beneficiary_id: String,
    pub attempt_id: String,
    pub collection_epoch: i64,
    pub source_set_generation: i64,
    pub expected_projection_revision: Option<i64>,
    pub source_bindings: Vec<SourceBinding>,
    pub status: CollectionStatus,
    pub completed_revision: Option<i64>,
}

/// A complete or explicitly unavailable observation for one registered source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceObservation {
    Complete {
        source_id: String,
        evidence_reference: String,
        paid_intervals: Vec<ConfirmedPaidInterval>,
    },
    Unavailable {
        source_id: String,
        evidence_reference: String,
        reason: UnavailableReason,
    },
}

/// The result of completing a collection attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationReceipt {
    pub attempt_id: String,
    pub revision: i64,
    pub outcome: PublicationOutcome,
}

/// Source coordination and validation failures.
#[derive(Debug, Error)]
pub enum ReconciliationError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("coverage store error: {0}")]
    Store(#[from] StoreError),
    #[error("invalid source binding: {0}")]
    InvalidSource(String),
    #[error("registration operation_id must not be empty")]
    EmptyOperationId,
    #[error("source registration conflicts with an existing binding")]
    RegistrationConflict,
    #[error("existing coverage is not owned by the reconciliation coordinator")]
    BootstrapConflict,
    #[error("stored source registration receipt is incomplete")]
    CorruptRegistration,
    #[error("source allocation is already bound to another beneficiary")]
    SourceBindingConflict,
    #[error("collection attempt_id must not be empty")]
    EmptyAttemptId,
    #[error("no registered coverage sources exist for this beneficiary")]
    NoSources,
    #[error("collection attempt does not exist")]
    AttemptMissing,
    #[error("collection attempt is superseded")]
    AttemptSuperseded,
    #[error("collection attempt conflicts with current source or projection state")]
    CollectionConflict,
    #[error("collection source batch does not match the registered source set")]
    SourceBatchMismatch,
    #[error("source observations conflict: {0}")]
    SourceObservationConflict(String),
    #[error("invalid source observation: {0}")]
    InvalidObservation(#[from] InvalidCoverage),
    #[error("collection epoch is exhausted")]
    EpochOverflow,
    #[error("source set generation is exhausted")]
    GenerationOverflow,
    #[error("serialised reconciliation value is invalid: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl fmt::Display for RegistrationOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Applied => "applied",
            Self::AlreadyApplied => "already_applied",
        })
    }
}

/// Register a source and invalidate completeness in one caller-owned transaction.
///
/// The caller must commit on success and roll back on error. The first registration publishes an
/// unavailable projection, and adding any later source advances the source-set generation and
/// publishes a new unavailable revision. No provider call is made while the coordinator is locked.
pub async fn register_source(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: &str,
    binding: &SourceBinding,
) -> Result<RegistrationReceipt, ReconciliationError> {
    validate_operation_id(operation_id)?;
    validate_binding(binding)?;

    let coordinator_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM cloud_coverage_coordinators WHERE beneficiary_id = $1)",
    )
    .bind(&binding.beneficiary_id)
    .fetch_one(&mut **tx)
    .await?;
    let existing_projection_revision = current_revision(tx, &binding.beneficiary_id).await?;
    if !coordinator_exists && existing_projection_revision.is_some() {
        return Err(ReconciliationError::BootstrapConflict);
    }

    sqlx::query(
        "INSERT INTO cloud_coverage_coordinators (beneficiary_id) VALUES ($1) \
         ON CONFLICT (beneficiary_id) DO NOTHING",
    )
    .bind(&binding.beneficiary_id)
    .execute(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?;

    let coordinator = sqlx::query(
        "SELECT source_set_generation FROM cloud_coverage_coordinators \
         WHERE beneficiary_id = $1 FOR UPDATE",
    )
    .bind(&binding.beneficiary_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?;
    let generation: i64 = coordinator
        .try_get("source_set_generation")
        .map_err(ReconciliationError::Database)?;
    if generation == 0 && existing_projection_revision.is_some() {
        return Err(ReconciliationError::BootstrapConflict);
    }

    if let Some(row) = sqlx::query(
        "SELECT beneficiary_id, source_id, provider_namespace, external_allocation_reference, \
                ownership_evidence_reference, registration_source_set_generation, \
                registration_projection_revision \
         FROM cloud_coverage_sources \
         WHERE beneficiary_id = $1 AND registration_operation_id = $2",
    )
    .bind(&binding.beneficiary_id)
    .bind(operation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?
    {
        let stored = source_binding_from_row(&row)?;
        if &stored == binding {
            let projection_revision: Option<i64> =
                row.try_get("registration_projection_revision")?;
            let Some(projection_revision) = projection_revision else {
                return Err(ReconciliationError::CorruptRegistration);
            };
            return Ok(RegistrationReceipt {
                source_id: stored.source_id,
                source_set_generation: row.try_get("registration_source_set_generation")?,
                projection_revision: Some(projection_revision),
                outcome: RegistrationOutcome::AlreadyApplied,
            });
        }
        return Err(ReconciliationError::RegistrationConflict);
    }

    let source_id_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM cloud_coverage_sources WHERE source_id = $1)",
    )
    .bind(&binding.source_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?;
    let external_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM cloud_coverage_sources \
                        WHERE provider_namespace = $1 AND external_allocation_reference = $2)",
    )
    .bind(&binding.provider_namespace)
    .bind(&binding.external_allocation_reference)
    .fetch_one(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?;
    if source_id_exists || external_exists {
        return Err(ReconciliationError::SourceBindingConflict);
    }

    let next_generation = generation
        .checked_add(1)
        .ok_or(ReconciliationError::GenerationOverflow)?;
    let expected_revision = current_revision(tx, &binding.beneficiary_id).await?;

    sqlx::query(
        "INSERT INTO cloud_coverage_sources \
         (source_id, beneficiary_id, provider_namespace, external_allocation_reference, \
          ownership_evidence_reference, registration_operation_id, registration_source_set_generation) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&binding.source_id)
    .bind(&binding.beneficiary_id)
    .bind(&binding.provider_namespace)
    .bind(&binding.external_allocation_reference)
    .bind(&binding.ownership_evidence_reference)
    .bind(operation_id)
    .bind(next_generation)
    .execute(&mut **tx)
    .await
    .map_err(map_source_insert_error)?;

    sqlx::query(
        "UPDATE cloud_coverage_collection_attempts SET status = 'superseded' \
         WHERE beneficiary_id = $1 AND status = 'pending'",
    )
    .bind(&binding.beneficiary_id)
    .execute(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?;
    sqlx::query(
        "UPDATE cloud_coverage_coordinators \
         SET source_set_generation = $2, current_attempt_id = NULL \
         WHERE beneficiary_id = $1",
    )
    .bind(&binding.beneficiary_id)
    .bind(next_generation)
    .execute(&mut **tx)
    .await
    .map_err(ReconciliationError::Database)?;

    let publication = publish(
        tx,
        &binding.beneficiary_id,
        expected_revision,
        &format!("source-registration:{operation_id}"),
        &binding.ownership_evidence_reference,
        &unavailable_projection(),
    )
    .await?;

    sqlx::query(
        "UPDATE cloud_coverage_sources SET registration_projection_revision = $2 \
         WHERE source_id = $1",
    )
    .bind(&binding.source_id)
    .bind(publication.revision)
    .execute(&mut **tx)
    .await?;

    Ok(RegistrationReceipt {
        source_id: binding.source_id.clone(),
        source_set_generation: next_generation,
        projection_revision: Some(publication.revision),
        outcome: projection_outcome(publication.outcome),
    })
}

/// Begin a collection against the current complete source set.
///
/// The returned ticket is durable only after the caller commits. A new attempt supersedes any
/// pending attempt for the same beneficiary. No provider call belongs inside this transaction.
pub async fn begin_collection(
    tx: &mut Transaction<'_, Postgres>,
    beneficiary_id: &str,
    attempt_id: &str,
) -> Result<CollectionTicket, ReconciliationError> {
    validate_identifier(beneficiary_id, "beneficiary_id")?;
    validate_attempt_id(attempt_id)?;

    let coordinator = sqlx::query(
        "SELECT source_set_generation, collection_epoch, current_attempt_id \
         FROM cloud_coverage_coordinators WHERE beneficiary_id = $1 FOR UPDATE",
    )
    .bind(beneficiary_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(coordinator) = coordinator else {
        return Err(ReconciliationError::NoSources);
    };
    let generation: i64 = coordinator.try_get("source_set_generation")?;
    let epoch: i64 = coordinator.try_get("collection_epoch")?;
    let current_attempt_id: Option<String> = coordinator.try_get("current_attempt_id")?;

    if let Some(existing) = sqlx::query(
        "SELECT attempt_id, beneficiary_id, collection_epoch, source_set_generation, \
                expected_projection_revision, source_bindings::text AS source_bindings, status, \
                projection_revision \
         FROM cloud_coverage_collection_attempts WHERE attempt_id = $1",
    )
    .bind(attempt_id)
    .fetch_optional(&mut **tx)
    .await?
    {
        let existing_beneficiary: String = existing.try_get("beneficiary_id")?;
        if existing_beneficiary != beneficiary_id {
            return Err(ReconciliationError::CollectionConflict);
        }
        return collection_ticket_from_row(&existing);
    }

    let source_rows = sqlx::query(
        "SELECT beneficiary_id, source_id, provider_namespace, external_allocation_reference, \
                ownership_evidence_reference \
         FROM cloud_coverage_sources WHERE beneficiary_id = $1 ORDER BY source_id COLLATE \"C\"",
    )
    .bind(beneficiary_id)
    .fetch_all(&mut **tx)
    .await?;
    if source_rows.is_empty() || generation == 0 {
        return Err(ReconciliationError::NoSources);
    }
    let source_bindings = source_rows
        .iter()
        .map(source_binding_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let source_bindings_json = serde_json::to_string(&source_bindings)?;
    let expected_projection_revision = current_revision(tx, beneficiary_id).await?;
    let next_epoch = epoch
        .checked_add(1)
        .ok_or(ReconciliationError::EpochOverflow)?;

    if let Some(previous_attempt_id) = current_attempt_id {
        sqlx::query(
            "UPDATE cloud_coverage_collection_attempts SET status = 'superseded' \
             WHERE attempt_id = $1 AND status = 'pending'",
        )
        .bind(previous_attempt_id)
        .execute(&mut **tx)
        .await?;
    }

    sqlx::query(
        "INSERT INTO cloud_coverage_collection_attempts \
         (attempt_id, beneficiary_id, collection_epoch, source_set_generation, \
          expected_projection_revision, source_bindings, status) \
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'pending')",
    )
    .bind(attempt_id)
    .bind(beneficiary_id)
    .bind(next_epoch)
    .bind(generation)
    .bind(expected_projection_revision)
    .bind(source_bindings_json)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE cloud_coverage_coordinators SET collection_epoch = $2, current_attempt_id = $3 \
         WHERE beneficiary_id = $1",
    )
    .bind(beneficiary_id)
    .bind(next_epoch)
    .bind(attempt_id)
    .execute(&mut **tx)
    .await?;

    Ok(CollectionTicket {
        beneficiary_id: beneficiary_id.into(),
        attempt_id: attempt_id.into(),
        collection_epoch: next_epoch,
        source_set_generation: generation,
        expected_projection_revision,
        source_bindings,
        status: CollectionStatus::Pending,
        completed_revision: None,
    })
}

/// Finish a collection, combine every registered source and publish one atomic projection.
///
/// The caller must commit on success and roll back on error. Provider work must be complete before
/// this function is called. A pending attempt is never converted into an empty result by timeout.
pub async fn finish_collection(
    tx: &mut Transaction<'_, Postgres>,
    ticket: &CollectionTicket,
    aggregate_evidence_reference: &str,
    observations: &[SourceObservation],
) -> Result<ReconciliationReceipt, ReconciliationError> {
    validate_identifier(&ticket.beneficiary_id, "beneficiary_id")?;
    validate_attempt_id(&ticket.attempt_id)?;
    validate_identifier(aggregate_evidence_reference, "aggregate_evidence_reference")?;

    let coordinator = sqlx::query(
        "SELECT source_set_generation, collection_epoch, current_attempt_id \
         FROM cloud_coverage_coordinators WHERE beneficiary_id = $1 FOR UPDATE",
    )
    .bind(&ticket.beneficiary_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ReconciliationError::NoSources)?;
    let current_generation: i64 = coordinator.try_get("source_set_generation")?;
    let current_epoch: i64 = coordinator.try_get("collection_epoch")?;
    let current_attempt: Option<String> = coordinator.try_get("current_attempt_id")?;

    let attempt = sqlx::query(
        "SELECT attempt_id, beneficiary_id, collection_epoch, source_set_generation, \
                expected_projection_revision, source_bindings::text AS source_bindings, status, \
                aggregate_evidence_reference, canonical_result::text AS canonical_result, \
                projection_revision \
         FROM cloud_coverage_collection_attempts WHERE attempt_id = $1",
    )
    .bind(&ticket.attempt_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(ReconciliationError::AttemptMissing)?;
    let stored_ticket = collection_ticket_from_row(&attempt)?;
    if stored_ticket.beneficiary_id != ticket.beneficiary_id
        || stored_ticket.collection_epoch != ticket.collection_epoch
        || stored_ticket.source_set_generation != ticket.source_set_generation
        || stored_ticket.expected_projection_revision != ticket.expected_projection_revision
        || stored_ticket.source_bindings != ticket.source_bindings
    {
        return Err(ReconciliationError::CollectionConflict);
    }

    let (canonical_result, projection) = canonical_collection(
        &ticket.beneficiary_id,
        &ticket.source_bindings,
        observations,
        aggregate_evidence_reference,
    )?;
    let status = stored_ticket.status;
    if status == CollectionStatus::Completed {
        let stored_evidence: String = attempt.try_get("aggregate_evidence_reference")?;
        let stored_result: String = attempt.try_get("canonical_result")?;
        let completed_revision: i64 = attempt.try_get("projection_revision")?;
        if stored_evidence == aggregate_evidence_reference
            && serde_json::from_str::<serde_json::Value>(&stored_result)? == canonical_result
        {
            return Ok(ReconciliationReceipt {
                attempt_id: ticket.attempt_id.clone(),
                revision: completed_revision,
                outcome: PublicationOutcome::AlreadyApplied,
            });
        }
        return Err(ReconciliationError::CollectionConflict);
    }
    if status == CollectionStatus::Superseded {
        return Err(ReconciliationError::AttemptSuperseded);
    }
    if current_attempt.as_deref() != Some(ticket.attempt_id.as_str())
        || current_generation != ticket.source_set_generation
        || current_epoch != ticket.collection_epoch
    {
        return Err(ReconciliationError::CollectionConflict);
    }
    let actual_revision = current_revision(tx, &ticket.beneficiary_id).await?;
    if actual_revision != ticket.expected_projection_revision {
        return Err(ReconciliationError::Store(StoreError::RevisionConflict {
            expected: ticket.expected_projection_revision,
            actual: actual_revision,
        }));
    }

    let publication = publish(
        tx,
        &ticket.beneficiary_id,
        ticket.expected_projection_revision,
        &format!("collection:{}", ticket.attempt_id),
        aggregate_evidence_reference,
        &projection,
    )
    .await?;
    let canonical_result_json = serde_json::to_string(&canonical_result)?;
    sqlx::query(
        "UPDATE cloud_coverage_collection_attempts SET status = 'completed', \
         aggregate_evidence_reference = $2, canonical_result = $3::jsonb, \
         projection_revision = $4, completed_at = now() \
         WHERE attempt_id = $1 AND status = 'pending'",
    )
    .bind(&ticket.attempt_id)
    .bind(aggregate_evidence_reference)
    .bind(canonical_result_json)
    .bind(publication.revision)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE cloud_coverage_coordinators SET current_attempt_id = NULL \
         WHERE beneficiary_id = $1 AND current_attempt_id = $2",
    )
    .bind(&ticket.beneficiary_id)
    .bind(&ticket.attempt_id)
    .execute(&mut **tx)
    .await?;
    Ok(ReconciliationReceipt {
        attempt_id: ticket.attempt_id.clone(),
        revision: publication.revision,
        outcome: publication.outcome,
    })
}

fn canonical_collection(
    beneficiary_id: &str,
    bindings: &[SourceBinding],
    observations: &[SourceObservation],
    aggregate_evidence_reference: &str,
) -> Result<(serde_json::Value, CoverageProjection), ReconciliationError> {
    if observations.len() != bindings.len() {
        return Err(ReconciliationError::SourceBatchMismatch);
    }
    let expected = bindings
        .iter()
        .map(|binding| (binding.source_id.as_str(), binding))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeMap::new();
    let mut canonical_observations = Vec::with_capacity(observations.len());
    let mut complete_facts = Vec::new();
    let mut fact_sources = BTreeMap::new();
    let mut renewal_sources = BTreeMap::new();
    let mut unavailable_reason = None;

    for observation in observations {
        let (source_id, evidence_reference) = match observation {
            SourceObservation::Complete {
                source_id,
                evidence_reference,
                ..
            }
            | SourceObservation::Unavailable {
                source_id,
                evidence_reference,
                ..
            } => (source_id, evidence_reference),
        };
        if evidence_reference.trim().is_empty()
            || !expected.contains_key(source_id.as_str())
            || seen.insert(source_id.clone(), ()).is_some()
        {
            return Err(ReconciliationError::SourceBatchMismatch);
        }
        match observation {
            SourceObservation::Complete { paid_intervals, .. } => {
                let normalised = normalise_confirmed_intervals(&PersonCoverage {
                    beneficiary_id: beneficiary_id.into(),
                    paid_intervals: paid_intervals.clone(),
                })?;
                for interval in &normalised {
                    if interval.source_id != *source_id {
                        return Err(ReconciliationError::SourceObservationConflict(
                            "coverage fact source_id does not match its registered source".into(),
                        ));
                    }
                    if fact_sources
                        .insert(interval.coverage_id.clone(), source_id.clone())
                        .is_some()
                    {
                        return Err(ReconciliationError::SourceObservationConflict(
                            "coverage_id appears in multiple sources".into(),
                        ));
                    }
                    if let Some(renewal_id) = interval.failed_renewal_id.as_deref() {
                        if renewal_sources
                            .insert(renewal_id.to_owned(), source_id.clone())
                            .is_some()
                        {
                            return Err(ReconciliationError::SourceObservationConflict(
                                "failed_renewal_id appears in multiple sources".into(),
                            ));
                        }
                    }
                }
                complete_facts.extend(normalised.iter().cloned());
                canonical_observations.push(serde_json::json!({
                    "source_id": source_id,
                    "evidence_reference": evidence_reference,
                    "status": "complete",
                    "paid_intervals": normalised,
                }));
            }
            SourceObservation::Unavailable { reason, .. } => {
                if *reason == UnavailableReason::ConflictingEvidence {
                    unavailable_reason = Some(UnavailableReason::ConflictingEvidence);
                } else if unavailable_reason.is_none() {
                    unavailable_reason = Some(UnavailableReason::NeedsReconciliation);
                }
                canonical_observations.push(serde_json::json!({
                    "source_id": source_id,
                    "evidence_reference": evidence_reference,
                    "status": "unavailable",
                    "reason": reason.to_string(),
                }));
            }
        }
    }
    if seen.len() != expected.len() {
        return Err(ReconciliationError::SourceBatchMismatch);
    }
    canonical_observations.sort_by(|left, right| {
        left["source_id"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["source_id"].as_str().unwrap_or_default())
    });
    let projection = match unavailable_reason {
        Some(reason) => CoverageProjection::Unavailable { reason },
        None => {
            complete_facts.sort_by(|left, right| left.coverage_id.cmp(&right.coverage_id));
            CoverageProjection::Complete {
                paid_intervals: complete_facts,
            }
        }
    };
    Ok((
        serde_json::json!({
            "aggregate_evidence_reference": aggregate_evidence_reference,
            "sources": canonical_observations,
        }),
        projection,
    ))
}

fn validate_operation_id(operation_id: &str) -> Result<(), ReconciliationError> {
    if operation_id.trim().is_empty() {
        Err(ReconciliationError::EmptyOperationId)
    } else {
        Ok(())
    }
}

fn validate_attempt_id(attempt_id: &str) -> Result<(), ReconciliationError> {
    if attempt_id.trim().is_empty() {
        Err(ReconciliationError::EmptyAttemptId)
    } else {
        Ok(())
    }
}

fn validate_identifier(value: &str, name: &str) -> Result<(), ReconciliationError> {
    if value.trim().is_empty() {
        Err(ReconciliationError::InvalidSource(format!(
            "{name} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn collection_ticket_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<CollectionTicket, ReconciliationError> {
    let status: String = row.try_get("status")?;
    let status = match status.as_str() {
        "pending" => CollectionStatus::Pending,
        "superseded" => CollectionStatus::Superseded,
        "completed" => CollectionStatus::Completed,
        _ => {
            return Err(ReconciliationError::CollectionConflict);
        }
    };
    let source_bindings_json: String = row.try_get("source_bindings")?;
    let source_bindings: Vec<SourceBinding> = serde_json::from_str(&source_bindings_json)?;
    let projection_revision: Option<i64> = row.try_get("projection_revision")?;
    Ok(CollectionTicket {
        beneficiary_id: row.try_get("beneficiary_id")?,
        attempt_id: row.try_get("attempt_id")?,
        collection_epoch: row.try_get("collection_epoch")?,
        source_set_generation: row.try_get("source_set_generation")?,
        expected_projection_revision: row.try_get("expected_projection_revision")?,
        source_bindings,
        status,
        completed_revision: projection_revision,
    })
}

fn validate_binding(binding: &SourceBinding) -> Result<(), ReconciliationError> {
    for (name, value) in [
        ("beneficiary_id", binding.beneficiary_id.as_str()),
        ("source_id", binding.source_id.as_str()),
        ("provider_namespace", binding.provider_namespace.as_str()),
        (
            "external_allocation_reference",
            binding.external_allocation_reference.as_str(),
        ),
        (
            "ownership_evidence_reference",
            binding.ownership_evidence_reference.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(ReconciliationError::InvalidSource(format!(
                "{name} must not be empty"
            )));
        }
    }
    Ok(())
}

fn source_binding_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<SourceBinding, ReconciliationError> {
    Ok(SourceBinding {
        beneficiary_id: row.try_get("beneficiary_id")?,
        source_id: row.try_get("source_id")?,
        provider_namespace: row.try_get("provider_namespace")?,
        external_allocation_reference: row.try_get("external_allocation_reference")?,
        ownership_evidence_reference: row.try_get("ownership_evidence_reference")?,
    })
}

fn map_source_insert_error(error: sqlx::Error) -> ReconciliationError {
    let is_binding_conflict = matches!(
        &error,
        sqlx::Error::Database(database)
            if matches!(
                database.constraint(),
                Some(
                    "cloud_coverage_sources_provider_allocation_key"
                        | "cloud_coverage_sources_pkey"
                )
            )
    );
    if is_binding_conflict {
        ReconciliationError::SourceBindingConflict
    } else {
        ReconciliationError::Database(error)
    }
}

#[allow(dead_code)]
fn projection_outcome(outcome: PublicationOutcome) -> RegistrationOutcome {
    match outcome {
        PublicationOutcome::Applied => RegistrationOutcome::Applied,
        PublicationOutcome::AlreadyApplied => RegistrationOutcome::AlreadyApplied,
    }
}

#[allow(dead_code)]
fn unavailable_projection() -> CoverageProjection {
    CoverageProjection::Unavailable {
        reason: UnavailableReason::NeedsReconciliation,
    }
}
