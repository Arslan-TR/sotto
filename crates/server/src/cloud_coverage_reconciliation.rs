//! Coordination for trusted person-level Cloud coverage sources.
//!
//! This module records which external allocations belong to a beneficiary and serialises later
//! complete collections. It does not call a provider or decide whether an allocation is paid.
//! Callers provide evidence that has already passed the provider-specific checks.

use std::fmt;

use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Row, Transaction};
use thiserror::Error;

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
    #[error("source allocation is already bound to another beneficiary")]
    SourceBindingConflict,
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

    if let Some(row) = sqlx::query(
        "SELECT beneficiary_id, source_id, provider_namespace, external_allocation_reference, \
                ownership_evidence_reference \
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
            return Ok(RegistrationReceipt {
                source_id: stored.source_id,
                source_set_generation: generation,
                projection_revision: current_revision(tx, &binding.beneficiary_id).await?,
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
          ownership_evidence_reference, registration_operation_id) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(&binding.source_id)
    .bind(&binding.beneficiary_id)
    .bind(&binding.provider_namespace)
    .bind(&binding.external_allocation_reference)
    .bind(&binding.ownership_evidence_reference)
    .bind(operation_id)
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

    Ok(RegistrationReceipt {
        source_id: binding.source_id.clone(),
        source_set_generation: next_generation,
        projection_revision: Some(publication.revision),
        outcome: projection_outcome(publication.outcome),
    })
}

fn validate_operation_id(operation_id: &str) -> Result<(), ReconciliationError> {
    if operation_id.trim().is_empty() {
        Err(ReconciliationError::EmptyOperationId)
    } else {
        Ok(())
    }
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
                        | "cloud_coverage_sources_source_id_key"
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
