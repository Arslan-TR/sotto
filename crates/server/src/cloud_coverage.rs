//! Pure person-level Cloud coverage evaluation.
//!
//! This module consumes already-verified coverage facts. It does not talk to Stripe, Postgres or
//! the clock, and it does not grant access to a secret. The later billing projection is responsible
//! for turning provider events into these facts.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Timestamp precision used by the coverage domain: Unix seconds in UTC.
pub type Timestamp = i64;

/// The failed-renewal recovery period agreed for Sotto Cloud.
pub const RENEWAL_RECOVERY_SECONDS: Timestamp = 14 * 24 * 60 * 60;
/// The read/export period after the last eligible coverage episode.
pub const EXPORT_WINDOW_SECONDS: Timestamp = 30 * 24 * 60 * 60;

/// A person whose Cloud coverage is being evaluated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonCoverage {
    /// Opaque account identifier for the beneficiary.
    pub beneficiary_id: String,
    /// Confirmed paid intervals from one or more payer/allocation sources.
    pub paid_intervals: Vec<ConfirmedPaidInterval>,
}

/// A paid interval that has already been verified by the billing projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedPaidInterval {
    /// Stable identity for this coverage fact. Retries must reuse this identity.
    pub coverage_id: String,
    /// Opaque payer or allocation provenance identifier.
    pub source_id: String,
    /// Inclusive start of the half-open paid interval.
    pub starts_at: Timestamp,
    /// Exclusive end of the half-open paid interval.
    pub paid_until: Timestamp,
    /// Stable identity of a failed renewal attached to this paid interval, if any.
    pub failed_renewal_id: Option<String>,
}

/// The account-level Cloud service state at a supplied instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageState {
    Free,
    Paid,
    RenewalRecovery,
    ExportOnly,
    Expired,
}

/// Coverage state and dates needed by later policy and customer-facing layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageDecision {
    pub state: CoverageState,
    /// End of the connected eligible episode containing the evaluation instant.
    pub active_until: Option<Timestamp>,
    /// End of the recovery interval containing the evaluation instant.
    pub recovery_until: Option<Timestamp>,
    /// End of the 30-day export window for the most recent ended episode.
    pub export_until: Option<Timestamp>,
}

/// Invalid trusted coverage input. Invalid input fails closed rather than becoming Free.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InvalidCoverage {
    #[error("beneficiary_id must not be empty")]
    EmptyBeneficiary,
    #[error("coverage_id must not be empty")]
    EmptyCoverageId,
    #[error("source_id must not be empty")]
    EmptySourceId,
    #[error("coverage interval must have starts_at before paid_until")]
    InvalidInterval,
    #[error("failed_renewal_id must not be empty")]
    EmptyRenewalId,
    #[error("failed renewal recovery deadline overflows the timestamp range")]
    RecoveryDeadlineOverflow,
    #[error("duplicate coverage_id has conflicting contents")]
    ConflictingCoverage,
    #[error("the same failed renewal is attached to conflicting paid intervals")]
    ConflictingRenewal,
    #[error("export deadline overflows the timestamp range")]
    ExportDeadlineOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IntervalKind {
    Paid,
    Recovery,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EligibleInterval {
    starts_at: Timestamp,
    ends_at: Timestamp,
    kind: IntervalKind,
    coverage_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Episode {
    starts_at: Timestamp,
    ends_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RenewalIdentity {
    coverage_id: String,
    source_id: String,
    starts_at: Timestamp,
    paid_until: Timestamp,
}

/// Evaluate one person's coverage at `now`.
pub fn evaluate(
    coverage: &PersonCoverage,
    now: Timestamp,
) -> Result<CoverageDecision, InvalidCoverage> {
    let intervals = eligible_intervals(coverage)?;
    let episodes = episodes(&intervals);

    if let Some(current) = episodes
        .iter()
        .find(|episode| episode.starts_at <= now && now < episode.ends_at)
    {
        let paid_now = intervals.iter().any(|interval| {
            interval.kind == IntervalKind::Paid
                && interval.starts_at <= now
                && now < interval.ends_at
        });
        let recovery_until = intervals
            .iter()
            .filter(|interval| {
                interval.kind == IntervalKind::Recovery
                    && interval.starts_at <= now
                    && now < interval.ends_at
            })
            .map(|interval| interval.ends_at)
            .max();

        return Ok(CoverageDecision {
            state: if paid_now {
                CoverageState::Paid
            } else {
                CoverageState::RenewalRecovery
            },
            active_until: Some(current.ends_at),
            recovery_until: if paid_now { None } else { recovery_until },
            export_until: None,
        });
    }

    let Some(latest_ended) = episodes
        .iter()
        .filter(|episode| episode.ends_at <= now)
        .max_by_key(|episode| episode.ends_at)
        .copied()
    else {
        return Ok(CoverageDecision {
            state: CoverageState::Free,
            active_until: None,
            recovery_until: None,
            export_until: None,
        });
    };

    let export_until = latest_ended
        .ends_at
        .checked_add(EXPORT_WINDOW_SECONDS)
        .ok_or(InvalidCoverage::ExportDeadlineOverflow)?;
    Ok(CoverageDecision {
        state: if now < export_until {
            CoverageState::ExportOnly
        } else {
            CoverageState::Expired
        },
        active_until: None,
        recovery_until: None,
        export_until: Some(export_until),
    })
}

fn eligible_intervals(coverage: &PersonCoverage) -> Result<Vec<EligibleInterval>, InvalidCoverage> {
    let unique = normalise_confirmed_intervals(coverage)?;

    let mut eligible = Vec::with_capacity(unique.len() * 2);
    for interval in unique {
        eligible.push(EligibleInterval {
            starts_at: interval.starts_at,
            ends_at: interval.paid_until,
            kind: IntervalKind::Paid,
            coverage_id: interval.coverage_id.clone(),
        });
        if interval.failed_renewal_id.is_some() {
            eligible.push(EligibleInterval {
                starts_at: interval.paid_until,
                ends_at: interval
                    .paid_until
                    .checked_add(RENEWAL_RECOVERY_SECONDS)
                    .ok_or(InvalidCoverage::RecoveryDeadlineOverflow)?,
                kind: IntervalKind::Recovery,
                coverage_id: interval.coverage_id,
            });
        }
    }
    eligible.sort_by(|left, right| {
        left.starts_at
            .cmp(&right.starts_at)
            .then(left.ends_at.cmp(&right.ends_at))
            .then(left.kind.cmp(&right.kind))
            .then(left.coverage_id.cmp(&right.coverage_id))
    });
    Ok(eligible)
}

/// Validate and deterministically de-duplicate confirmed coverage facts.
///
/// The storage projection uses this same function before writing a snapshot, so the pure
/// evaluator and the durable loader agree on duplicate coverage and renewal identity rules.
pub(crate) fn normalise_confirmed_intervals(
    coverage: &PersonCoverage,
) -> Result<Vec<ConfirmedPaidInterval>, InvalidCoverage> {
    if coverage.beneficiary_id.trim().is_empty() {
        return Err(InvalidCoverage::EmptyBeneficiary);
    }

    let mut unique = HashMap::<String, ConfirmedPaidInterval>::new();
    let mut renewal_identity = HashMap::<String, RenewalIdentity>::new();
    for interval in &coverage.paid_intervals {
        if interval.coverage_id.trim().is_empty() {
            return Err(InvalidCoverage::EmptyCoverageId);
        }
        if interval.source_id.trim().is_empty() {
            return Err(InvalidCoverage::EmptySourceId);
        }
        if interval.starts_at >= interval.paid_until {
            return Err(InvalidCoverage::InvalidInterval);
        }
        if let Some(renewal_id) = interval.failed_renewal_id.as_deref() {
            if renewal_id.trim().is_empty() {
                return Err(InvalidCoverage::EmptyRenewalId);
            }
            let identity = RenewalIdentity {
                coverage_id: interval.coverage_id.clone(),
                source_id: interval.source_id.clone(),
                starts_at: interval.starts_at,
                paid_until: interval.paid_until,
            };
            if let Some(previous) = renewal_identity.get(renewal_id) {
                if previous != &identity {
                    return Err(InvalidCoverage::ConflictingRenewal);
                }
            }
            renewal_identity.insert(renewal_id.to_owned(), identity);
            interval
                .paid_until
                .checked_add(RENEWAL_RECOVERY_SECONDS)
                .ok_or(InvalidCoverage::RecoveryDeadlineOverflow)?;
        }

        if let Some(previous) = unique.get(&interval.coverage_id) {
            if previous != interval {
                return Err(InvalidCoverage::ConflictingCoverage);
            }
        } else {
            unique.insert(interval.coverage_id.clone(), interval.clone());
        }
    }

    let mut intervals: Vec<_> = unique.into_values().collect();
    intervals.sort_by(|left, right| {
        left.coverage_id
            .cmp(&right.coverage_id)
            .then(left.source_id.cmp(&right.source_id))
            .then(left.starts_at.cmp(&right.starts_at))
            .then(left.paid_until.cmp(&right.paid_until))
            .then(left.failed_renewal_id.cmp(&right.failed_renewal_id))
    });
    Ok(intervals)
}

fn episodes(intervals: &[EligibleInterval]) -> Vec<Episode> {
    let mut episodes: Vec<Episode> = Vec::new();
    for interval in intervals {
        match episodes.last_mut() {
            Some(episode) if interval.starts_at <= episode.ends_at => {
                episode.ends_at = episode.ends_at.max(interval.ends_at);
            }
            _ => episodes.push(Episode {
                starts_at: interval.starts_at,
                ends_at: interval.ends_at,
            }),
        }
    }
    episodes
}
