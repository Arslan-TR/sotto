use std::future::Future;

use futures_util::FutureExt;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{sleep, timeout, Duration, Instant};

/// The race tests deliberately use a finite budget. A blocked backend must never
/// leave an integration test waiting indefinitely when a scenario fails.
pub const RACE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct OwnedTask<T> {
    receiver: oneshot::Receiver<T>,
}

pub struct RaceTaskOwner {
    handles: Vec<JoinHandle<()>>,
}

impl RaceTaskOwner {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn spawn<T, F>(&mut self, future: F) -> OwnedTask<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        self.handles.push(tokio::spawn(async move {
            let output = future.await;
            let _ = sender.send(output);
        }));
        OwnedTask { receiver }
    }

    pub async fn abort_and_join(&mut self) -> Result<(), String> {
        for handle in &self.handles {
            handle.abort();
        }
        let mut failures = Vec::new();
        for handle in self.handles.drain(..) {
            match timeout(RACE_TIMEOUT, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(_) => failures.push("timed out joining aborted owned race task".into()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    pub async fn join_all(&mut self) -> Result<(), String> {
        let mut failures = Vec::new();
        for handle in self.handles.drain(..) {
            match timeout(RACE_TIMEOUT, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(_) => failures.push("timed out joining owned race task".into()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}

impl Drop for RaceTaskOwner {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub async fn receive_owned<T>(
    task: &mut Option<OwnedTask<T>>,
    label: &'static str,
) -> Result<T, String> {
    let task = task
        .take()
        .ok_or_else(|| format!("{label} task was already consumed"))?;
    timeout(RACE_TIMEOUT, task.receiver)
        .await
        .map_err(|_| format!("timed out waiting for {label}"))?
        .map_err(|_| format!("{label} task exited before reporting its result"))
}

pub async fn run_with_teardown<T, F, C, CF>(
    owner: &mut RaceTaskOwner,
    scenario: F,
    cleanup: C,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
    C: FnOnce() -> CF,
    CF: Future<Output = Result<(), String>>,
{
    let outcome = std::panic::AssertUnwindSafe(scenario).catch_unwind().await;
    let scenario_result = match outcome {
        Ok(result) => result,
        Err(_) => Err("scenario panicked".into()),
    };
    let task_result = if scenario_result.is_ok() {
        owner.join_all().await
    } else {
        owner.abort_and_join().await
    };
    let cleanup_result = cleanup().await;
    match (scenario_result, task_result, cleanup_result) {
        (Ok(value), Ok(()), Ok(())) => Ok(value),
        (scenario, tasks, cleanup) => Err(format_failure(scenario, tasks, cleanup)),
    }
}

fn format_failure<T>(
    scenario: Result<T, String>,
    tasks: Result<(), String>,
    cleanup: Result<(), String>,
) -> String {
    let mut failures = Vec::new();
    if let Err(error) = scenario {
        failures.push(format!("scenario: {error}"));
    }
    if let Err(error) = tasks {
        failures.push(format!("tasks: {error}"));
    }
    if let Err(error) = cleanup {
        failures.push(format!("cleanup: {error}"));
    }
    failures.join("; ")
}

/// Owns cancellation for every task in a race scenario. The guard is installed
/// before readiness or lock observation starts, so an early panic cancels
/// siblings instead of detaching them during unwinding. Successful scenarios
/// still drain their typed handles with `join_with_timeout`.
pub struct RaceTaskGuard {
    handles: Vec<AbortHandle>,
}

impl RaceTaskGuard {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn watch<T>(&mut self, handle: &JoinHandle<T>) {
        self.handles.push(handle.abort_handle());
    }
}

impl Drop for RaceTaskGuard {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub async fn transaction_pid(tx: &mut Transaction<'_, Postgres>) -> i32 {
    timeout(
        RACE_TIMEOUT,
        sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(&mut **tx),
    )
    .await
    .unwrap_or_else(|_| panic!("timed out reading transaction backend pid"))
    .unwrap_or_else(|error| panic!("failed to read transaction backend pid: {error}"))
}

pub async fn wait_for_specific_block(pool: &PgPool, waiter_pid: i32, holder_pid: i32) {
    let deadline = Instant::now() + RACE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for backend {waiter_pid} to block on {holder_pid}");
        }
        let blocked = timeout(
            remaining,
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE pid = $1 AND $2 = ANY(pg_blocking_pids(pid))
                 )",
            )
            .bind(waiter_pid)
            .bind(holder_pid)
            .fetch_one(pool),
        )
        .await
        .unwrap_or_else(|_| panic!("timed out inspecting transaction blocking"))
        .unwrap_or_else(|error| panic!("failed to inspect transaction blocking: {error}"));
        if blocked {
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
}

pub async fn receive_pid(receiver: oneshot::Receiver<i32>, label: &'static str) -> i32 {
    timeout(RACE_TIMEOUT, receiver)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"))
        .unwrap_or_else(|_| panic!("{label} task exited before reporting its backend pid"))
}

/// Wait for a spawned race task without losing ownership when the deadline is
/// exceeded. The surrounding `RaceTaskGuard` cancels sibling tasks if this
/// operation or a later assertion unwinds the scenario.
pub async fn join_with_timeout<T>(
    handle: &mut Option<tokio::task::JoinHandle<T>>,
    label: &'static str,
) -> T {
    let task = handle
        .as_mut()
        .unwrap_or_else(|| panic!("{label} task was already consumed"));
    match timeout(RACE_TIMEOUT, task).await {
        Ok(result) => {
            let _ = handle.take();
            result.unwrap_or_else(|error| panic!("{label} task failed: {error}"))
        }
        Err(_) => {
            let task = handle
                .take()
                .expect("task handle remains present after timeout");
            task.abort();
            match timeout(RACE_TIMEOUT, task).await {
                Ok(Ok(_)) | Ok(Err(_)) => {}
                Err(_) => panic!("timed out joining aborted {label} task"),
            }
            panic!("timed out waiting for {label} task")
        }
    }
}

#[allow(dead_code)]
pub async fn abort_and_join<T>(
    handle: &mut Option<tokio::task::JoinHandle<T>>,
    label: &'static str,
) {
    let Some(handle) = handle.take() else {
        return;
    };
    handle.abort();
    match timeout(RACE_TIMEOUT, handle).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) if error.is_cancelled() => {}
        Ok(Err(error)) => panic!("{label} task failed while being aborted: {error}"),
        Err(_) => panic!("timed out joining aborted {label} task"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        abort_and_join, receive_owned, run_with_teardown, RaceTaskGuard, RaceTaskOwner,
        RACE_TIMEOUT,
    };

    #[tokio::test]
    async fn abort_and_join_drains_an_owned_task() {
        let mut task = Some(tokio::spawn(async {
            std::future::pending::<()>().await;
        }));
        abort_and_join(&mut task, "pending test task").await;
        assert!(task.is_none());
    }

    #[tokio::test]
    async fn task_guard_aborts_siblings_on_unwind() {
        let mut task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        {
            let mut guard = RaceTaskGuard::new();
            guard.watch(&task);
        }
        let result = tokio::time::timeout(RACE_TIMEOUT, &mut task).await;
        assert!(matches!(result, Ok(Err(error)) if error.is_cancelled()));
    }

    #[tokio::test]
    async fn owner_drains_a_panicking_child_and_a_parked_sibling() {
        let mut owner = RaceTaskOwner::new();
        let mut failed = Some(owner.spawn(async {
            panic!("intentional child failure");
        }));
        let mut sibling = Some(owner.spawn(async {
            std::future::pending::<()>().await;
        }));
        let failed_result = receive_owned(&mut failed, "failed child").await;
        assert!(failed_result.is_err());
        let teardown = owner.abort_and_join().await;
        assert!(teardown.is_err());
        assert!(receive_owned(&mut sibling, "parked sibling").await.is_err());
    }

    #[tokio::test]
    async fn scenario_teardown_reports_panic_and_cleanup_failure_together() {
        let mut owner = RaceTaskOwner::new();
        let result = run_with_teardown(
            &mut owner,
            async {
                panic!("intentional scenario failure");
                #[allow(unreachable_code)]
                Ok::<(), String>(())
            },
            || async { Err::<(), _>("intentional cleanup failure".into()) },
        )
        .await;
        assert_eq!(
            result,
            Err("scenario: scenario panicked; cleanup: intentional cleanup failure".into())
        );
    }
}
