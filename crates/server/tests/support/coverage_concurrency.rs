use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout, Duration, Instant};

/// The race tests deliberately use a finite budget. A blocked backend must never
/// leave an integration test waiting indefinitely when a scenario fails.
pub const RACE_TIMEOUT: Duration = Duration::from_secs(10);

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
/// exceeded. Callers can invoke `abort_and_join` from failure teardown.
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
    use super::abort_and_join;

    #[tokio::test]
    async fn abort_and_join_drains_an_owned_task() {
        let mut task = Some(tokio::spawn(async {
            std::future::pending::<()>().await;
        }));
        abort_and_join(&mut task, "pending test task").await;
        assert!(task.is_none());
    }
}
