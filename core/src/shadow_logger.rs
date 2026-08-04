use crate::types::ExecutionSignal;
use crossbeam::channel::Receiver;
use rusqlite::{params, Connection};
use std::{
    fs::{self, OpenOptions},
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const SHADOW_CANDIDATE_QUEUE_CAPACITY: usize = 1_024;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const INSERT_CANDIDATE_SQL: &str = "
    INSERT INTO shadow_trade_candidates (
        run_started_at_ms_raw,
        run_process_id,
        target_mint,
        source_pool_id,
        observed_at_ms_raw,
        vwap_quote_sum_raw,
        vwap_base_sum_raw,
        persisted_at_ms_raw
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
    ON CONFLICT (
        run_started_at_ms_raw,
        run_process_id,
        target_mint,
        source_pool_id,
        observed_at_ms_raw
    ) DO NOTHING";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShadowRunId {
    pub started_at_ms: u64,
    pub process_id: u32,
}

impl ShadowRunId {
    pub fn generate() -> Result<Self, String> {
        Ok(Self {
            started_at_ms: now_ms()?,
            process_id: std::process::id(),
        })
    }
}

#[derive(Debug, Default)]
pub struct ShadowCandidateMetrics {
    pub candidates_enqueued: AtomicU64,
    pub candidate_queue_drops: AtomicU64,
    pub candidates_persisted: AtomicU64,
    pub duplicate_candidates: AtomicU64,
    pub consumer_started: AtomicBool,
    pub writer_started: AtomicBool,
    pub writer_channel_closed: AtomicBool,
    pub wal_checkpoint_completed: AtomicBool,
    pub shadow_db_healthy: AtomicBool,
}

pub struct ShadowCandidateWriter {
    connection: Connection,
    database_path: PathBuf,
    run_id: ShadowRunId,
    metrics: Arc<ShadowCandidateMetrics>,
}

impl ShadowCandidateWriter {
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn run(mut self, candidate_rx: Receiver<ExecutionSignal>) -> Result<(), String> {
        self.metrics.writer_started.store(true, Ordering::Release);

        for signal in candidate_rx {
            if let Err(error) = self.persist_candidate(&signal) {
                self.metrics
                    .shadow_db_healthy
                    .store(false, Ordering::Release);
                return Err(error);
            }
        }

        self.metrics
            .writer_channel_closed
            .store(true, Ordering::Release);
        if let Err(error) = self.checkpoint_wal() {
            self.metrics
                .shadow_db_healthy
                .store(false, Ordering::Release);
            return Err(error);
        }
        self.metrics
            .wal_checkpoint_completed
            .store(true, Ordering::Release);
        Ok(())
    }

    fn persist_candidate(&mut self, signal: &ExecutionSignal) -> Result<(), String> {
        let persisted_at_ms = now_ms()?;
        let inserted = self
            .connection
            .execute(
                INSERT_CANDIDATE_SQL,
                params![
                    self.run_id.started_at_ms.to_be_bytes().as_slice(),
                    i64::from(self.run_id.process_id),
                    signal.target_mint,
                    signal.source_pool_id,
                    signal.observed_at_ms.to_be_bytes().as_slice(),
                    signal.vwap_baseline.quote_sum.to_be_bytes().as_slice(),
                    signal.vwap_baseline.base_sum.to_be_bytes().as_slice(),
                    persisted_at_ms.to_be_bytes().as_slice(),
                ],
            )
            .map_err(|error| format!("failed to persist shadow candidate: {error}"))?;

        if inserted == 1 {
            self.metrics
                .candidates_persisted
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .duplicate_candidates
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn checkpoint_wal(&mut self) -> Result<(), String> {
        let (busy, _log_frames, _checkpointed_frames): (i64, i64, i64) = self
            .connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|error| format!("shadow WAL checkpoint failed: {error}"))?;
        if busy != 0 {
            self.metrics
                .shadow_db_healthy
                .store(false, Ordering::Release);
            return Err(format!(
                "shadow WAL checkpoint remained busy with status {busy}"
            ));
        }
        Ok(())
    }
}

pub fn initialize_shadow_candidate_writer(
    configured_path: &Path,
    run_id: ShadowRunId,
    metrics: Arc<ShadowCandidateMetrics>,
) -> Result<ShadowCandidateWriter, String> {
    reserve_and_initialize_with(
        configured_path,
        move |resolved_path| {
            let connection = Connection::open(resolved_path).map_err(|error| {
                format!(
                    "failed to open shadow telemetry database {}: {error}",
                    resolved_path.display()
                )
            })?;
            initialize_connection(connection, resolved_path, run_id, metrics)
        },
        |reserved_path| fs::remove_file(reserved_path),
    )
}

fn initialize_connection(
    connection: Connection,
    database_path: &Path,
    run_id: ShadowRunId,
    metrics: Arc<ShadowCandidateMetrics>,
) -> Result<ShadowCandidateWriter, String> {
    initialize_connection_with_hook(connection, database_path, run_id, metrics, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InitializationStage {
    PragmaVerification,
    SchemaCreation,
    WriterInitialization,
}

fn initialize_connection_with_hook<Hook>(
    connection: Connection,
    database_path: &Path,
    run_id: ShadowRunId,
    metrics: Arc<ShadowCandidateMetrics>,
    hook: Hook,
) -> Result<ShadowCandidateWriter, String>
where
    Hook: Fn(InitializationStage) -> Result<(), String>,
{
    hook(InitializationStage::PragmaVerification)?;
    connection
        .busy_timeout(SQLITE_BUSY_TIMEOUT)
        .map_err(|error| format!("failed to set shadow SQLite busy timeout: {error}"))?;

    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(|error| format!("failed to enable shadow SQLite WAL mode: {error}"))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(format!(
            "shadow SQLite journal mode verification failed: expected WAL, got {journal_mode}"
        ));
    }

    connection
        .execute_batch("PRAGMA synchronous=FULL;")
        .map_err(|error| format!("failed to set shadow SQLite synchronous=FULL: {error}"))?;
    let synchronous: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(|error| format!("failed to verify shadow SQLite synchronous mode: {error}"))?;
    if synchronous != 2 {
        return Err(format!(
            "shadow SQLite synchronous verification failed: expected 2 (FULL), got {synchronous}"
        ));
    }

    hook(InitializationStage::SchemaCreation)?;
    connection
        .execute_batch(
            "
            CREATE TABLE shadow_trade_candidates (
                id INTEGER PRIMARY KEY,
                run_started_at_ms_raw BLOB NOT NULL
                    CHECK(length(run_started_at_ms_raw) = 8),
                run_process_id INTEGER NOT NULL
                    CHECK(run_process_id >= 0),
                target_mint TEXT NOT NULL,
                source_pool_id TEXT NOT NULL,
                observed_at_ms_raw BLOB NOT NULL
                    CHECK(length(observed_at_ms_raw) = 8),
                vwap_quote_sum_raw BLOB NOT NULL
                    CHECK(length(vwap_quote_sum_raw) = 16),
                vwap_base_sum_raw BLOB NOT NULL
                    CHECK(length(vwap_base_sum_raw) = 16),
                persisted_at_ms_raw BLOB NOT NULL
                    CHECK(length(persisted_at_ms_raw) = 8),
                UNIQUE (
                    run_started_at_ms_raw,
                    run_process_id,
                    target_mint,
                    source_pool_id,
                    observed_at_ms_raw
                )
            );
            ",
        )
        .map_err(|error| format!("failed to create shadow candidate schema: {error}"))?;

    hook(InitializationStage::WriterInitialization)?;
    connection
        .prepare(INSERT_CANDIDATE_SQL)
        .map_err(|error| format!("failed to initialize shadow candidate insertion: {error}"))?;

    metrics.shadow_db_healthy.store(true, Ordering::Release);
    Ok(ShadowCandidateWriter {
        connection,
        database_path: database_path.to_path_buf(),
        run_id,
        metrics,
    })
}

fn reserve_and_initialize_with<Initialize, Cleanup>(
    configured_path: &Path,
    initialize: Initialize,
    cleanup: Cleanup,
) -> Result<ShadowCandidateWriter, String>
where
    Initialize: FnOnce(&Path) -> Result<ShadowCandidateWriter, String>,
    Cleanup: FnOnce(&Path) -> std::io::Result<()>,
{
    let resolved_target = validate_shadow_database_path(configured_path)?;
    let reservation = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&resolved_target)
        .map_err(|error| {
            format!(
                "failed to reserve shadow telemetry database {}: {error}",
                resolved_target.display()
            )
        })?;

    // SQLite must never receive a path while the create_new reservation handle
    // is still open. This is required for deterministic Windows behavior.
    drop(reservation);

    match initialize(&resolved_target) {
        Ok(writer) => Ok(writer),
        Err(initialization_error) => {
            if let Err(cleanup_error) = cleanup(&resolved_target) {
                log::error!(
                    "failed to clean reserved shadow telemetry database {} after initialization \
                     error: {cleanup_error}",
                    resolved_target.display()
                );
            }
            Err(initialization_error)
        }
    }
}

fn validate_shadow_database_path(configured_path: &Path) -> Result<PathBuf, String> {
    if !configured_path.is_absolute() {
        return Err("SHADOW_TELEMETRY_DB_PATH must be absolute".to_string());
    }
    if configured_path.exists() {
        return Err(format!(
            "SHADOW_TELEMETRY_DB_PATH must not exist: {}",
            configured_path.display()
        ));
    }

    let filename = configured_path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "SHADOW_TELEMETRY_DB_PATH must include a filename".to_string())?;
    if configured_path
        .components()
        .next_back()
        .is_some_and(|component| !matches!(component, Component::Normal(_)))
    {
        return Err("SHADOW_TELEMETRY_DB_PATH must end in a normal filename".to_string());
    }

    let parent = configured_path
        .parent()
        .ok_or_else(|| "SHADOW_TELEMETRY_DB_PATH must have a parent directory".to_string())?;
    let canonical_parent = parent.canonicalize().map_err(|error| {
        format!("SHADOW_TELEMETRY_DB_PATH parent must exist and be canonicalizable: {error}")
    })?;
    let resolved_target = canonical_parent.join(filename);
    if resolved_target.exists() {
        return Err(format!(
            "resolved SHADOW_TELEMETRY_DB_PATH must not exist: {}",
            resolved_target.display()
        ));
    }

    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .map_err(|error| format!("failed to canonicalize rust_daemon directory: {error}"))?;
    let repository_root = manifest_dir
        .parent()
        .ok_or_else(|| "rust_daemon directory has no repository parent".to_string())?
        .canonicalize()
        .map_err(|error| format!("failed to canonicalize repository root: {error}"))?;
    let tracked_database = manifest_dir
        .join("trade_telemetry.db")
        .canonicalize()
        .map_err(|error| format!("failed to canonicalize tracked trade_telemetry.db: {error}"))?;

    if paths_equal(&resolved_target, &tracked_database) {
        return Err(
            "shadow telemetry database must not be the tracked trade_telemetry.db".to_string(),
        );
    }
    if path_is_within(&resolved_target, &repository_root) {
        return Err("shadow telemetry database must be external to the repository".to_string());
    }

    Ok(resolved_target)
}

#[cfg(windows)]
fn path_component_equal(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left.to_string_lossy()
        .eq_ignore_ascii_case(&right.to_string_lossy())
}

#[cfg(not(windows))]
fn path_component_equal(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    left == right
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    let left_components: Vec<_> = left.components().collect();
    let right_components: Vec<_> = right.components().collect();
    left_components.len() == right_components.len()
        && left_components
            .iter()
            .zip(right_components.iter())
            .all(|(left, right)| path_component_equal(left.as_os_str(), right.as_os_str()))
}

fn path_is_within(candidate: &Path, parent: &Path) -> bool {
    let candidate_components: Vec<_> = candidate.components().collect();
    let parent_components: Vec<_> = parent.components().collect();
    candidate_components.len() >= parent_components.len()
        && candidate_components
            .iter()
            .zip(parent_components.iter())
            .all(|(candidate, parent)| {
                path_component_equal(candidate.as_os_str(), parent.as_os_str())
            })
}

fn now_ms() -> Result<u64, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock is before UNIX epoch: {error}"))?
        .as_millis();
    u64::try_from(millis).map_err(|error| format!("millisecond timestamp exceeds u64: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    static TEST_DIRECTORY_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    fn external_test_directory(label: &str) -> PathBuf {
        let sequence = TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "alphanexus-shadow-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&directory).expect("create isolated test directory");
        directory
    }

    fn test_run_id() -> ShadowRunId {
        ShadowRunId {
            started_at_ms: 1,
            process_id: 2,
        }
    }

    fn real_initializer(path: &Path) -> Result<ShadowCandidateWriter, String> {
        initialize_connection(
            Connection::open(path).map_err(|error| error.to_string())?,
            path,
            test_run_id(),
            Arc::new(ShadowCandidateMetrics::default()),
        )
    }

    #[test]
    fn reservation_handle_is_closed_before_sqlite_opens() {
        let directory = external_test_directory("closed-handle");
        let path = directory.join("shadow.db");
        let writer = reserve_and_initialize_with(
            &path,
            |reserved_path| {
                fs::remove_file(reserved_path)
                    .map_err(|error| format!("reservation handle remained open: {error}"))?;
                let probe = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(reserved_path)
                    .map_err(|error| format!("failed to recreate reservation probe: {error}"))?;
                drop(probe);
                real_initializer(reserved_path)
            },
            |reserved_path| fs::remove_file(reserved_path),
        )
        .expect("SQLite opens only after reservation handle closes");
        drop(writer);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn initialization_failures_at_every_stage_remove_reserved_file() {
        let stages = [
            ("sqlite-open", None),
            (
                "pragma-verification",
                Some(InitializationStage::PragmaVerification),
            ),
            ("schema-creation", Some(InitializationStage::SchemaCreation)),
            (
                "writer-initialization",
                Some(InitializationStage::WriterInitialization),
            ),
        ];

        for (label, injected_stage) in stages {
            let directory = external_test_directory(label);
            let path = directory.join("shadow.db");
            let expected_error = format!("injected failure at {label}");
            let injected_error = expected_error.clone();

            let error = reserve_and_initialize_with(
                &path,
                move |reserved_path| {
                    let Some(injected_stage) = injected_stage else {
                        return Err(injected_error.clone());
                    };
                    initialize_connection_with_hook(
                        Connection::open(reserved_path)
                            .map_err(|open_error| open_error.to_string())?,
                        reserved_path,
                        test_run_id(),
                        Arc::new(ShadowCandidateMetrics::default()),
                        |current_stage| {
                            if current_stage == injected_stage {
                                Err(injected_error.clone())
                            } else {
                                Ok(())
                            }
                        },
                    )
                },
                |reserved_path| fs::remove_file(reserved_path),
            )
            .err()
            .expect("injected initialization failure must be returned");

            assert_eq!(error, expected_error);
            assert!(
                !path.exists(),
                "reserved primary file remained after {label} failure"
            );
            fs::remove_dir_all(directory).expect("remove test directory");
        }
    }

    #[test]
    fn initialization_failure_removes_reserved_file_and_same_path_retries() {
        let directory = external_test_directory("retry");
        let path = directory.join("shadow.db");
        let resolved_path = directory
            .canonicalize()
            .expect("canonical test directory")
            .join("shadow.db");
        let attempted_paths = Arc::new(Mutex::new(Vec::new()));
        let first_attempts = attempted_paths.clone();

        let error = reserve_and_initialize_with(
            &path,
            move |reserved_path| {
                first_attempts
                    .lock()
                    .expect("attempt lock")
                    .push(reserved_path.to_path_buf());
                initialize_connection_with_hook(
                    Connection::open(reserved_path).map_err(|error| error.to_string())?,
                    reserved_path,
                    test_run_id(),
                    Arc::new(ShadowCandidateMetrics::default()),
                    |stage| {
                        if stage == InitializationStage::WriterInitialization {
                            Err("injected SQLite initialization failure".to_string())
                        } else {
                            Ok(())
                        }
                    },
                )
            },
            |reserved_path| fs::remove_file(reserved_path),
        )
        .err()
        .expect("injected failure must be returned");
        assert_eq!(error, "injected SQLite initialization failure");
        assert!(!path.exists());

        let second_attempts = attempted_paths.clone();
        let writer = reserve_and_initialize_with(
            &path,
            move |reserved_path| {
                second_attempts
                    .lock()
                    .expect("attempt lock")
                    .push(reserved_path.to_path_buf());
                real_initializer(reserved_path)
            },
            |reserved_path| fs::remove_file(reserved_path),
        )
        .expect("identical path succeeds after cleanup");

        let attempts = attempted_paths.lock().expect("attempt lock");
        assert_eq!(
            attempts.as_slice(),
            &[resolved_path.clone(), resolved_path.clone()]
        );
        drop(attempts);
        drop(writer);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn cleanup_failure_preserves_original_error_and_never_falls_back() {
        let directory = external_test_directory("cleanup-error");
        let path = directory.join("shadow.db");
        let resolved_path = directory
            .canonicalize()
            .expect("canonical test directory")
            .join("shadow.db");
        let attempted_paths = Arc::new(Mutex::new(Vec::new()));
        let recorded_attempts = attempted_paths.clone();

        let error = reserve_and_initialize_with(
            &path,
            move |reserved_path| {
                recorded_attempts
                    .lock()
                    .expect("attempt lock")
                    .push(reserved_path.to_path_buf());
                Err("original writer initialization error".to_string())
            },
            |_reserved_path| Err(std::io::Error::other("injected cleanup failure")),
        )
        .err()
        .expect("initialization must fail");

        assert_eq!(error, "original writer initialization error");
        assert_eq!(
            attempted_paths.lock().expect("attempt lock").as_slice(),
            &[resolved_path]
        );
        assert!(path.exists());

        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
