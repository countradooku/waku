//! `state.sqlite`: connections, pragmas and the migration ledger (RFC 0001
//! "Storage").

pub mod outbox;
pub mod receipts;
pub mod sink;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{RecvTimeoutError, Sender, bounded};

use parking_lot::{Mutex, MutexGuard};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use waku_protocol::orchestration::{Sequence, Timestamp};

mod embedded {
    include!(concat!(env!("OUT_DIR"), "/migrations.rs"));
}

pub use embedded::MIGRATIONS;

/// Bumped in code when `apply_sql` changes meaning; a lower stored version
/// rebuilds the projection from the log at startup.
pub const GRAPH_SCHEMA_VERSION: i64 = 1;
pub const GRAPH_PROJECTION: &str = "graph";

/// Every storage failure of the orchestrator.
#[derive(Debug)]
pub enum StoreError {
    /// A `WriteGuard` did not hold; the whole decision was dropped.
    GuardFailed,
    Sqlite(rusqlite::Error),
    Encode(serde_json::Error),
    /// A stored row does not decode.
    Corrupt(String),
    /// A migration problem: an edited migration or a downgrade.
    Migration(String),
    /// A test-only fault point fired (crash injection).
    #[cfg(test)]
    Injected(sink::FaultPoint),
}

/// RFC 0001 calls the sink's error `SinkError`.
pub type SinkError = StoreError;

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GuardFailed => formatter.write_str("write guard failed"),
            Self::Sqlite(error) => write!(formatter, "sqlite: {error}"),
            Self::Encode(error) => write!(formatter, "encode: {error}"),
            Self::Corrupt(detail) => write!(formatter, "corrupt row: {detail}"),
            Self::Migration(detail) => write!(formatter, "migration: {detail}"),
            #[cfg(test)]
            Self::Injected(point) => write!(formatter, "injected fault at {point:?}"),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Encode(error)
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

/// Decodes a JSON column, reporting which row failed.
pub(crate) fn decode<T: serde::de::DeserializeOwned>(what: &str, json: &str) -> StoreResult<T> {
    serde_json::from_str(json).map_err(|error| StoreError::Corrupt(format!("{what}: {error}")))
}

/// One writer connection behind a mutex, a pool of read-only connections and
/// a background WAL checkpointer.
pub struct Store {
    path: PathBuf,
    /// Identifies this event log (RFC 0003 §4 `StreamCursor::sequence`):
    /// random when the file is created, so a cursor of another (or a
    /// recreated) database never resumes here.
    store_id: uuid::Uuid,
    writer: Mutex<Connection>,
    readers: ReadPool,
    checkpointer: Checkpointer,
}

/// Runs WAL checkpoints off the commit path. The writer has
/// `wal_autocheckpoint = 0`, so a commit never pays for a checkpoint (with
/// `checkpoint_fullfsync` on macOS one costs several milliseconds); this
/// thread runs a `PASSIVE` checkpoint at most every
/// [`CHECKPOINT_INTERVAL`] while commits arrive, and a `TRUNCATE` one on
/// close.
struct Checkpointer {
    wake: Option<Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

/// The shortest gap between two background checkpoints.
pub const CHECKPOINT_INTERVAL: Duration = Duration::from_millis(100);

impl Checkpointer {
    fn start(path: &Path) -> StoreResult<Self> {
        let connection = Connection::open(path)?;
        configure(&connection)?;
        let (wake, signal) = bounded::<()>(1);
        let handle = std::thread::Builder::new()
            .name("waku-checkpoint".to_owned())
            .spawn(move || {
                let disconnected = |result| matches!(result, Err(RecvTimeoutError::Disconnected));
                'run: while !disconnected(signal.recv_timeout(Duration::from_secs(1))) {
                    let _ = connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()));
                    // Coalesce commits for an interval, but stop at once.
                    let until = Instant::now() + CHECKPOINT_INTERVAL;
                    loop {
                        match signal.recv_timeout(until.saturating_duration_since(Instant::now())) {
                            Ok(()) => {}
                            Err(RecvTimeoutError::Timeout) => break,
                            Err(RecvTimeoutError::Disconnected) => break 'run,
                        }
                    }
                }
                let _ = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
            })
            .map_err(|error| StoreError::Migration(format!("checkpointer thread: {error}")))?;
        Ok(Self {
            wake: Some(wake),
            handle: Some(handle),
        })
    }

    fn note_commit(&self) {
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(());
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        drop(self.wake.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl fmt::Debug for Store {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Store")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl Store {
    /// Opens (or creates) the database, applies pragmas and pending
    /// migrations, then opens `read_connections` read-only connections.
    pub fn open(path: &Path, read_connections: usize) -> StoreResult<Self> {
        let mut writer = Connection::open(path)?;
        configure(&writer)?;
        // Only takes effect before the first table exists.
        set_pragma(&writer, "auto_vacuum", "INCREMENTAL")?;
        set_pragma(&writer, "journal_mode", "WAL")?;
        apply_migrations(&mut writer)?;
        let store_id = ensure_store_id(&writer)?;
        set_pragma(&writer, "wal_autocheckpoint", "0")?;
        let readers = (0..read_connections.max(1))
            .map(|_| open_reader(path))
            .collect::<StoreResult<Vec<_>>>()?;
        Ok(Self {
            path: path.to_owned(),
            store_id,
            writer: Mutex::new(writer),
            readers: ReadPool {
                connections: readers.into_iter().map(Mutex::new).collect(),
                next: AtomicUsize::new(0),
            },
            checkpointer: Checkpointer::start(path)?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The identity of this event log; stream cursors carry it.
    pub fn store_id(&self) -> uuid::Uuid {
        self.store_id
    }

    /// Tells the checkpointer the WAL grew.
    pub(crate) fn note_commit(&self) {
        self.checkpointer.note_commit();
    }

    /// The single writer. Sink commits, outbox claims and settles all go
    /// through it, so they never contend with `SQLITE_BUSY`.
    pub fn writer(&self) -> MutexGuard<'_, Connection> {
        self.writer.lock()
    }

    pub fn reader(&self) -> &ReadPool {
        &self.readers
    }
}

/// Read-only connections for streams, history, search and receipt lookups.
pub struct ReadPool {
    connections: Vec<Mutex<Connection>>,
    next: AtomicUsize,
}

impl ReadPool {
    /// Runs `read` on an idle connection, or waits for the next one in turn.
    pub fn with<R>(&self, read: impl FnOnce(&Connection) -> R) -> R {
        for connection in &self.connections {
            if let Some(connection) = connection.try_lock() {
                return read(&connection);
            }
        }
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        read(&self.connections[index].lock())
    }
}

/// Reads the store identity, creating it on first open. Like the migration
/// ledger it lives outside the numbered migrations: it describes the file,
/// not the schema.
fn ensure_store_id(connection: &Connection) -> StoreResult<uuid::Uuid> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS store_identity (
           singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
           store_id   TEXT    NOT NULL,
           created_at INTEGER NOT NULL
         ) STRICT;",
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO store_identity (singleton, store_id, created_at) VALUES (1, ?1, ?2)",
        rusqlite::params![uuid::Uuid::new_v4().to_string(), now_millis()],
    )?;
    let text: String =
        connection.query_row("SELECT store_id FROM store_identity", [], |row| row.get(0))?;
    uuid::Uuid::parse_str(&text)
        .map_err(|error| StoreError::Corrupt(format!("store_identity: {error}")))
}

fn configure(connection: &Connection) -> StoreResult<()> {
    connection.busy_timeout(Duration::from_millis(5_000))?;
    set_pragma(connection, "synchronous", "NORMAL")?;
    set_pragma(connection, "foreign_keys", "ON")?;
    set_pragma(connection, "journal_size_limit", "67108864")?;
    #[cfg(target_os = "macos")]
    set_pragma(connection, "checkpoint_fullfsync", "ON")?;
    Ok(())
}

fn open_reader(path: &Path) -> StoreResult<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    connection.busy_timeout(Duration::from_millis(5_000))?;
    set_pragma(&connection, "foreign_keys", "ON")?;
    Ok(connection)
}

/// `PRAGMA name = value`, ignoring the row some pragmas return.
fn set_pragma(connection: &Connection, name: &str, value: &str) -> StoreResult<()> {
    connection
        .query_row(&format!("PRAGMA {name} = {value}"), [], |_| Ok(()))
        .optional()?;
    Ok(())
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Applies pending migrations, each in its own transaction with its ledger
/// row. Refuses an edited migration or a ledger version this binary does not
/// know (a downgrade).
pub(crate) fn apply_migrations(connection: &mut Connection) -> StoreResult<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS orchestrator_migrations (
           version    INTEGER PRIMARY KEY,
           name       TEXT    NOT NULL,
           checksum   TEXT    NOT NULL,
           applied_at INTEGER NOT NULL
         ) STRICT;",
    )?;
    let applied = {
        let mut statement = connection.prepare(
            "SELECT version, name, checksum FROM orchestrator_migrations ORDER BY version",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (version, name, checksum) in &applied {
        let Some((_, _, sql)) = MIGRATIONS.iter().find(|(known, ..)| known == version) else {
            return Err(StoreError::Migration(format!(
                "state.sqlite has migration {version:04}_{name}.sql, which this build does not \
                 know; refusing to run an older binary against a newer database"
            )));
        };
        if sha256_hex(sql) != *checksum {
            return Err(StoreError::Migration(format!(
                "migration {version:04}_{name}.sql differs from the one applied to state.sqlite \
                 (checksum mismatch); migrations must never be edited after release"
            )));
        }
    }
    let latest = applied.last().map_or(0, |(version, ..)| *version);
    for (version, name, sql) in MIGRATIONS.iter().filter(|(version, ..)| *version > latest) {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(sql).map_err(|error| {
            StoreError::Migration(format!("applying {version:04}_{name}.sql: {error}"))
        })?;
        transaction.execute(
            "INSERT INTO orchestrator_migrations (version, name, checksum, applied_at)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![version, name, sha256_hex(sql), now_millis()],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

fn now_millis() -> Timestamp {
    use crate::clock::{Clock, SystemClock};
    SystemClock.now()
}

/// The highest committed sequence, 0 before the first event.
pub fn max_sequence(connection: &Connection) -> StoreResult<Sequence> {
    Ok(
        connection.query_row("SELECT COALESCE(MAX(sequence), 0) FROM events", [], |row| {
            row.get::<_, i64>(0)
        })? as Sequence,
    )
}

#[cfg(test)]
pub(crate) mod faults;
