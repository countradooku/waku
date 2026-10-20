//! R5: the orchestrator's own migration ledger.

use rusqlite::Connection;

use crate::store::{MIGRATIONS, Store};

fn ledger(path: &std::path::Path) -> Vec<(u32, String)> {
    let connection = Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT version, name FROM orchestrator_migrations ORDER BY version")
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn migrations_apply_once_and_record_the_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    drop(Store::open(&path, 1).unwrap());
    drop(Store::open(&path, 1).unwrap());
    let expected = MIGRATIONS
        .iter()
        .map(|(version, name, _)| (*version, (*name).to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(ledger(&path), expected);
    assert_eq!(MIGRATIONS[0].0, 1, "migrations start at 0001");
    // STRICT tables reject a mistyped value.
    let connection = Connection::open(&path).unwrap();
    assert!(
        connection
            .execute(
                "INSERT INTO projection_metadata VALUES ('x', 'not a number', 0, 0)",
                [],
            )
            .is_err()
    );
    let auto_vacuum: i64 = connection
        .query_row("PRAGMA auto_vacuum", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        auto_vacuum, 2,
        "incremental auto-vacuum is set before the first table"
    );
    let journal: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal, "wal");
}

#[test]
fn an_edited_migration_refuses_to_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    drop(Store::open(&path, 1).unwrap());
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE orchestrator_migrations SET checksum = 'edited' WHERE version = 1",
            [],
        )
        .unwrap();
    let error = Store::open(&path, 1).unwrap_err().to_string();
    assert!(error.contains("0001_init.sql"), "{error}");
    assert!(error.contains("checksum"), "{error}");
}

#[test]
fn a_newer_database_refuses_an_older_binary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    drop(Store::open(&path, 1).unwrap());
    Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO orchestrator_migrations VALUES (999, 'future', 'x', 0)",
            [],
        )
        .unwrap();
    let error = Store::open(&path, 1).unwrap_err().to_string();
    assert!(error.contains("0999_future.sql"), "{error}");
}
