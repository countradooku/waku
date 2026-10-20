//! Exports the `waku-protocol` v8 schema and generates clients from it
//! (RFC 0003 §9).
//!
//! The Rust types in `waku_protocol::v8` are the source of truth. This crate
//! writes their schema to `crates/waku-protocol/schema/v8.json` (committed,
//! so a wire change is a reviewable diff), lints it against the data-model
//! rules (§1.1), and generates the TypeScript client types from that file
//! alone. Swift and Kotlin generators read the same file when they arrive.
//!
//! `bun run protocol:generate` writes everything; `bun run protocol:check`
//! regenerates in memory and fails on any difference.

pub mod lint;
pub mod typescript;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// The schema document, as exported from the Rust types.
pub fn schema_document() -> Value {
    waku_protocol::v8::schema::document()
}

/// `schema/v8.json` as written to disk.
pub fn schema_text(document: &Value) -> String {
    serde_json::to_string_pretty(document).expect("a JSON value serializes") + "\n"
}

fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// `crates/waku-protocol/schema/v8.json`.
pub fn schema_path() -> PathBuf {
    workspace().join("crates/waku-protocol/schema/v8.json")
}

/// Where the generated TypeScript lives.
pub fn typescript_directory() -> PathBuf {
    workspace().join("packages/waku-client/src/v8/generated")
}

/// The v8 sources the lint scans.
pub fn protocol_sources() -> PathBuf {
    workspace().join("crates/waku-protocol/src/v8")
}

/// Every output file, by path, from the current Rust types.
pub fn outputs() -> Result<BTreeMap<PathBuf, String>, String> {
    let document = schema_document();
    let mut problems = lint::lint_schema(&document);
    problems.extend(lint::lint_sources(&protocol_sources()));
    if !problems.is_empty() {
        return Err(format!(
            "the v8 wire types break RFC 0003 §1.1:\n{}",
            problems.join("\n")
        ));
    }
    // The generators read the schema as it is written to disk, so a client
    // generated from the committed file is the same as one generated here.
    let text = schema_text(&document);
    let reparsed: Value = serde_json::from_str(&text).map_err(|error| error.to_string())?;
    let mut outputs = BTreeMap::new();
    outputs.insert(schema_path(), text);
    let directory = typescript_directory();
    for (name, contents) in typescript::generate(&reparsed) {
        outputs.insert(directory.join(name), contents);
    }
    Ok(outputs)
}

/// Files that differ from `outputs`, plus stray files in the generated
/// TypeScript directory.
pub fn stale(outputs: &BTreeMap<PathBuf, String>) -> Vec<String> {
    let mut stale = outputs
        .iter()
        .filter(|(path, contents)| std::fs::read_to_string(path).ok().as_ref() != Some(*contents))
        .map(|(path, _)| path.display().to_string())
        .collect::<Vec<_>>();
    if let Ok(entries) = std::fs::read_dir(typescript_directory()) {
        for entry in entries.flatten() {
            if !outputs.contains_key(&entry.path()) {
                stale.push(format!("{} (not generated)", entry.path().display()));
            }
        }
    }
    stale
}
