//! The data-model rules of RFC 0003 §1.1, enforced on the exported schema
//! and on the v8 sources.
//!
//! The schema walk catches what survives into the wire shape: `Value`
//! (an unconstrained schema), `untagged` (an `anyOf` that is not an
//! `Option`), closed string enums, open unions without an `unknown`
//! fallback (or an `x-waku-fallback` variant, or `x-waku-closed` for unions
//! only clients send), and newtype variants of primitives. `flatten` and `HashMap`
//! leave no trace a schema can tell apart from a nested struct or a
//! `BTreeMap`, so the sources of `waku_protocol::v8` are scanned for them
//! (and for the other forbidden attributes) as well.

use std::path::Path;

use serde_json::{Map, Value};

/// Every violation in `document`, as `path: problem` lines.
pub fn lint_schema(document: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    let definitions = document
        .get("$defs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (name, schema) in &definitions {
        walk(
            schema,
            &format!("$defs/{name}"),
            &definitions,
            &mut problems,
        );
    }
    for (section, keys) in [
        ("methods", &["params", "result"][..]),
        ("streams", &["spec", "item"][..]),
    ] {
        for entry in document
            .get(section)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let name = entry.get("name").and_then(Value::as_str).unwrap_or("?");
            for key in keys {
                if let Some(schema) = entry.get(*key) {
                    walk(
                        schema,
                        &format!("{section}/{name}/{key}"),
                        &definitions,
                        &mut problems,
                    );
                }
            }
            if section == "methods"
                && let Some(params) = entry.get("params")
                && !is_object(params, &definitions)
            {
                problems.push(format!(
                    "methods/{name}/params: params must be a struct (a map)"
                ));
            }
        }
    }
    problems
}

fn resolve<'a>(schema: &'a Value, definitions: &'a Map<String, Value>) -> &'a Value {
    match schema
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| reference.strip_prefix("#/$defs/"))
    {
        Some(name) => definitions.get(name).unwrap_or(schema),
        None => schema,
    }
}

fn is_object(schema: &Value, definitions: &Map<String, Value>) -> bool {
    let schema = resolve(schema, definitions);
    schema.get("type").and_then(Value::as_str) == Some("object") || schema.get("oneOf").is_some()
}

fn is_null(schema: &Value) -> bool {
    schema.get("type").and_then(Value::as_str) == Some("null")
}

fn walk(schema: &Value, path: &str, definitions: &Map<String, Value>, problems: &mut Vec<String>) {
    let Some(object) = schema.as_object() else {
        if schema == &Value::Bool(true) {
            problems.push(format!("{path}: unconstrained value (serde_json::Value?)"));
        }
        return;
    };
    if object.contains_key("x-waku-dependent") {
        return;
    }
    let structural = [
        "$ref",
        "type",
        "oneOf",
        "anyOf",
        "allOf",
        "enum",
        "const",
        "properties",
        "items",
        "additionalProperties",
    ];
    if !structural.iter().any(|key| object.contains_key(*key)) {
        problems.push(format!("{path}: unconstrained value (serde_json::Value?)"));
        return;
    }
    if object.contains_key("allOf") {
        problems.push(format!(
            "{path}: allOf (a newtype variant of a primitive or a flattened type)"
        ));
    }
    if let Some(any_of) = object.get("anyOf").and_then(Value::as_array) {
        let non_null = any_of
            .iter()
            .filter(|alternative| !is_null(alternative))
            .count();
        if non_null != 1 || any_of.len() != 2 {
            problems.push(format!(
                "{path}: anyOf that is not an Option (an untagged enum?)"
            ));
        }
        for (index, alternative) in any_of.iter().enumerate() {
            walk(
                alternative,
                &format!("{path}/anyOf/{index}"),
                definitions,
                problems,
            );
        }
    }
    if let Some(variants) = object.get("enum") {
        let open = object.get("x-waku-open") == Some(&Value::Bool(true));
        if !open && variants.is_array() {
            problems.push(format!(
                "{path}: closed string enum; use open_string_enum! so unknown values survive"
            ));
        }
    }
    if let Some(variants) = object.get("oneOf").and_then(Value::as_array) {
        // A union only clients send may be closed: the daemon refuses an
        // unknown tag (`badRequest`) instead of decoding it.
        let closed = object.get("x-waku-closed") == Some(&Value::Bool(true));
        // A union may fall back to one of its own variants instead of
        // `Unknown` (the weakest reading, RFC 0001 amendments).
        let fallback = object.get("x-waku-fallback").and_then(Value::as_str);
        let mut has_unknown = closed;
        for (index, variant) in variants.iter().enumerate() {
            let variant_path = format!("{path}/oneOf/{index}");
            let tag = variant
                .pointer("/properties/type/const")
                .and_then(Value::as_str);
            match tag {
                None => problems.push(format!(
                    "{variant_path}: union variant without a `type` tag (only internally tagged enums are allowed)"
                )),
                Some(tag) if tag.eq_ignore_ascii_case("unknown") => has_unknown = true,
                Some(tag) if Some(tag) == fallback => has_unknown = true,
                Some(_) => {}
            }
            if variant.get("$ref").is_some() && !is_object(variant, definitions) {
                problems.push(format!(
                    "{variant_path}: newtype variant of a non-struct type"
                ));
            }
            walk_object_parts(variant, &variant_path, definitions, problems);
        }
        if !has_unknown {
            problems.push(format!(
                "{path}: open union without an `Unknown` variant (#[serde(other)])"
            ));
        }
    }
    walk_object_parts(schema, path, definitions, problems);
}

/// Properties, array items and map values.
fn walk_object_parts(
    schema: &Value,
    path: &str,
    definitions: &Map<String, Value>,
    problems: &mut Vec<String>,
) {
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, property) in properties {
            walk(property, &format!("{path}/{name}"), definitions, problems);
        }
    }
    if let Some(items) = schema.get("items") {
        walk(items, &format!("{path}/items"), definitions, problems);
    }
    match schema.get("additionalProperties") {
        Some(Value::Bool(true)) => {
            problems.push(format!("{path}: map of unconstrained values"));
        }
        Some(values @ Value::Object(_)) => {
            walk(values, &format!("{path}/values"), definitions, problems);
        }
        _ => {}
    }
}

/// Forbidden serde attributes and types in the v8 sources. Comments and the
/// trailing `#[cfg(test)]` module of each file are not wire types and are
/// skipped, as is the exporter itself.
pub fn lint_sources(directory: &Path) -> Vec<String> {
    const FORBIDDEN: [(&str, &str); 5] = [
        ("untagged", "untagged enums are not allowed on the wire"),
        ("flatten", "flatten is not allowed on the wire"),
        ("HashMap", "use BTreeMap so encoding is deterministic"),
        (
            "serde_json::Value",
            "opaque JSON travels as JsonText(String)",
        ),
        ("deny_unknown_fields", "unknown fields must be ignored"),
    ];
    let mut problems = Vec::new();
    let mut files = Vec::new();
    collect_rust_files(directory, &mut files);
    files.sort();
    for file in files {
        if file.file_name().is_some_and(|name| name == "schema.rs") {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&file) else {
            problems.push(format!("{}: unreadable", file.display()));
            continue;
        };
        for (number, line) in source.lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            if code.trim_start().starts_with("#[cfg(test)]") {
                break;
            }
            for (token, reason) in FORBIDDEN {
                if code.contains(token) {
                    problems.push(format!(
                        "{}:{}: `{token}`: {reason}",
                        file.display(),
                        number + 1
                    ));
                }
            }
        }
    }
    problems
}

fn collect_rust_files(directory: &Path, files: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lint(definitions: Value) -> Vec<String> {
        lint_schema(&json!({ "$defs": definitions, "methods": [], "streams": [] }))
    }

    #[test]
    fn the_exported_schema_follows_the_rules() {
        let problems = lint_schema(&crate::schema_document());
        assert!(problems.is_empty(), "{problems:#?}");
    }

    #[test]
    fn values_untagged_enums_and_closed_enums_are_rejected() {
        let problems = lint(json!({
            "Opaque": true,
            "Loose": { "type": "object", "properties": { "payload": {} } },
            "Either": { "anyOf": [{ "type": "string" }, { "type": "integer" }] },
            "Closed": { "type": "string", "enum": ["a", "b"] },
            "Optional": { "anyOf": [{ "type": "string" }, { "type": "null" }] },
            "Open": { "type": "string", "enum": ["a", "b"], "x-waku-open": true },
            "Map": { "type": "object", "additionalProperties": true },
        }));
        let joined = problems.join("\n");
        assert!(joined.contains("$defs/Opaque: unconstrained"), "{joined}");
        assert!(
            joined.contains("$defs/Loose/payload: unconstrained"),
            "{joined}"
        );
        assert!(joined.contains("$defs/Either: anyOf"), "{joined}");
        assert!(
            joined.contains("$defs/Closed: closed string enum"),
            "{joined}"
        );
        assert!(
            joined.contains("$defs/Map: map of unconstrained"),
            "{joined}"
        );
        assert!(!joined.contains("Optional"), "{joined}");
        assert!(!joined.contains("$defs/Open"), "{joined}");
    }

    #[test]
    fn unions_must_be_tagged_and_open() {
        let tagged = |tag: &str| json!({ "type": "object", "properties": { "type": { "type": "string", "const": tag } } });
        let problems = lint(json!({
            "Closed": { "oneOf": [tagged("a"), tagged("b")] },
            "External": { "oneOf": [{ "type": "object", "properties": { "a": { "type": "string" } } }, tagged("unknown")] },
            "Good": { "oneOf": [tagged("a"), tagged("unknown")] },
            "Newtype": { "oneOf": [
                { "$ref": "#/$defs/Text", "properties": { "type": { "const": "text" } } },
                tagged("unknown"),
            ] },
            "Text": { "type": "string" },
        }));
        let joined = problems.join("\n");
        assert!(
            joined.contains("$defs/Closed: open union without"),
            "{joined}"
        );
        assert!(
            joined.contains("$defs/External/oneOf/0: union variant without"),
            "{joined}"
        );
        assert!(
            joined.contains("$defs/Newtype/oneOf/0: newtype variant"),
            "{joined}"
        );
        assert!(!joined.contains("$defs/Good"), "{joined}");
    }

    #[test]
    fn closed_and_fallback_unions_are_accepted() {
        let tagged = |tag: &str| json!({ "type": "object", "properties": { "type": { "type": "string", "const": tag } } });
        let problems = lint(json!({
            "Command": { "oneOf": [tagged("a"), tagged("b")], "x-waku-closed": true },
            "Source": { "oneOf": [tagged("a"), tagged("fallback")], "x-waku-fallback": "fallback" },
            "Wrong": { "oneOf": [tagged("a")], "x-waku-fallback": "missing" },
        }));
        let joined = problems.join("\n");
        assert!(!joined.contains("$defs/Command"), "{joined}");
        assert!(!joined.contains("$defs/Source"), "{joined}");
        assert!(
            joined.contains("$defs/Wrong: open union without"),
            "{joined}"
        );
    }

    #[test]
    fn sources_with_forbidden_attributes_are_rejected() {
        let directory =
            std::env::temp_dir().join(format!("waku-codegen-lint-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("wire.rs"),
            "// flatten in a comment is fine\n#[serde(flatten)]\npub extra: HashMap<String, u8>,\n#[cfg(test)]\nmod tests { use serde_json::Value; }\n",
        )
        .unwrap();
        let problems = lint_sources(&directory);
        let _ = std::fs::remove_dir_all(&directory);
        assert_eq!(problems.len(), 2, "{problems:#?}");
        assert!(problems[0].ends_with("`flatten`: flatten is not allowed on the wire"));
        assert!(problems[1].contains("`HashMap`"));
    }

    #[test]
    fn the_v8_sources_follow_the_rules() {
        let problems = lint_sources(&crate::protocol_sources());
        assert!(problems.is_empty(), "{problems:#?}");
    }
}
