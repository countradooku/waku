//! Embeds `migrations/NNNN_name.sql` as `MIGRATIONS: &[(u32, &str, &str)]`
//! (version, name, SQL), RFC 0001 R5. The numeric prefix is the apply order
//! and starts at 0001; a gap or an empty file fails the build.

use std::path::Path;
use std::{env, fs};

fn main() {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    println!("cargo:rerun-if-changed={}", directory.display());

    let mut migrations = fs::read_dir(&directory)
        .unwrap_or_else(|error| panic!("reading {}: {error}", directory.display()))
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension()? != "sql" {
                return None;
            }
            let stem = path.file_stem()?.to_str()?.to_owned();
            let (prefix, name) = stem
                .split_once('_')
                .unwrap_or_else(|| panic!("migration {stem} is not named NNNN_name.sql"));
            let version: u32 = prefix
                .parse()
                .unwrap_or_else(|_| panic!("migration {stem} has no numeric prefix"));
            Some((version, name.to_owned(), path))
        })
        .collect::<Vec<_>>();
    migrations.sort_by_key(|(version, ..)| *version);

    let mut source = String::from(
        "/// Orchestrator migrations in apply order: (version, name, SQL).\n\
         pub static MIGRATIONS: &[(u32, &str, &str)] = &[\n",
    );
    for (position, (version, name, path)) in migrations.iter().enumerate() {
        let expected = position as u32 + 1;
        assert_eq!(
            *version, expected,
            "migration {version:04}_{name} breaks the sequence; expected prefix {expected:04}"
        );
        let sql = fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
        assert!(
            !sql.trim().is_empty(),
            "migration {version:04}_{name} is empty"
        );
        println!("cargo:rerun-if-changed={}", path.display());
        source.push_str(&format!(
            "    ({version}, {name:?}, include_str!({:?})),\n",
            path.canonicalize().unwrap_or_else(|_| path.clone())
        ));
    }
    source.push_str("];\n");

    let out = Path::new(&env::var("OUT_DIR").expect("OUT_DIR")).join("migrations.rs");
    fs::write(&out, source).unwrap_or_else(|error| panic!("writing {}: {error}", out.display()));
}
