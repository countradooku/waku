//! `waku-codegen [--check]`: write (or verify) `schema/v8.json` and the
//! generated TypeScript client types.

use std::process::ExitCode;

fn main() -> ExitCode {
    let check = std::env::args().nth(1).as_deref() == Some("--check");
    let outputs = match waku_codegen::outputs() {
        Ok(outputs) => outputs,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    if check {
        let stale = waku_codegen::stale(&outputs);
        if !stale.is_empty() {
            eprintln!(
                "the v8 schema or generated client is stale; run `bun run protocol:generate`:\n{}",
                stale.join("\n")
            );
            return ExitCode::FAILURE;
        }
        println!("v8 schema and TypeScript client are current");
        return ExitCode::SUCCESS;
    }
    let directory = waku_codegen::typescript_directory();
    if directory.exists()
        && let Err(error) = std::fs::remove_dir_all(&directory)
    {
        eprintln!("could not clear {}: {error}", directory.display());
        return ExitCode::FAILURE;
    }
    for (path, contents) in &outputs {
        if let Some(parent) = path.parent()
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            eprintln!("could not create {}: {error}", parent.display());
            return ExitCode::FAILURE;
        }
        if let Err(error) = std::fs::write(path, contents) {
            eprintln!("could not write {}: {error}", path.display());
            return ExitCode::FAILURE;
        }
    }
    println!("generated the v8 schema and TypeScript client");
    ExitCode::SUCCESS
}
