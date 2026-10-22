//! Makes Cargo track the locale catalog read by rust-i18n's proc macro.

use std::path::Path;

fn main() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    println!(
        "cargo:rerun-if-changed={}",
        repository.join("locales").display()
    );
}
