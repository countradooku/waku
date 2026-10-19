//! Platform build metadata.
//!
//! Every native updater reads its release host and public key from the values
//! exported here. On Windows, Explorer, the taskbar, and the Programs list
//! also read the icon and version block out of the PE image itself.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    export_release_configuration();

    #[cfg(target_os = "windows")]
    {
        // GPUI's Taffy layout and text shaping recurse deeply enough to
        // overflow the 1 MiB the MSVC linker defaults to.
        println!("cargo:rustc-link-arg-bins=/stack:{}", 8 * 1024 * 1024);
        embed_windows_resources();
    }
}

/// Export where this build looks for updates and which key it trusts.
///
/// Both values come from the build environment, so the source tree carries no
/// release endpoint or key (see `docs/fork/RELEASING.md`):
///
/// - `WAKU_RELEASES_URL`: the release host's base URL, for example
///   `https://releases.example.com`. Every feed lives directly under it
///   (`appcast.xml`, `appcast-linux-<arch>.xml`, `appcast-windows-<arch>.xml`).
/// - `WAKU_SPARKLE_PUBLIC_ED_KEY`: the base64 Ed25519 public key that
///   Sparkle's `generate_keys -p` prints. `scripts/bundle.sh` writes the same
///   value into the macOS Info.plist; Linux and Windows verify against this
///   compiled-in copy.
///
/// When either is missing, both are exported empty and `Updater::init` leaves
/// in-app updates disabled.
fn export_release_configuration() {
    const RELEASES_URL: &str = "WAKU_RELEASES_URL";
    const PUBLIC_ED_KEY: &str = "WAKU_SPARKLE_PUBLIC_ED_KEY";

    println!("cargo:rerun-if-env-changed={RELEASES_URL}");
    println!("cargo:rerun-if-env-changed={PUBLIC_ED_KEY}");

    let read = |name: &str| std::env::var(name).unwrap_or_default().trim().to_owned();
    let releases_url = read(RELEASES_URL).trim_end_matches('/').to_owned();
    let public_key = read(PUBLIC_ED_KEY);

    // A value that is set but malformed is a release mistake rather than a
    // choice to build without updates, so it fails the build instead of
    // shipping an updater that can never succeed.
    if !releases_url.is_empty()
        && (!releases_url.starts_with("https://")
            || releases_url.len() <= "https://".len()
            || releases_url.chars().any(char::is_whitespace))
    {
        panic!("{RELEASES_URL} must be an https:// URL without whitespace, got {releases_url:?}");
    }
    if public_key.chars().any(char::is_whitespace) {
        panic!("{PUBLIC_ED_KEY} must be the single base64 line `generate_keys -p` prints");
    }

    let configured = !releases_url.is_empty() && !public_key.is_empty();
    if !configured && (!releases_url.is_empty() || !public_key.is_empty()) {
        println!(
            "cargo:warning=only one of {RELEASES_URL} and {PUBLIC_ED_KEY} is set; \
             in-app updates are disabled in this build"
        );
    }
    let (releases_url, public_key) = if configured {
        (releases_url.as_str(), public_key.as_str())
    } else {
        ("", "")
    };

    println!("cargo:rustc-env={RELEASES_URL}={releases_url}");
    println!("cargo:rustc-env={PUBLIC_ED_KEY}={public_key}");
}

#[cfg(target_os = "windows")]
fn embed_windows_resources() {
    const ICON: &str = "resources/windows/AppIcon.ico";

    println!("cargo:rerun-if-changed={ICON}");

    let icon = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(ICON);
    // The resource compiler reads `.rc` as C source, so a Windows path
    // separator has to survive as a literal backslash.
    let icon = icon.to_string_lossy().replace('\\', "\\\\");

    let package_version = std::env::var("CARGO_PKG_VERSION").unwrap_or_default();
    // VERSIONINFO wants four numeric fields; Waku's version has three.
    let mut fields = package_version
        .split(['.', '-', '+'])
        .map(|field| field.parse::<u16>().unwrap_or(0))
        .chain(std::iter::repeat(0));
    let file_version = format!(
        "{},{},{},{}",
        fields.next().unwrap_or(0),
        fields.next().unwrap_or(0),
        fields.next().unwrap_or(0),
        fields.next().unwrap_or(0),
    );
    let description = std::env::var("CARGO_PKG_DESCRIPTION").unwrap_or_default();

    let resources = format!(
        r#"1 ICON "{icon}"

1 VERSIONINFO
FILEVERSION {file_version}
PRODUCTVERSION {file_version}
FILEFLAGSMASK 0x3fL
FILEFLAGS 0x0L
FILEOS 0x40004L
FILETYPE 0x1L
FILESUBTYPE 0x0L
BEGIN
    BLOCK "StringFileInfo"
    BEGIN
        BLOCK "040904b0"
        BEGIN
            VALUE "CompanyName", "Waku\0"
            VALUE "FileDescription", "{description}\0"
            VALUE "FileVersion", "{package_version}\0"
            VALUE "InternalName", "waku\0"
            VALUE "OriginalFilename", "waku.exe\0"
            VALUE "ProductName", "Waku\0"
            VALUE "ProductVersion", "{package_version}\0"
        END
    END
    BLOCK "VarFileInfo"
    BEGIN
        VALUE "Translation", 0x0409, 1200
    END
END
"#
    );

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    let script = out_dir.join("waku.rc");
    std::fs::write(&script, resources).expect("write the resource script");

    // GPUI embeds the application manifest through its own resource script,
    // so this one only claims the icon and version block.
    embed_resource::compile(&script, embed_resource::NONE)
        .manifest_optional()
        .expect("compile Windows resources");
}
