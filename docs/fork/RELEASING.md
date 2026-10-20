# Releasing the fork

The fork ships with **no release host and no update key**. Upstream's feeds
(`releases.waku.sh`), its R2 bucket and its Sparkle public key are gone from
the tree, so no fork build can update itself back to upstream, and nothing
signed with upstream's key is trusted.

Everything release-related is configured at build time by two values:

| Build env | Example | Used by |
| --- | --- | --- |
| `WAKU_RELEASES_URL` | `https://releases.example.com` | `build.rs` compiles it in; `scripts/bundle.sh` writes `SUFeedURL`; the release scripts and workflows derive every URL from it |
| `WAKU_SPARKLE_PUBLIC_ED_KEY` | the line `generate_keys -p` prints | `build.rs` compiles it in; `scripts/bundle.sh` writes `SUPublicEDKey`; the appcast scripts refuse a private key that does not match it |

If either one is missing, the build has **in-app updates disabled**: the
updater never starts, Sparkle is never loaded, and **Check for Updates…** and
the Settings toggle are hidden. That is the state of every local and CI build
until you finish this checklist.

With both set, each platform reads one feed directly under the base URL:

- macOS (Sparkle): `$WAKU_RELEASES_URL/appcast.xml`
- Linux: `$WAKU_RELEASES_URL/appcast-linux-{x86_64,aarch64}.xml`
- Windows: `$WAKU_RELEASES_URL/appcast-windows-{x86_64,aarch64}.xml`

Linux and Windows only download enclosures served over `https` from the same
host as `WAKU_RELEASES_URL`, and verify every download against the compiled-in
key. On macOS the app also checks that the bundle's `SUFeedURL` and
`SUPublicEDKey` match the compiled-in values before loading Sparkle; a
mismatched bundle runs with updates disabled.

The root [`RELEASING.md`](../../RELEASING.md) describes the release pipeline
itself (artifacts, feeds, workflows). This file is the owner's setup
checklist.

---

## 1. Choose where releases live

Pick one. `WAKU_RELEASES_URL` follows from the choice.

### A. Cloudflare R2 (recommended)

Supports binary deltas for macOS, keeps every old version addressable, and
lets `install.sh` pin `WAKU_VERSION` to any release.

1. Cloudflare dashboard → R2 → **Create bucket** (any name, for example
   `waku-fork-releases`). The scripts never create it.
2. Bucket → Settings → **Custom Domains** → attach a domain you own, for
   example `releases.example.com`. Objects are then public at
   `https://releases.example.com/<file>`.
3. R2 → **Manage API Tokens** → create an *Object Read & Write* token scoped
   to that bucket. Note the account id, access key id and secret.
4. `WAKU_RELEASES_URL=https://releases.example.com`.

For local publishing (`bun run release` without `--local`), add an rclone
remote named `r2` (type S3, provider Cloudflare, `no_check_bucket = true`)
with that token, and export `WAKU_R2_BUCKET=<bucket name>`.

### B. GitHub Releases

No extra infrastructure. Feeds are read from the latest *published* release,
and each enclosure points at its own tag's download URL.

1. `WAKU_RELEASES_URL=https://github.com/countradooku/waku/releases/latest/download`
2. Repository variable `WAKU_RELEASE_HOSTING=github`. The release workflow
   then writes per-tag enclosure URLs, and **Sync release** skips R2.

Limitations: drafts and prereleases are never "latest", so updates start only
once a release is published; macOS gets full updates only (no deltas); and
`install.sh` can only install the latest version.

## 2. Generate the Sparkle EdDSA key pair

One Ed25519 key signs updates for all three platforms.

1. Get Sparkle's tools. Run `scripts/bundle.sh debug` once (they are cached in
   `.waku-cache/sparkle/<version>/bin`), or download the matching release from
   [sparkle-project/Sparkle](https://github.com/sparkle-project/Sparkle/releases).
2. Generate the key. It goes into the login keychain and the public half is
   printed:
   ```sh
   .waku-cache/sparkle/2.9.4/bin/generate_keys
   ```
   If this Mac already holds a Sparkle key for another app, use a separate
   account instead (`generate_keys --account waku-fork`). In that case local
   releases must pass the key through `SPARKLE_PRIVATE_KEY`, because
   `generate_appcast` only reads the default account from the keychain.
3. Print the public key again at any time with `generate_keys -p` (plus
   `--account …` if you used one). This value is
   `WAKU_SPARKLE_PUBLIC_ED_KEY`.
4. Export the private key for CI and for your backup:
   ```sh
   .waku-cache/sparkle/2.9.4/bin/generate_keys -x sparkle_private_key.txt
   ```
   The file holds one base64 line; that is the `SPARKLE_PRIVATE_KEY` secret.
   Store it in a password manager, then delete the file.

> Lose the private key and existing installs can never update again. Rotating
> it later requires one release signed with the old key that already carries
> the new public key.

## 3. Apple Developer ID and notarization (macOS)

1. Join the Apple Developer Program.
2. Create a **Developer ID Application** certificate: Xcode → Settings →
   Accounts → Manage Certificates → **+** → Developer ID Application (or on
   developer.apple.com → Certificates).
3. In Keychain Access, export the certificate with its private key as a
   `.p12` with a password, then:
   ```sh
   base64 -i DeveloperID.p12 | pbcopy   # → APPLE_CERTIFICATE
   ```
4. The signing identity is the name `security find-identity -v -p codesigning`
   prints, for example `Developer ID Application: Your Name (TEAMID)`
   → `WAKU_SIGNING_IDENTITY`.
5. Notarization: create an app-specific password at
   [account.apple.com](https://account.apple.com) → Sign-In and Security →
   App-Specific Passwords. You need the Apple ID, that password, and the
   Team ID (developer.apple.com → Membership).
6. For local releases, store the same credentials once:
   ```sh
   xcrun notarytool store-credentials NOTARY \
     --apple-id you@example.com --team-id TEAMID
   ```

## 4. Windows Authenticode certificate (optional)

`scripts/bundle-windows.ts` signs the executables and the installer when a
certificate is configured, and packages them unsigned otherwise. Unsigned
builds still update safely (the EdDSA signature is what the updater checks);
they only trigger SmartScreen warnings.

1. Buy an OV or EV code-signing certificate and export it as a `.pfx` with a
   password.
2. `base64 -i codesign.pfx` → `WINDOWS_CERTIFICATE`; the password →
   `WINDOWS_CERTIFICATE_PASSWORD`.

Linux needs no separate key: the draft-release job signs the tarballs with the
Sparkle key from step 2.

## 5. Configure GitHub

Repository → Settings → Secrets and variables → Actions.

**Variables** (public values):

| Name | Required | Value |
| --- | --- | --- |
| `WAKU_RELEASES_URL` | yes | base URL from step 1 |
| `WAKU_SPARKLE_PUBLIC_ED_KEY` | yes | public key from step 2 |
| `WAKU_RELEASE_HOSTING` | no | `github` for GitHub Releases hosting; unset for R2 |

**Secrets**:

| Name | Required | Value |
| --- | --- | --- |
| `SPARKLE_PRIVATE_KEY` | yes | exported private key from step 2 |
| `WAKU_SIGNING_IDENTITY` | yes | Developer ID identity name (step 3.4) |
| `APPLE_CERTIFICATE` | yes | base64 `.p12` (step 3.3) |
| `APPLE_CERTIFICATE_PASSWORD` | yes | `.p12` password |
| `APPLE_ID` | yes | Apple ID used for notarization |
| `APPLE_APP_SPECIFIC_PASSWORD` | yes | app-specific password (step 3.5) |
| `APPLE_TEAM_ID` | yes | Developer Team ID |
| `WINDOWS_CERTIFICATE` | no | base64 `.pfx` (step 4) |
| `WINDOWS_CERTIFICATE_PASSWORD` | no | `.pfx` password |
| `R2_ACCOUNT_ID` | R2 only | Cloudflare account id |
| `R2_ACCESS_KEY_ID` | R2 only | token access key id |
| `R2_SECRET_ACCESS_KEY` | R2 only | token secret |
| `R2_BUCKET` | R2 only | bucket name |
| `WAKU_ANALYTICS_ENDPOINT` | optional | analytics endpoint embedded in builds; analytics stays off without it |
| `WAKU_ANALYTICS_WEBSITE_ID` | optional | analytics website id, set together with the endpoint |

Analytics is opt-in for users and optional for releases: without both values the build embeds no endpoint and never sends usage events.

The **Release** workflow fails in its first job when `WAKU_RELEASES_URL`,
`WAKU_SPARKLE_PUBLIC_ED_KEY` or `SPARKLE_PRIVATE_KEY` is missing, and
**Sync release** fails when an R2 value is missing (it is skipped entirely
with `WAKU_RELEASE_HOSTING=github`).

## 6. Cut a release

1. Bump `version` in `Cargo.toml` and add a `## [<version>]` section to
   `CHANGELOG.md`.
2. Push a `v<version>` tag, or run **Actions → Release → Run workflow**.
3. Review the draft release, then **publish** it. With R2 hosting, publishing
   runs **Sync release**, which uploads the assets and the mutable feeds.
4. Check the feeds:
   ```sh
   curl -fsSL "$WAKU_RELEASES_URL/appcast.xml" | head
   curl -fsSL "$WAKU_RELEASES_URL/appcast-linux-x86_64.xml" | head
   ```
5. Install an older build, launch it, and choose **Check for Updates…**.

The draft also carries `install.sh` with `DEFAULT_RELEASES_URL` filled in, so
Linux users can run `curl -fsSL "$WAKU_RELEASES_URL/install.sh" | sh`.

## Local builds

- **Development** (`bun run dev`, `cargo run`): debug builds never update, so
  nothing needs configuring.
- **Unsigned or ad-hoc release bundle, updates disabled**:
  ```sh
  WAKU_CODESIGN_IDENTITY=- scripts/bundle.sh release
  open "${CARGO_TARGET_DIR:-target}/release/Waku.app"
  ```
- **Ad-hoc DMG against your own feed**: export `WAKU_RELEASES_URL` and
  `WAKU_SPARKLE_PUBLIC_ED_KEY`, then run
  `bun run release --adhoc`. It writes `dist/Waku-<version>.dmg`, the Sparkle
  zip and a signed `dist/appcast.xml` without publishing. Without the two
  release values it builds the same artifacts with updates disabled and skips
  the appcast.

`build.rs` re-runs whenever either variable changes, so switching them
rebuilds the binary. `--skip-build` and `WAKU_SKIP_CARGO_BUILD=1` reuse
existing binaries: build them with the same values you bundle with, or the
macOS app sees a mismatched Info.plist and disables updates.
