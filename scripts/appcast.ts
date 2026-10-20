#!/usr/bin/env bun
//
// Sign update archives and (re)generate the Sparkle appcast for a directory.
//
// Usage:
//   bun scripts/appcast.ts <updates-dir>
//
// <updates-dir> holds the packaged archives (e.g. Waku-0.2.0.zip) plus any
// older archives so Sparkle can build binary deltas. appcast.xml is written
// into that directory. The private EdDSA key is read from SPARKLE_PRIVATE_KEY
// when set, otherwise from the login keychain (see docs/fork/RELEASING.md).
//
// Env:
//   WAKU_RELEASES_URL          release host base URL (required unless
//                              WAKU_DOWNLOAD_URL_PREFIX is set)
//   WAKU_SPARKLE_PUBLIC_ED_KEY public key the app trusts (required)
//   SPARKLE_BIN                dir containing the Sparkle tools
//   SPARKLE_PRIVATE_KEY        EdDSA private key (CI; otherwise the keychain)
//   WAKU_DOWNLOAD_URL_PREFIX   base URL for enclosure links
//                              (default: $WAKU_RELEASES_URL/)
import { createPrivateKey } from "node:crypto";
import { existsSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";

const projectRoot = resolve(import.meta.dir, "..");

const releasingDoc = "docs/fork/RELEASING.md";

/** The release host every feed and artifact is served from, without a
 *  trailing slash. Fork builds carry no default host, so publishing anything
 *  without one configured is an error rather than a fallback. */
export function releasesBaseUrl(): string {
  const raw = process.env.WAKU_RELEASES_URL?.trim().replace(/\/+$/, "");
  if (!raw) {
    throw new Error(
      `WAKU_RELEASES_URL is not set. Set it to the release host's base URL ` +
        `(e.g. https://releases.example.com); see ${releasingDoc}.`,
    );
  }
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`WAKU_RELEASES_URL is not a URL: ${raw}`);
  }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) {
    throw new Error(`WAKU_RELEASES_URL must be a plain https:// URL, got ${raw}`);
  }
  return raw;
}

/** Where enclosure links point: WAKU_DOWNLOAD_URL_PREFIX when set (e.g. a
 *  per-tag GitHub Releases download path), otherwise the release host. */
export function defaultDownloadUrlPrefix(): string {
  const override = process.env.WAKU_DOWNLOAD_URL_PREFIX?.trim();
  if (override) return override.endsWith("/") ? override : `${override}/`;
  return `${releasesBaseUrl()}/`;
}

/** The public key every Waku build trusts, as `generate_keys -p` prints it.
 *  It is compiled into the app and written into the macOS Info.plist from the
 *  same WAKU_SPARKLE_PUBLIC_ED_KEY, so this is the one value every feed has to
 *  be signed against. */
export function appPublicKey(): string {
  const key = process.env.WAKU_SPARKLE_PUBLIC_ED_KEY?.trim();
  if (!key) {
    throw new Error(
      `WAKU_SPARKLE_PUBLIC_ED_KEY is not set. Set it to the public key ` +
        `\`generate_keys -p\` prints; see ${releasingDoc}.`,
    );
  }
  return key;
}

/** `generate_keys` emits libsodium's 64-byte secret key: seed then public. */
const PKCS8_ED25519_PREFIX = Buffer.from("302e020100300506032b657004220420", "hex");

export function privateKeyFromSparkleSecret(secret: string) {
  const bytes = Buffer.from(secret.trim(), "base64");
  if (bytes.length !== 64 && bytes.length !== 32) {
    throw new Error(
      `SPARKLE_PRIVATE_KEY decodes to ${bytes.length} bytes; expected 32 or 64.`,
    );
  }
  return createPrivateKey({
    key: Buffer.concat([PKCS8_ED25519_PREFIX, bytes.subarray(0, 32)]),
    format: "der",
    type: "pkcs8",
  });
}

/** Derive the public half from the key material itself, rather than trusting
 *  the tail of the stored blob — a mismatched pair has to fail the check
 *  below, not sail through it and sign a feed nothing can verify. The JWK
 *  export carries the raw public key in `x`, base64url-encoded. */
export function publicKeyBase64(privateKey: ReturnType<typeof createPrivateKey>): string {
  const jwk = privateKey.export({ format: "jwk" });
  if (!jwk.x) throw new Error("SPARKLE_PRIVATE_KEY has no public half");
  return Buffer.from(jwk.x, "base64url").toString("base64");
}

/** Refuse to sign with a private key that does not match
 *  WAKU_SPARKLE_PUBLIC_ED_KEY: the app would reject every resulting update. */
export function assertSigningKeyMatches(secret: string): void {
  const derived = publicKeyBase64(privateKeyFromSparkleSecret(secret));
  const expected = appPublicKey();
  if (derived !== expected) {
    throw new Error(
      `SPARKLE_PRIVATE_KEY does not match WAKU_SPARKLE_PUBLIC_ED_KEY (${expected}); ` +
        `it derives ${derived}. Signing with it would ship a feed the app rejects.`,
    );
  }
}

/** Locate Sparkle's `generate_appcast`: SPARKLE_BIN first, then the pinned
 *  distribution scripts/bundle.sh caches under .waku-cache, then PATH. */
export function findGenerateAppcast(): string | null {
  const fromEnv = process.env.SPARKLE_BIN;
  if (fromEnv) {
    const candidate = join(fromEnv, "generate_appcast");
    if (existsSync(candidate)) return candidate;
  }

  const cacheRoot = join(projectRoot, ".waku-cache", "sparkle");
  if (existsSync(cacheRoot)) {
    const versionOrder = new Intl.Collator("en", { numeric: true });
    const versions = readdirSync(cacheRoot)
      .filter((name) => !name.startsWith("."))
      .sort((a, b) => versionOrder.compare(b, a));
    for (const version of versions) {
      const candidate = join(cacheRoot, version, "bin", "generate_appcast");
      if (existsSync(candidate)) return candidate;
    }
  }

  return Bun.which("generate_appcast");
}

/** Sign the archives in `updatesDir` and (re)write appcast.xml. */
export async function generateAppcast(
  updatesDir: string,
  downloadUrlPrefix: string,
): Promise<void> {
  const generator = findGenerateAppcast();
  if (!generator) {
    throw new Error(
      "generate_appcast not found. Run scripts/bundle.sh once to populate " +
        ".waku-cache/sparkle, or set SPARKLE_BIN to a Sparkle tools bin/ dir.",
    );
  }
  console.log(`Using: ${generator}`);
  // Same prefix for both: archives and the Waku-<version>.md release notes are
  // served from the same origin. The notes prefix makes generate_appcast emit
  // <sparkle:releaseNotesLink> for any notes file matching an archive name.
  const privateKey = process.env.SPARKLE_PRIVATE_KEY?.trim();
  // The keychain path cannot be checked up front; the unsigned-enclosure
  // check below still catches a mismatch there.
  if (privateKey) assertSigningKeyMatches(privateKey);
  else appPublicKey();
  const command = [
    generator,
    "--download-url-prefix",
    downloadUrlPrefix,
    "--release-notes-url-prefix",
    downloadUrlPrefix,
    ...(privateKey ? ["--ed-key-file", "-"] : []),
    updatesDir,
  ];
  // The key is fed on stdin (`--ed-key-file -`) so it never lands on disk.
  // Without one, generate_appcast reads the login keychain instead and wants
  // nothing on stdin, so that path just leaves the terminal's attached.
  const child = Bun.spawn(command, {
    stdin: privateKey ? new Blob([`${privateKey}\n`]) : "inherit",
    stdout: "inherit",
    stderr: "inherit",
  });
  const exitCode = await child.exited;
  if (exitCode !== 0) {
    throw new Error(`generate_appcast exited with ${exitCode}`);
  }

  // generate_appcast exits 0 after writing an *unsigned* feed when the key it
  // used does not match the bundle's SUPublicEDKey (written by bundle.sh from
  // WAKU_SPARKLE_PUBLIC_ED_KEY), and Sparkle rejects an unsigned enclosure —
  // so a silent mismatch would ship a dead update feed.
  const appcastPath = join(updatesDir, "appcast.xml");
  const unsigned = [
    ...(await Bun.file(appcastPath).text()).matchAll(/<enclosure\b[^>]*>/g),
  ]
    .filter(([tag]) => !tag.includes("sparkle:edSignature="))
    .map(([tag]) => tag.match(/url="([^"]*)"/)?.[1] ?? tag);
  if (unsigned.length > 0) {
    throw new Error(
      `generate_appcast left ${unsigned.length} enclosure(s) unsigned: ` +
        `${unsigned.join(", ")}. The signing key does not match the app's ` +
        "SUPublicEDKey (check SPARKLE_PRIVATE_KEY or the login keychain, and " +
        "that the app was bundled with WAKU_SPARKLE_PUBLIC_ED_KEY set).",
    );
  }
  console.log(`Wrote ${appcastPath}`);
}

if (import.meta.main) {
  const updatesDir = process.argv[2];
  if (!updatesDir) {
    console.error("usage: bun scripts/appcast.ts <updates-dir>");
    process.exit(1);
  }
  await generateAppcast(updatesDir, defaultDownloadUrlPrefix());
}
