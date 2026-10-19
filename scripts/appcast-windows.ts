#!/usr/bin/env bun
//
// Write the Sparkle-format appcasts the Windows updater reads.
//
// Usage:
//   bun scripts/appcast-windows.ts <assets-dir> <version>
//
// <assets-dir> holds this release's `Waku-<version>-<arch>-Setup.exe` files.
// One feed is written per architecture, because a Sparkle appcast has no way
// to say which binary an item is for. Existing feeds in the directory are
// merged, so older releases keep their entries.
//
// macOS signs through Sparkle's `sign_update`; there is no such tool here, so
// the same EdDSA key is used through Node's Ed25519 primitives. The derived
// public key is checked against WAKU_SPARKLE_PUBLIC_ED_KEY (the key compiled
// into the app) first — an unsigned or wrongly-signed feed is a dead update
// path, and it must fail loudly.
//
// Env:
//   SPARKLE_PRIVATE_KEY        EdDSA private key, base64 (required)
//   WAKU_SPARKLE_PUBLIC_ED_KEY public key the app trusts (required)
//   WAKU_RELEASES_URL          release host base URL (required unless
//                              WAKU_DOWNLOAD_URL_PREFIX is set)
//   WAKU_DOWNLOAD_URL_PREFIX   base URL for enclosure links
//                              (default: $WAKU_RELEASES_URL/)
import { sign } from "node:crypto";
import { readdirSync, statSync } from "node:fs";
import { join } from "node:path";

import {
  assertSigningKeyMatches,
  defaultDownloadUrlPrefix,
  privateKeyFromSparkleSecret,
} from "./appcast.ts";

export {
  appPublicKey,
  assertSigningKeyMatches,
  privateKeyFromSparkleSecret,
  publicKeyBase64,
} from "./appcast.ts";

/** Rust target arch, as it appears in the installer name and the feed name. */
export const architectures = ["x86_64", "aarch64"] as const;
export type Architecture = (typeof architectures)[number];

export const appcastName = (arch: Architecture) => `appcast-windows-${arch}.xml`;

export function escapeXml(value: string): string {
  return value.replace(
    /[&<>"']/g,
    (character) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;" })[
        character
      ]!,
  );
}

export interface AppcastItem {
  version: string;
  url: string;
  length: number;
  signature: string;
  pubDate: string;
}

/** Keep every previously published item so a far-behind install still
 *  resolves, and let a re-run of the same version replace its own entry. */
export function mergeItems(
  existing: AppcastItem[],
  incoming: AppcastItem[],
): AppcastItem[] {
  const byVersion = new Map(existing.map((item) => [item.version, item]));
  for (const item of incoming) byVersion.set(item.version, item);
  return [...byVersion.values()].sort((a, b) => compareVersions(b.version, a.version));
}

export function compareVersions(left: string, right: string): number {
  const fields = (version: string) =>
    version
      .split(/[-+]/)[0]!
      .split(".")
      .map((field) => Number.parseInt(field, 10) || 0);
  const a = fields(left);
  const b = fields(right);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const difference = (a[index] ?? 0) - (b[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

export function parseAppcast(xml: string): AppcastItem[] {
  return xml
    .split("<item>")
    .slice(1)
    .flatMap((chunk) => {
      const item = chunk.split("</item>")[0] ?? "";
      const enclosure = item.split("<enclosure")[1]?.split(">")[0] ?? "";
      const attribute = (name: string) =>
        enclosure.split(`${name}="`)[1]?.split('"')[0];
      const version =
        item.split("<sparkle:shortVersionString>")[1]?.split("<")[0]?.trim() ??
        attribute("sparkle:shortVersionString");
      const url = attribute("url");
      const signature = attribute("sparkle:edSignature");
      if (!version || !url || !signature) return [];
      return [
        {
          version,
          url,
          signature,
          length: Number.parseInt(attribute("length") ?? "0", 10) || 0,
          pubDate: item.split("<pubDate>")[1]?.split("<")[0]?.trim() ?? "",
        },
      ];
    });
}

export function renderAppcast(arch: Architecture, items: AppcastItem[]): string {
  const entries = items
    .map(
      (item) => `    <item>
      <title>${escapeXml(item.version)}</title>
      <pubDate>${escapeXml(item.pubDate)}</pubDate>
      <sparkle:version>${escapeXml(item.version)}</sparkle:version>
      <sparkle:shortVersionString>${escapeXml(item.version)}</sparkle:shortVersionString>
      <sparkle:minimumSystemVersion>10.0.17763</sparkle:minimumSystemVersion>
      <enclosure url="${escapeXml(item.url)}" length="${item.length}" type="application/octet-stream" sparkle:edSignature="${escapeXml(item.signature)}" sparkle:os="windows" />
    </item>`,
    )
    .join("\n");
  return `<?xml version="1.0" encoding="utf-8"?>
<rss xmlns:sparkle="http://www.andymatuschak.org/xml-namespaces/sparkle" version="2.0">
  <channel>
    <title>Waku (Windows ${arch})</title>
${entries}
  </channel>
</rss>
`;
}

export async function generateWindowsAppcasts(
  assetsDir: string,
  version: string,
  downloadUrlPrefix: string,
  pubDate: string,
): Promise<string[]> {
  const secret = process.env.SPARKLE_PRIVATE_KEY?.trim();
  if (!secret) {
    throw new Error("SPARKLE_PRIVATE_KEY is required to sign the Windows feed.");
  }
  assertSigningKeyMatches(secret);
  const privateKey = privateKeyFromSparkleSecret(secret);

  const present = new Set(readdirSync(assetsDir));
  const written: string[] = [];
  for (const arch of architectures) {
    const installer = `Waku-${version}-${arch}-Setup.exe`;
    if (!present.has(installer)) {
      console.warn(`No ${installer} in ${assetsDir}; leaving that feed alone.`);
      continue;
    }
    const path = join(assetsDir, installer);
    const bytes = new Uint8Array(await Bun.file(path).arrayBuffer());
    const item: AppcastItem = {
      version,
      url: `${downloadUrlPrefix}${installer}`,
      length: statSync(path).size,
      signature: Buffer.from(sign(null, bytes, privateKey)).toString("base64"),
      pubDate,
    };

    const feedPath = join(assetsDir, appcastName(arch));
    const previous = (await Bun.file(feedPath).exists())
      ? parseAppcast(await Bun.file(feedPath).text())
      : [];
    await Bun.write(feedPath, renderAppcast(arch, mergeItems(previous, [item])));
    written.push(feedPath);
    console.log(`Wrote ${feedPath} (${item.length} bytes signed)`);
  }
  if (written.length === 0) {
    throw new Error(`No Waku-${version}-<arch>-Setup.exe found in ${assetsDir}`);
  }
  return written;
}

if (import.meta.main) {
  const [assetsDir, version] = process.argv.slice(2);
  if (!assetsDir || !version) {
    console.error("usage: bun scripts/appcast-windows.ts <assets-dir> <version>");
    process.exit(1);
  }
  await generateWindowsAppcasts(
    assetsDir,
    version,
    defaultDownloadUrlPrefix(),
    new Date().toUTCString(),
  );
}
