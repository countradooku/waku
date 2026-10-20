// Drive a real daemon's /v8 endpoint with the TypeScript client over JSON, as
// a browser would, and print what came back. Run by the Rust test
// `the_typescript_client_talks_to_the_daemon` (crates/waku-core/src/v8):
//
//   bun smoke.ts ws://127.0.0.1:<port> <token>

import { CallError, V8Client } from "../client";

const [url, token] = process.argv.slice(2);
if (!url || !token) {
  console.error("usage: bun smoke.ts <ws url> <token>");
  process.exit(2);
}

const client = await V8Client.connect({
  url,
  credential: { type: "local", token },
  client: { kind: "web", appVersion: "smoke", os: process.platform, instanceId: crypto.randomUUID() },
});
const settings = await client.call("settings.get", {});
const projects = await client.call("projects.list", {});
const streamItems: string[] = [];
await new Promise<void>((resolve) => {
  client.subscribe(
    "settings",
    {},
    {
      item(item) {
        streamItems.push(item.type);
        if (item.type === "synchronized") resolve();
      },
    },
  );
});
let missingFileError = "";
try {
  await client.call("files.read", { root: { type: "path", path: "/nonexistent-waku-smoke-root" }, path: "x.txt" });
} catch (error) {
  missingFileError = error instanceof CallError ? error.error.code : String(error);
}
console.log(JSON.stringify({ settingsRevision: settings.revision, projects: projects.length, streamItems, missingFileError }));
client.close();
process.exit(0);
