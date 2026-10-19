# T3 Code: remote, auth, multi-device, install/update and CLI feature catalogue (for a Rust reimplementation)

This was a read-only audit. Every path below is relative to the repo root `t3code/`. I read the docs you listed and the source under `apps/server/src/{auth,cloud,relay,environment,device,desktopUpdate,secrets,cli,background,process,diagnostics}` plus the top-level files, along with `packages/{ssh,tailscale}`, `packages/shared/src/{relayClient,connectAuth,relayJwt,desktopBootstrapToken,desktopAppControl,cliRelease,dpop}.ts` and the contracts. I also followed a few dependencies outside your list where in-scope code relies on them (scheduledTasks webhook route, mcp/McpHttpServer, persistence auth tables, resourceTelemetry desktop control channel).

Complexity scale for Rust: **S** is under 1 day, **M** is a few days, **L** is 1–2 weeks, **XL** is several weeks or protocol-heavy.

---

## 0. Cross-cutting facts that shape the whole port

- **WebSocket transport is the Effect RPC protocol, JSON-serialized.** It is `RpcServer.makeProtocolWithHttpEffectWebsocket` plus `RpcSerialization.layerJson`, with `disableTracing` and `disableFatalDefects` set (`apps/server/src/ws.ts`, `WS_RPC_SERVER_OPTIONS`).
  - Rust has to reproduce the Effect RPC wire framing: request, chunk/stream, exit, interrupt, ack and ping messages, and the tagged-error encoding.
  - Terminal output has custom flow control (`withTerminalOutputWindow`, `apps/server/src/terminal/OutputProtocol.ts`).
  - The server uses per-message deflate with context takeover (`server.ts`, `layerHttpServer`). **XL.**
- **Single-owner state directory.** All durable state lives under the T3 home (`~/.t3` by default, or `T3CODE_HOME` / `--base-dir`). Paths come from `apps/server/src/config.ts` `deriveServerPaths`:
  - State dir is `<base>/userdata`, or `<base>/dev` when `--dev-url` is set and the base dir is not explicit.
  - Inside it: `statev2.sqlite`, `settings.json`, `keybindings.json`, `themes/`, `secrets/*.bin` (mode 0700/0600), `environment-id`, `server-runtime.json`, `server-owner.sqlite`, `logs/server.trace.ndjson`, `logs/provider/events.log`, `logs/terminals/`, `attachments/`, `browser-artifacts/`, `anonymous-id`.
  - Outside it, under the base: `<base>/worktrees`, `<base>/caches`, `<base>/runtime/` (service state, versions, db-backup, markers), `<base>/tools/{cloudflared,expo-device-hub,agent-device}/<ver>/`.
- **The CLI writes the same SQLite and secrets directly** while a server runs. `t3 pair`, `t3 auth …` and `t3 connect …` mint credentials through `EnvironmentAuth.layerRuntime` against the shared DB. Other commands (`t3 project …`, live `connect unlink`) mint a session locally and then call the live server over HTTP. So Rust needs SQLite WAL multi-process safety.
- **HTTP request gating.**
  - Every HTTP route waits for startup command readiness (global middleware `layerCommandReadiness` in `server.ts`).
  - Before routes are attached, the raw Node server answers 503 with `Retry-After: 1` (`apps/server/src/httpResponseErrorGuard.ts`, `rejectRequestsUntilServing`).
  - Effect's built-in global middlewares are on: CORS (`layerBrowserApiCors`) and compression (`layerHttpCompression`).
- **Wire-compatibility rules** (from `docs/internals/environment-auth.md` and `remote.md`): clients rely on advertised capabilities, the legacy `scopes` plus new `permissions` fields, and the legacy `requiredScope` plus new `requiredPermission` fields. Old clients must keep decoding.

---

## 1. Complete HTTP route inventory (environment server)

Routes are assembled in `apps/server/src/server.ts` (`layerMakeRoutes`). Typed routes are declared in `packages/contracts/src/environmentHttp.ts` (`EnvironmentHttpApi`).

| Method | Path | Auth | Handler / notes |
|---|---|---|---|
| GET | `/.well-known/t3/environment` | none | Execution environment descriptor (`apps/server/src/http.ts` `layerServerEnvironmentHttpApi`). Also used as a reachability probe by `t3 pair`, the Tailscale probe and clients. |
| GET | `/api/auth/session` | optional | `AuthSessionState`. Returns the descriptor even when unauthenticated. Migrates a legacy cookie to the new name (`apps/server/src/auth/http.ts`). |
| POST | `/api/auth/browser-session` | none (credential in body) | Exchanges a bootstrap credential for an HttpOnly SameSite=Lax cookie. Can replace the previous browser session. |
| POST | `/oauth/token` | optional DPoP header | Form-encoded RFC 8693 token exchange. Bootstrap credential becomes a Bearer or DPoP access token. |
| POST | `/api/auth/websocket-ticket` | authenticated | Short-lived `wsTicket` (5 min). |
| POST | `/api/auth/pairing-token` | `access:write` plus every delegated scope | Create a pairing credential. |
| GET | `/api/auth/pairing-links` | `access:read` | List active links (no secrets). |
| POST | `/api/auth/pairing-links/revoke` | `access:write` | |
| GET | `/api/auth/clients` | `access:read` | List client sessions. |
| POST | `/api/auth/clients/revoke` | `access:write` | Revoking your own session gives 403 `current_session_revoke_not_allowed`. |
| POST | `/api/auth/clients/revoke-others` | `access:write` | |
| GET | `/.well-known/oauth-protected-resource` and `/.well-known/oauth-protected-resource/mcp` | none | MCP OAuth metadata (`apps/server/src/auth/mcpOAuthHttp.ts`). |
| GET | `/.well-known/oauth-authorization-server` | none | MCP OAuth authorization-server metadata. |
| POST | `/oauth/mcp/register` | none | Stateless dynamic client registration (signed client_id). |
| GET | `/oauth/mcp/authorize` | none | Validates the request, then 302 to `/connect-agent?...` (a web SPA page) or to the error redirect/page. |
| POST | `/oauth/mcp/approval` | optional cookie | Returns approval details (client name, redirect host, CSRF token, one-click access). |
| POST | `/oauth/mcp/decision` | cookie+CSRF or pairing code | deny / pairing-code / browser-session decision; returns `{redirectTo}`. |
| POST | `/oauth/mcp/token` | none (PKCE) | Form-encoded authorization_code to bearer token. |
| GET/POST/DELETE | `/mcp` | MCP bearer (process registry token or `mcp-client` OAuth session) | MCP streamable HTTP, protocol 2025-06-18, session termination allowed (`apps/server/src/mcp/McpHttpServer.ts`). 401 carries `www-authenticate` with `resource_metadata`. |
| GET | `/api/orchestration/shell` | `orchestration:read` + header `x-t3-orchestration-protocol: 2` | `apps/server/src/orchestration-v2/http.ts` |
| GET | `/api/orchestration/threads/:threadId` | same | |
| GET | `/api/orchestration/threads/:threadId/transcript` | same | |
| GET | `/api/orchestration/threads/:threadId/bounded?compactTurnItems=` | same | |
| GET | `/api/orchestration/threads/:threadId/history?cursor=&throughEntryId=&view=` | same | |
| GET | `/api/projects` | `orchestration:read` | `apps/server/src/project/http.ts` |
| POST | `/api/projects/mutate` | `orchestration:operate`, plus `settings:write` when scripts are included | |
| POST | `/api/pull-requests/diff` | `orchestration:read` | `apps/server/src/pullRequest/http.ts` |
| POST | `/api/connect/link-proof` | `relay:write`; must arrive on loopback with no `X-Forwarded-*` headers | `apps/server/src/cloud/http.ts` |
| POST | `/api/connect/relay-config` | `relay:write` | Installs the relay link config; may answer 503 `EnvironmentCloudEndpointUnavailableError`. |
| GET | `/api/connect/link-state` | `relay:read` | |
| POST | `/api/connect/unlink` | `relay:write` | |
| POST | `/api/connect/preferences` | `relay:write` | publishAgentActivity / holdWebhooksWhileOffline |
| POST | `/api/t3-connect/health` | relay-signed JWT in body | Signed health response. |
| POST | `/api/connect/mint-credential` and `/api/t3-connect/mint-credential` | relay-signed JWT in body | Mints a DPoP-bound one-time credential. |
| POST/PUT/PATCH/GET | `/api/hooks/:hookId/:token` | token in path, optional HMAC signature | Webhook trigger (`apps/server/src/scheduledTasks/webhookRoute.ts`). 1 MiB cap. Sets `x-t3-hook-outcome`. Excluded from tracing. |
| POST | `/api/observability/v1/traces` | `orchestration:operate` | Browser OTLP trace proxy; tracing disabled for it (`apps/server/src/http.ts`). |
| GET (HEAD) | `/api/assets/*` | signed URL token (bearer-in-URL) | Assets/media with Range support (`apps/server/src/assets/AssetAccess.ts`, `http.ts`). |
| POST | `/api/attachments/upload/<token>` | signed upload token | Content-Length must match the token's size. |
| * | `/api/device-hub/*` | cookie or `wsTicket`; read or operate scope | Allowlisted reverse proxy, HTTP + WebSocket, to the loopback device hub (`apps/server/src/device/DeviceHubProxy.ts`). |
| GET | `/api/preview-stream/*` (incl. `/ws`, `/download`) and POST `/api/preview-stream/upload` | cookie or `wsTicket`; `orchestration:read` (interactive needs `preview:operate`) | `apps/server/src/preview/ServerBrowserStream.ts` |
| GET | `/ws` | cookie, Bearer, DPoP or `?wsTicket=` | WebSocket RPC (see §2). |
| GET | `*` | none | Static SPA, or dev 302 redirect to Vite for loopback hosts. Handles `/pair` and `/connect-agent` client routes; immutable caching for hashed assets; path-traversal guard (`http.ts` `layerStaticAndDevRoute`). |

CLI-only temporary listener: `GET /callback` on `127.0.0.1:34338` during `t3 connect login` (`apps/server/src/cloud/CliTokenManager.ts`).

**CORS** (`apps/server/src/httpCors.ts`, `http.ts` `layerBrowserApiCors`):
- Methods GET, POST, OPTIONS; headers `authorization`, `b3`, `traceparent`, `content-type`, `dpop`, `x-t3-orchestration-protocol`; maxAge 600.
- In dev, credentialed with explicit origins: devUrl origin, `t3code://app`, `t3code-dev://app`, `T3CODE_DEV_ALLOWED_ORIGINS`. Otherwise wildcard. **S.**

**Error envelope** (`environmentHttp.ts`): tagged JSON errors `EnvironmentRequestInvalidError` (400), `EnvironmentAuthInvalidError` (401, with `dpopFailureReason`), `EnvironmentScopeRequiredError` (403, `requiredScope` + `requiredPermission`), `EnvironmentOperationForbiddenError` (403), `EnvironmentResourceNotFoundError` (404), `EnvironmentInternalError` (500). Each carries `traceId`. A DPoP 401 adds `WWW-Authenticate: DPoP`. Credential responses carry `cache-control: no-store`.

---

## 2. `/ws` handshake (`apps/server/src/ws.ts`, around line 3095)

1. **Protocol check.** The query must have `orchestrationProtocol=2` (`ORCHESTRATION_PROTOCOL_QUERY_PARAM`, `ORCHESTRATION_PROTOCOL_VERSION` in `packages/contracts/src/environment.ts`). Otherwise the server answers HTTP 426 with JSON `{code:"orchestration_protocol_incompatible", message, orchestrationProtocolVersion:2}`.
2. **Authentication** via `EnvironmentAuth.authenticateWebSocketUpgrade`. A `wsTicket` query param takes precedence; otherwise cookie, Bearer or DPoP. `mcp-client` sessions are rejected. Browser-cookie credentials must pass the Origin check (`isBrowserOriginAllowed`).
3. **Client metadata** comes from query params `clientSurface`, `clientAppVersion`, `clientDeviceType`, `clientOs`, `clientWebDeployment`, `clientBrowser`, `connectionMethod`. It is recorded to `auth_sessions` and to analytics `client.connected`.
4. **Connection tracking.** `sessions.markConnected` / `markDisconnected` keep a refcount per session, emit access-stream upserts and set `last_connected_at`.
5. **Revocation and expiry.** The socket is raced against `sessions.awaitInvalidation`: revocation, or the session expiry timer, ends the socket.
6. **Per-RPC authorization.** The `RpcScopeAuthorization` middleware (`apps/server/src/auth/RpcAuthorization.ts`) checks each RPC's required scope(s), including payload-dependent scopes, before the handler runs. A successful RPC can emit a `feature.used` analytics event.

Complexity: **M** (handshake), **XL** with the RPC protocol itself.

---

## 3. WebSocket RPC method names in scope (exact strings, from `packages/contracts/src/rpc.ts` `WS_METHODS`)

Required scopes are listed per group (from `RpcAuthorization.ts` and `packages/contracts/src/clientRpcPermissions.ts`).

**Server / config / lifecycle**
- `server.probe` (read) and `server.getConfig` (read). `getConfig` returns `ServerConfig`: descriptor, auth descriptor, keybindings, providers, editors, `remoteOpenTargets`, `directEndpoints`, observability, settings, and capability flags.
- `subscribeServerConfig` (read, stream). Payload flags `environmentThemes`, `usageLimitSources`, `usageLimitsCommand`. Events: `snapshot`, `keybindingsUpdated`, `providerStatuses`, `settingsUpdated`, `environmentThemesUpdated`, `usageLimitSourcesUpdated`.
- `subscribeServerLifecycle` (read, stream). Events `welcome`, `ready` (with `updateOutcome`), `legacyThreadMigration`, each with a sequence number. Sent as a snapshot replay followed by live events.

**Settings, keybindings, storage**
- `server.getSettings` (read; secrets redacted).
- `server.updateSettings`: scope depends on the patch. `settings:write`, plus `providers:manage` when `providers`, `providerInstances`, `usageLimitSources` or a provider-instance mutation is touched.
- `server.upsertKeybinding` and `server.removeKeybinding` (`settings:write`).
- `server.runStorageCleanup` (`settings:write`); `server.getStorageCleanupReport` (read, stream); `server.getDatabaseStorage` (read, stream).

**Update**
- `server.updateServer`, `server.updateServerWithProgress` (stream: `progress{stage:downloading|installing}` then `complete{result}`) and `server.commitDesktopUpdate`. All need `environment:maintain`.

**Auth / access**
- `subscribeAuthAccess` (`access:read`, stream). Events `snapshot`, `pairingLinkUpserted`, `pairingLinkRemoved`, `clientUpserted`, `clientRemoved`, each with a revision number.

**Background / power**
- `server.reportClientActivity` (read) creates client activity leases.
- `server.reportHostPowerState` (`environment:maintain`).
- `server.getBackgroundPolicy` and `subscribeBackgroundPolicy` (read).

**Diagnostics**
- `server.getTraceDiagnostics`, `server.getProcessDiagnostics`, `server.getProcessResourceHistory`, `server.getResourceTelemetryHistory`, `subscribeResourceTelemetry`, `server.getUsageSummary`, `server.refreshUsageRates`: all `diagnostics:read`.
- `server.retryResourceTelemetry`: `environment:maintain` + `diagnostics:read`.
- `server.getHostResources`: `orchestration:read` (used for load balancing).
- `server.signalProcess` (`environment:maintain`; SIGINT/SIGKILL by pid + start time).
- `server.checkOtlpEndpoint` (`settings:write`).

**T3 Connect relay client**
- `cloud.getRelayClientStatus` (`relay:read`).
- `cloud.installRelayClient` (`relay:write`, stream). Stages: `checking`, `waiting_for_lock`, `downloading`, `verifying`, `installing`, `validating`, `activating`, then `complete`.

**Secrets**
- `secrets.answerRequest` (`orchestration:operate`).

**Devices**
- `device.configure` and `device.testHost` (`settings:write`).
- `device.list` (read; operate if `retryHostId` or `updateTool` is set).
- `device.open`, `device.close`, `device.shutdown`, `device.action` (`orchestration:operate`).
- `device.detail` and `subscribeDeviceState` (read).

**Webhook automation** (adjacent)
- `scheduledTasks.list`, `scheduledTasks.subscribe` (read).
- `scheduledTasks.upsert`, `.setEnabled`, `.delete`, `.runNow`, `.rotateWebhookToken`, `.listWebhookDeliveries`, `.getWebhookDelivery` (operate).

**Other method groups that the scope table also covers** (outside this slice, listed for completeness):
- `projects.*` (`projects.list`, `.add`, `.remove`, `.listEntries`, `.readFile`, `.searchContents`, `.searchEntries`, `.writeFile`, `.mutate`, `.ensureScratch`, `.createNew`), `shell.openInEditor`, `filesystem.browse`, `filesystem.getMetadata`.
- `agentSessions.scan` / `.import`; `assets.createUrl` (filesystem:read for workspace, media or draft file resources, otherwise orchestration:read); `assets.persistChatAttachments`; `attachments.createUploadUrl` / `.delete`; `mcpApps.*`.
- `provider.*`: `provider.auth.start|complete|respond|cancel|logout|subscribe`, `provider.install.start|cancel|subscribe|remove`, `provider.consumeResetCredit`, `provider.chatgpt.reconnect-profile|import-profile|handoff.subscribe`, `provider.codex.auth-callback.subscribe`, `provider.uploadFeedback`.
- `server.refreshProviders`, `server.updateProvider`, the `server.*AcpRegistry*` family, `server.discoverSourceControl`.
- `vcs.*`, `git.*`, `review.*`, `terminal.*`, `preview.*`, `pullRequests.*`, `sourceControl.*`, `projectClone.*`, and the subscriptions `subscribeVcsStatus`, `subscribeWorktreeSetup`, `worktreeSetup.cancel`, `subscribeTerminalEvents`, `subscribeTerminalMetadata`, `subscribePreviewEvents`, `subscribeDiscoveredLocalServers`, `subscribeProjectClones`.
- Orchestration (`packages/contracts/src/orchestrationV2.ts` `ORCHESTRATION_V2_WS_METHODS`): `orchestration.dispatchCommand`, `.getTurnDiff`, `.getFullThreadDiff`, `.searchThreads`, `.searchThread`, `.searchThreadStream`, `.getArchivedShellSnapshot`, `.getThreadProjection`, `.getWorkflowScript`, `.stopWorkflow`, `.getTurnItem`, `.launchThread`, `.subscribeArchivedShell`, `.subscribeShell`, `.subscribeThread`, `.subscribeCommandOutput`.

---

## 4. Feature catalogue

### A. Environment authentication and access control

**A1. Scope model and permission compatibility.** 17 scope literals:
- `orchestration:read|operate`, `settings:write`, `providers:manage`, `environment:maintain`, `preview:operate`, `diagnostics:read`, `terminal:read|operate`, `filesystem:read|write`, `review:write` (retired; decode only), `source-control:write`, `access:read|write`, `relay:read|write`.
- `AuthStandardClientScopes` is the default grant: everything except `review:write`.
- `authScopeResponse` filters `scopes` down to the legacy vocabulary and puts the exact grant in `permissions`. `legacyParents` maps new scopes to legacy parents for 403 `requiredScope`.
- Paths: `packages/contracts/src/auth.ts`, `apps/server/src/auth/RpcAuthorization.ts`, `packages/contracts/src/clientRpcPermissions.ts`, `packages/contracts/src/settings.ts` (`requiredScopesForServerSettingsPatch`, `requiredScopesForProjectMutation`). **M.**

**A2. Auth policy descriptor.**
- Policy is `desktop-managed-local`, `loopback-browser` or `remote-reachable`, chosen by mode and whether the bind host is remote-reachable (`unsafe-no-auth` exists only in the contract).
- Bootstrap methods: `desktop-bootstrap` and/or `one-time-token`. Session methods: cookie, bearer, DPoP. `serverUpdateScope` is `environment:maintain`.
- Cookie name: desktop uses `t3_session_<port>`; web uses `t3_session_<port>_<hash12(stateDir)>`, or `t3_session_<hash12(environmentId)>` when remote-reachable and not in dev. The legacy name `t3_session` is migrated.
- Paths: `apps/server/src/auth/EnvironmentAuthPolicy.ts`, `apps/server/src/auth/utils.ts`. **S.**

**A3. Pairing links (one-time bootstrap credentials).**
- 12-character token from alphabet `23456789ABCDEFGHJKLMNPQRSTUVWXYZ` (rejection sampling). Default TTL 5 minutes; dev startup links 24 hours.
- Stored in table `auth_pairing_links`: id, credential, method, scopes, subject, label, proof_key_thumbprint, created/expires/consumed/revoked.
- Consumption is atomic: scope intersection and the proof-key check are part of the `UPDATE … WHERE`, so a rejected request never consumes the link. Errors distinguish unknown, expired, revoked/unavailable, proof-mismatch and scope-not-granted.
- A PubSub change stream feeds the access stream.
- Subjects: `one-time-token`, `administrative-bootstrap` (startup link; hidden from listings), `cloud-connect` (T3 Connect mint; 2-minute TTL; DPoP-thumbprint-bound).
- Paths: `apps/server/src/auth/PairingGrantStore.ts`, `apps/server/src/persistence/AuthPairingLinks.ts`, migrations `020`, `031`, `032` in `apps/server/src/persistence/Migrations/`. **M.**

**A4. Desktop bootstrap credential.**
- Either a fixed seeded token (unbounded uses, 24-hour TTL) or a rotating token. The rotating token is `HMAC-SHA256(secret, "t3-desktop-bootstrap:<window>")` hex, with a 12-hour window, accepting the previous, current and next window.
- The desktop grant replaces active sessions for the same subject and method in one transaction (`replaceActiveForSubjectAndMethod`).
- Paths: `packages/shared/src/desktopBootstrapToken.ts`, `PairingGrantStore.ts`, `apps/server/src/persistence/AuthSessions.ts`. **S.**

**A5. Session tokens.**
- Format: `base64url(JSON claims).base64url(HMAC-SHA256)`, keyed by secret `server-signing-key` (32 random bytes).
- Claims: `{v, kind:"session", sid, sub, scopes, method, jkt?, rtc?, iat, exp}` (milliseconds).
- Default TTL 30 days; DPoP tokens 1 hour; MCP client 30 days.
- Verification checks signature, expiry and the DB row (not revoked).
- Table `auth_sessions` stores client metadata (label, ip, UA, deviceType, os, browser, surface, appVersion) and `last_connected_at`.
- Operations: list, revoke, revoke-all-except, connected refcount, and an invalidation stream.
- Path: `apps/server/src/auth/SessionStore.ts`. **M.**

**A6. Browser cookie sessions.**
- `POST /api/auth/browser-session` sets an HttpOnly, SameSite=Lax, path `/` cookie with `no-store` headers. A previous cookie session is replaced.
- Cookie credentials are accepted only when the request passes the Origin check:
  - no Origin header;
  - `Sec-Fetch-Site: same-origin`;
  - Origin equals Host or `X-Forwarded-Host`;
  - Origin is one of the trusted origins (devUrl, `t3code://app`, `t3code-dev://app`, `T3CODE_DEV_ALLOWED_ORIGINS`).
- Credential precedence: cookie, then Bearer, then DPoP, then legacy cookie, then dev cookie.
- Paths: `EnvironmentAuth.ts` (`selectRequestCredential`, `isBrowserOriginAllowed`), `auth/http.ts`. **M.**

**A7. OAuth token exchange (Bearer / DPoP).**
- `POST /oauth/token`, form-encoded.
  - `grant_type`: `urn:ietf:params:oauth:grant-type:token-exchange`
  - `subject_token_type`: `urn:t3:params:oauth:token-type:environment-bootstrap`
  - `requested_token_type`: `urn:ietf:params:oauth:token-type:access_token`
  - Optional `scope`, `client_label`, `client_device_type`, `client_os`.
- Requested scopes are intersected with the grant; unknown names are dropped. An empty result fails before the link is consumed.
- A DPoP header binds the token to the key's `jkt`.
- Paths: `auth/http.ts` handler `token`, `EnvironmentAuth.exchangeBootstrapCredentialForAccessToken`, `packages/shared/src/oauthScope.ts`. **M.**

**A8. DPoP verification.**
- ES256 proof with `typ`, `htm`, `htu` (normalized), `jti`, `iat`, and `ath` for access tokens. Max age 300 seconds, +5 seconds future skew.
- Replay guard: secret-store file `dpop-proof-<b64url(sha256(jkt:jti))>.bin` created exclusively; "already exists" means replay.
- Failure reasons: `time_window`, `key_mismatch`, `request_mismatch`, `token_mismatch`, `replay`, `invalid_proof`.
- A DPoP-bound token used without DPoP must fail; it never falls back to Bearer.
- The URL is rebuilt from the request (Host / X-Forwarded-Proto), so proxies must preserve them.
- Paths: `apps/server/src/auth/dpop.ts`, `packages/shared/src/dpop.ts`. **M.**

**A9. WebSocket and media tickets.**
- `POST /api/auth/websocket-ticket` returns `{v:1, kind:"websocket", sid, iat, exp}` signed with the same key, valid 5 minutes, used as `?wsTicket=`.
- The same mechanism authenticates `/api/device-hub/*` and `/api/preview-stream/*` (`authenticateMediaRequest`), and the ticket is stripped before proxying.
- Paths: `SessionStore.ts`, `auth/http.ts`. **S.**

**A10. Access management APIs and live access stream.** Pairing link list/revoke, client session list/revoke/revoke-others (self-revoke forbidden), and the `subscribeAuthAccess` stream. Sessions are sorted by access:write first, then connected, then newest. Paths: `auth/http.ts`, `ws.ts` (`subscribeAuthAccess`, `loadAuthAccessSnapshot`). **M.**

**A11. Startup and headless pairing URL.**
- URL shape: `<base>/pair#token=<credential>`. The token is in the fragment, never in the query.
- `t3 serve` prints connection string, token, pairing URL and an ANSI block-character QR code.
- `t3`/`start` prints `pairingUrl:` and opens the browser.
- Paths: `apps/server/src/startupAccess.ts`, `EnvironmentAuth.issueStartupPairingUrl`, `serverRuntimeStartup.ts`. **S** (QR rendering needs a crate).

**A12. Reusable dev credential (`T3CODE_DEV_AUTH_TOKEN`).**
- Web + dev mode only; the token must be at least 32 characters.
- sha256 of the token gives the session id `dev-auth-<hex>` and cookie `t3_dev_session_<hex>`. A DB row is seeded with expiry year 9999. Children issued from it are normal sessions.
- Paths: `apps/server/src/auth/ReusableDevAuth.ts`, `SessionStore.ts`, `cli/config.ts`. **S.**

**A13. Replay-marker pruning.** Every hour, delete `dpop-proof-*`, `cloud-mint-nonce-*`, `cloud-mint-jti-*`, `cloud-health-nonce-*` and `cloud-health-jti-*` `.bin` files older than 1 day. Runs after activation. Path: `apps/server/src/auth/replayMarkers.ts`. **S.**

**A14. RPC authorization middleware.** A static method-to-scope table plus payload-dependent rules (`assets.createUrl`, `server.updateSettings`, `projects.mutate`, `git.preparePullRequestThread`, `server.retryResourceTelemetry`, `device.list`). Rejects with `EnvironmentAuthorizationError{requiredScope, requiredPermission}`. Path: `RpcAuthorization.ts`. **M.**

### B. MCP OAuth server (outside agents)

**B1. Authorization-code server.**
- Endpoints: `/oauth/mcp/*` and the `/.well-known` metadata.
- Issuer and resource are derived per request from Host and `X-Forwarded-Proto`; resource is `<issuer>/mcp`.
- Registration is stateless: `client_id = base64url({v:1,n,r}).sig` signed with `mcp-oauth-signing-key`, HMAC domain-separated. Limits: 1–5 redirect URIs, each up to 512 characters, name up to 100.
- Redirect URIs must be loopback (port-agnostic match per RFC 8252) or HTTPS (exact match).
- PKCE S256 only; challenge 43 chars; verifier 43–128.
- Codes are held in memory with a 60-second TTL. The CSRF token is an HMAC over session id and request.
- Decisions: `deny`; `pairing-code` (spends a one-time code that must hold all scopes for the chosen access; proof-bound or desktop grants are refused); `browser-session` (cookie session with `access:write`, plus CSRF).
- Access levels: `read-only` maps to `orchestration:read`. `approval-required`, `auto-accept-edits` and `auto` map to read+operate. `full-access` adds `settings:write` and `source-control:write`. The ceiling is stored as the `rtc` claim.
- The resulting session has subject `mcp-client` and is accepted only on `/mcp`.
- Error HTML page: `mcpOAuthHtml.ts`.
- Paths: `apps/server/src/auth/McpOAuth.ts`, `mcpOAuthHttp.ts`, `mcpOAuthHtml.ts`, `EnvironmentAuth.ts` (`mcpClientScopes`, `issueMcpClientSession`, `consumeMcpApprovalCode`, `authenticateMcpClient`). **L.**

**B2. `/mcp` auth middleware.** Accepts either a process-registry token (a bare base64url secret for T3-launched agents, via `McpSessionRegistry`) or an OAuth `mcp-client` bearer token. A 401 offers OAuth via `resource_metadata` in `www-authenticate`. An empty 200 is normalized to 202. Path: `apps/server/src/mcp/McpHttpServer.ts`. **M** (the MCP protocol itself is out of scope here).

### C. Environment identity and descriptor

**C1. Environment ID.**
- UUIDv4 persisted at `<stateDir>/environment-id`. Atomic publish: write a temp file, then `link()` without overwrite.
- Recovery path `environment-id.recovery` handles an empty file so concurrent initializers converge on one winner.
- Path: `apps/server/src/environment/ServerEnvironment.ts` (`makeIdentity`). **S.**

**C2. Execution environment descriptor and capabilities.**
- Fields: `environmentId`, `label`, `platform{os, arch, machine?}`, `serverVersion`, `orchestrationProtocolVersion: 2`, `capabilities` (about 45 booleans).
- Notable capabilities: `serverSelfUpdate` is `boot-service` or `desktop-managed`; `serverInstallation` is set when neither applies. `serverSelfUpdateProgress`, `desktopAppUpdate`, `serverBrowser`, `fileAttachments.maxUploadBytes`.
- `agentActivityPublishing` is computed per request from secrets.
- Paths: `ServerEnvironment.ts`, `packages/contracts/src/environment.ts`. **M** (mostly constant data).

**C3. Environment label.** macOS `scutil --get ComputerName`; Linux `/etc/machine-info` `PRETTY_HOSTNAME` or `hostnamectl`; fallback hostname. Path: `apps/server/src/environment/ServerEnvironmentLabel.ts`. **S.**

**C4. Machine kind detection.**
- Values: `server`, `cloud`, `linux`, `desktop`, `laptop`, `mac-mini`, `mac-studio`.
- Sources: Apple product name or `sysctl hw.model`; Linux DMI (`/sys/class/dmi/id/chassis_type`, vendor, product); VM/cloud vendor heuristics; WSL via `/proc/sys/kernel/osrelease`.
- Path: `apps/server/src/environment/ServerEnvironmentMachine.ts`. **S.**

**C5. Direct endpoints (learned routes).**
- Enumerates private IPv4 addresses on non-virtual interfaces. Skips docker, br-, veth, virbr, vmnet, vboxnet, vEthernet, podman, cni, flannel, cali, lxcbr, lxdbr and bridge1xx; skips 127/8 and 169.254/16.
- Tags `tailnet` when the interface is Tailscale's (`tailscale*`, or a utun carrying a Tailscale IPv6 address).
- Adds the Tailscale Serve HTTPS URL only after it answers `/.well-known/t3/environment` (3-second budget).
- Exposed in `ServerConfig.directEndpoints`.
- Paths: `apps/server/src/environment/DirectEndpoints.ts`, `packages/shared/src/hostClassification.ts`. **M.**

**C6. Remote open targets.** SSH targets for "open in remote editor": checks an sshd listener on 127.0.0.1/::1:22, the Tailscale MagicDNS name (or Tailscale SSH prefs when no sshd), and `<shorthost>.local`. Path: `apps/server/src/environment/RemoteOpenTargets.ts`. **S.**

**C7. Advertised endpoint model.** Types only, used by desktop/clients: provider kind, reachability, hosted-HTTPS compatibility. Path: `packages/contracts/src/remoteAccess.ts`. **S.**

### D. T3 Connect (Clerk + relay) — environment side

**D1. Public cloud config.**
- Build-time or env values: `T3CODE_RELAY_URL` (HTTPS only), `T3CODE_CLERK_PUBLISHABLE_KEY` (the Clerk frontend API is derived from it), `T3CODE_CLERK_CLI_OAUTH_CLIENT_ID`, `T3CODE_HOSTED_APP_URL` (default `https://app.t3.codes`), and relay-client OTLP tracing (`T3CODE_RELAY_CLIENT_OTLP_TRACES_URL|DATASET|TOKEN`).
- `hasCloudPublicConfig` gates the `t3 connect` command.
- Paths: `apps/server/src/cloud/publicConfig.ts`, `apps/server/src/cloud/config.ts`. **S.**

**D2. CLI OAuth with Clerk (loopback PKCE and device grant).**
- Loopback flow: listener on 127.0.0.1:34338 `/callback`, PKCE + state. Opens the hosted `/connect#state=&challenge=&port=` page, which then goes to Clerk's authorize endpoint. 10-minute timeout.
- Headless/SSH flow: OAuth device authorization grant (`urn:ietf:params:oauth:grant-type:device_code`), 5-second default interval, +5 seconds on `slow_down`.
- Tokens persist to secret `cloud-cli-oauth-token` as JSON with refresh token and identity claims. Refreshed 5 minutes early; 10-second request timeout; serialized by a semaphore.
- Scopes: `openid profile email offline_access`.
- Paths: `apps/server/src/cloud/CliTokenManager.ts`, `packages/shared/src/connectAuth.ts`, `apps/server/src/cloud/cliAuthHtml.ts`. **L.**

**D3. Environment Ed25519 key pair.** Secret `cloud-link-ed25519-key-pair` (JSON of PEM pkcs8/spki), migrated from the older `-private-key`/`-public-key` pair; created exclusively to avoid races. Path: `apps/server/src/cloud/environmentKeys.ts`. **S.**

**D4. Relay JWT family.**
- EdDSA with fixed `typ` values: `t3-env-link+jwt`, `t3-cloud-mint+jwt`, `t3-cloud-health+jwt`, `t3-env-mint+jwt`, `t3-env-health+jwt`, `t3-env-activity+jwt`, `t3-env-managed-tunnel-recovery+jwt`, `t3-relay-hook-delivery+jwt`.
- Issuer `t3-env:<environmentId>`; audience is the normalized relay issuer. Verification uses `maxTokenAge` 5 minutes and 60-second clock tolerance; cloud proofs also need lifetime ≤ 300 seconds and an exact single scope.
- Paths: `packages/shared/src/relayJwt.ts`, `apps/server/src/cloud/linkChecks.ts`. **M.**

**D5. Link proof (`POST /api/connect/link-proof`).**
- Signs `{challenge, descriptor, environmentId, environmentPublicKey, endpoint, origin, scopes}`, 5-minute expiry.
- Scopes: `agent_activity_notifications`, plus `managed_tunnels` for the `cloudflare_tunnel` provider.
- The request must reach the server directly on a loopback origin whose port matches, and must not carry `X-Forwarded-Host`/`X-Forwarded-Proto`.
- Paths: `CloudLink.ts` (`linkProof`, `makeCloudLinkProof`), `linkChecks.ts`. **M.**

**D6. Apply relay config (`POST /api/connect/relay-config`).**
- Validates the payload, that the cloud user is the same linked user (otherwise `CloudLinkAccountMismatchError`), and the mint public key.
- Stops the connector, then writes secrets in a crash-safe order: `cloud-relay-url`, `cloud-relay-issuer`, `cloud-linked-user-id`, `cloud-relay-environment-credential`, `cloud-mint-ed25519-public-key`, then `cloud-endpoint-runtime-config` last. Clears `cloud-link-revoked`.
- Then registers tunnel recovery and starts cloudflared. Can return 503 with the runtime status.
- Requests an agent-awareness catch-up.
- Path: `CloudLink.ts` (`applyCloudRelayConfig`, `applyRelayConfig`). **L.**

**D7. Link state, unlink and preferences.**
- Link state: `{linked, cloudUserId, relayUrl, relayIssuer, managedTunnelActive, publishAgentActivity, holdWebhooksWhileOffline, linkRevoked}`.
- Unlink removes all cloud secrets and clears the desired CLI link.
- Preferences: an uninterruptible, rollback-capable update of `cloud-publish-agent-activity` and `cloud-hold-webhooks-while-offline`. The hold flag is pushed to relay `POST /v1/environments/:id/link-preferences`.
- Path: `CloudLink.ts`. **M.**

**D8. Relay health answer (`/api/t3-connect/health`).** Verifies the relay-signed health JWT (scope `environment:status`, `sub` equals the linked user). jti and nonce replay guards are files in the secret store. Returns a signed health response with descriptor and `requestNonce`. Path: `CloudLink.ts` (`answerHealthRequest`). **M.**

**D9. Mint credential (`/api/connect/mint-credential`, `/api/t3-connect/mint-credential`).** Verifies the relay mint JWT (scope `environment:connect`, `cnf.jkt` equal to `clientProofKeyThumbprint`). Issues a pairing link with subject `cloud-connect`, 2-minute TTL, bound to the client DPoP key. Returns `{credential, expiresAt, proof}` with the response signed and bound to nonce and key. Path: `CloudLink.ts` (`mintCredential`). **M.**

**D10. Desired CLI link and startup reconcile.**
- Secret `cloud-cli-desired-link` holds `managed` or `publish_only`.
- At startup, after activation:
  1. Read the desired mode.
  2. Start the stored tunnel if the confirmed-origin marker matches.
  3. Otherwise wait a jitter of up to 30 seconds and register recovery, retrying for up to 10 minutes, then start the unconfirmed stored config.
  4. If not linked and a link is desired, reconcile: relay `POST /v1/client/environment-link-challenges`, sign a proof, `POST /v1/client/environment-links` (2-minute timeout), then apply config.
- Paths: `apps/server/src/server.ts` (`layerCloudDesiredLinkReconcile`), `apps/server/src/cloud/managedTunnelStartup.ts`, `apps/server/src/cloud/CliState.ts`, `CloudLink.reconcileDesiredLink*`. **L.**

**D11. Managed tunnel recovery.**
- Register: `POST /v1/environments/:id/tunnel/recovery` with a signed `register` proof (60-second expiry) carrying tunnelId and loopback origin. Response is `ready` or `recovery_required`.
- Recover: `POST /v1/environments/:id/tunnel` with a `recover` proof, which returns a new runtime config.
- A 401 marks `cloud-link-revoked`. The recovery cooldown is 2 minutes. Recovery requests are triggered by the runtime (connector rejections or no registration).
- Paths: `CloudLink.ts`, `server.ts`. **L.**

**D12. Release managed tunnel on shutdown.**
- Only for CLI-desired `managed` links: CLI bearer `DELETE /v1/client/environment-links/:id/tunnel`.
- Skipped when a desktop update restart marker (`<base>/runtime/desktop-update-restart`, under 1 minute old, consumed on read) or a pending service-update handoff exists. A handoff is a pending update in `service-state.json` with no `.service-stopping` marker.
- On success, drops the stored runtime config.
- Paths: `CloudLink.releaseManagedTunnelOnShutdown`, `apps/server/src/cloud/updateHandoff.ts`. **M.**

**D13. cloudflared connector runtime.**
- Spawns `cloudflared tunnel --no-autoupdate --loglevel info --output default --metrics 127.0.0.1:0 run` with env `TUNNEL_TOKEN`.
- Classifies output lines (`Registered tunnel connection` means connected; ERR/WRN/FTL/PNC are warnings) and detects rejections.
- Crash backoff 1 second to 60 seconds; reset after 30 seconds of stable uptime.
- Requests recovery after 4 authorization failures, or if not registered within 3 minutes.
- Readiness check via the metrics server every 30 seconds; restart after 6 not-ready checks.
- Zero-downtime swap to the pinned version (start the new connector, stop the old after registration); background install retried at most every 10 minutes.
- `tunnelConnected` stream; link-state lock semaphore.
- Path: `apps/server/src/cloud/ManagedEndpointRuntime.ts`. **XL.**

**D14. Relay client (cloudflared) install manager.**
- Pinned `CLOUDFLARED_VERSION` 2026.10.0 with per-platform GitHub URLs and sha256 values; minimum `2025.6.1`.
- Sources: `T3CODE_CLOUDFLARED_PATH` override, then PATH, then managed `<base>/tools/cloudflared/<ver>/<plat>-<arch>/cloudflared`.
- Version probe via `cloudflared version`, cached by size and mtime.
- Install lock, staged download with a 10-minute timeout, checksum, tgz extraction on macOS, validation.
- Prune older managed releases except the newest older one.
- Status values: `available`, `missing`, `unsupported`.
- Paths: `packages/shared/src/relayClient.ts`, `packages/contracts/src/relayClient.ts`; RPCs `cloud.getRelayClientStatus` and `cloud.installRelayClient`. **L.**

**D15. Agent awareness publishing (mobile push / Live Activities).**
- Subscribes to orchestration domain events; a filter decides which event types matter.
- Derives a per-thread `RelayAgentActivityState`:
  - phases `starting`, `running`, `waiting_for_approval`, `waiting_for_input`, `completed`, `failed`, `stale`;
  - detail up to 160 characters, and failure detail redacted;
  - built with `projectThreadAwarenessV2` in `packages/shared/src/agentAwareness.ts`.
- Signs an activity JWT and calls relay `POST /v1/environments/:id/threads/:threadId/agent-activity` with the environment bearer credential.
- Retries at 1, 2, 4, 8 and 16 seconds; dedupes published state; supports catch-up requests; handles subagent threads specially.
- Gated by `cloud-publish-agent-activity == "true"` and the presence of relay credentials.
- The relay itself does APNs/FCM; the environment never talks to mobile devices.
- Paths: `apps/server/src/relay/AgentAwarenessRelay.ts`, `packages/contracts/src/relay.ts`. **L.**

**D16. Held-hooks waker.** On each `tunnelConnected` (3-second debounce), if hold-webhooks is enabled, calls relay `POST /v1/environments/:id/hooks/wake`; 10-second timeout, retry 3 times. Path: `apps/server/src/relay/HeldHooksWaker.ts`. **S.**

**D17. Webhooks automation and relay forwarding.**
- Public webhook URL: when linked with a managed tunnel it is `<relayUrl>/v1/hooks/<endpointKey>`, where `endpointKey` is the last `-` segment of `tunnelName` (16 hex characters). Otherwise the direct `/api/hooks/:hookId/:token`.
- Environment route handling:
  - 1 MiB body cap; HMAC-SHA256 signature verification (configurable header, prefix and hex/base64 encoding; `webhookVerification.ts`).
  - Rate limit 60 per minute per task; queue up to 20.
  - Outcome header `x-t3-hook-outcome`.
  - Status codes: 202, 404, 401, 409, 429, 410, 500.
- Relay delivery proof: header `x-t3-relay-delivery` (JWT signed with the relay mint key, max age 25 hours) must match `x-t3-relay-delivery-id`, `x-t3-relay-received-at` and the hookId. Only then are the relay delivery id (dedupe), received-at and traceparent trusted.
- Paths: `apps/server/src/scheduledTasks/webhookRoute.ts`, `RelayDeliveryProof.ts`, `ScheduledTaskService.ts` (`relayHookBaseUrl`), `webhookVerification.ts`. **L** (route + verification **M**).

**D18. Relay response classification and tracing.**
- HTTP status maps to `unauthorized`, `forbidden`, `rejected` (4xx except 408/429) or `unavailable`.
- Relay-client OTLP tracer: `environment.relay.request` spans exported only for verified `cloud-connect` sessions; relay traceparent honored.
- Paths: `apps/server/src/cloud/relayResponse.ts`, `relayTracing.ts`, `traceRelayRequest.ts`, `packages/shared/src/relayTracing.ts`. **M** (only needed for observability parity).

**D19. Relay API contract** (for reference; implemented in `infra/relay`, not the server). Full RelayApi in `packages/contracts/src/relay.ts`:
- `/health`, `/.well-known/oauth-*`
- `/v1/mobile/devices` (POST, DELETE `:deviceId`), `/v1/mobile/live-activities`, `/v1/mobile/agent-activity`
- `/v1/environments`, `/v1/client/devices`, `/v2/client/devices`, `/v1/client/environment-links` (POST, DELETE `:id`, DELETE `:id/tunnel`), `/v1/client/environment-link-challenges`
- `/v1/client/dpop-token`
- `/v1/environments/:id/connect`, `/status`, `/tunnel/recovery`, `/tunnel`, `/threads/:threadId/agent-activity`, `/link-preferences`, `/hooks/wake`
- `/v1/hooks/:endpointKey/:hookId/:token` (POST, PUT, PATCH, GET)

The environment server only calls the client-link endpoints (CLI bearer) and the "server group" (environment bearer).

### E. Tailscale integration

**E1. Tailscale CLI wrapper.**
- `tailscale status --json` (3.5-second timeout): MagicDNS name and IPv4 check (100.64/10).
- `tailscale debug prefs` for the SSH-enabled flag.
- `tailscale serve --bg --https=<port> http://127.0.0.1:<localPort>` and `tailscale serve --https=<port> off`.
- HTTPS base URL builder (default port 443); probe of `/.well-known/t3/environment`; stderr diagnostics classification.
- Path: `packages/tailscale/src/tailscale.ts`. **M.**

**E2. `--tailscale-serve` server mode.** After activation, configures Serve for the bound port and disables it on shutdown (acquire/release); failures are logged, not fatal. Env vars `T3CODE_TAILSCALE_SERVE` and `T3CODE_TAILSCALE_SERVE_PORT`. Path: `apps/server/src/server.ts` (`layerTailscaleServe`). **S.**

**E3. `t3 pair --tailscale`.** Ensures Serve fronts this server, persistently (errors include another environment already on the port, or a non-T3 service occupying it). Probes up to 5 times at 1-second intervals for cert provisioning, then prints the tailnet pairing URL. Path: `apps/server/src/cli/pair.ts`. **M.**

### F. SSH remote launch and tunnel (desktop-driven, shared package)

**F1. SSH environment manager.**
- Resolves the target via `ssh -G` and discovers hosts from ssh config and known_hosts.
- Launches or reuses a remote server: remote shell script downloads `t3-<ver>-<plat>-<arch>.tar.gz` plus `SHA256SUMS` from the release base (curl or wget), verifies with sha256sum or shasum, installs under `~/.t3/runtime/versions/<ver>` with an install lock, and keeps per-connection state in `~/.t3/ssh-launch/<stateKey>`.
- Picks a port and waits for readiness; issues a remote pairing token over SSH; opens `ssh -L` forwards; supports askpass password prompts.
- Stops the remote server only if this launcher started it ("managed" vs "external").
- Node-path hydration covers volta, asdf, mise, fnm, nodenv and nvm.
- Paths: `packages/ssh/src/{tunnel,command,config,auth,errors}.ts`, IPC types in `packages/contracts/src/ipc.ts` (`DesktopSshEnvironment*`). **XL** (if Rust must also host this; today it lives in desktop main).

**F2. `t3 __ssh-helper` subcommands** (server CLI side of F1):
- `pick-port <port-file> <default-port> <scan-window>`: bind test plus a loopback-answer check.
- `wait-ready <port> <timeout-ms> <probe-timeout-ms>`.
- `runtime-port <runtime-file>`: reads `server-runtime.json`.
- Path: `apps/server/src/cli/sshHelper.ts`. **S.**

### G. Devices (simulators and emulators)

**G1. Device service and hosts.**
- Local host: macOS needs Xcode/xcrun; Android needs the SDK detected from `ANDROID_HOME` or standard locations with adb, emulator and cmdline-tools.
- SSH device hosts: `ssh -T -L` forwards of the hub and daemon ports with `ControlMaster=no`, `ServerAliveInterval=10`, `ServerAliveCountMax=3`; remote script and environment; Node 22+ required.
- The host id is carried on every request.
- Operations: list, configure (settings `enableDeviceSupport`, `enableAgentDeviceAccess`, `deviceHosts`, `deviceOnboardingCompleted`), test host, open, close, shutdown (`/api/devices/shutdown`), detail, screenshot, readiness, state subscription.
- Paths: `apps/server/src/device/{DeviceService,DeviceHost,LocalDeviceHost,SshDeviceHost,sshDeviceScript,localSshDeviceHost}.ts`, `packages/contracts/src/device.ts`. **XL.**

**G2. Device toolchain.** npm-installs pinned `expo-device-hub@0.15.3` and `agent-device@0.21.25` into `<base>/tools/<name>/<ver>` with an `.install-complete` sentinel and an install lock; runs them with the server's Node; supervises the hub as a child bound to loopback; prunes old versions. Paths: `apps/server/src/device/{DeviceToolchain,deviceToolMaintenance,deviceToolVersions.json}`. **M.**

**G3. Device hub proxy (`/api/device-hub/*`).**
- Path allowlist for HTTP and WebSocket (serve-sim / serve-emu streams, config, screenshot, accessibility, event-log, foreground, appstate, fold).
- Non-GET methods only on mutable paths; operate scope for input/tuning, read scope otherwise.
- Strips cookie, auth, dpop, x-forwarded-proto, ws headers and `wsTicket`/`hostId`; rewrites Origin.
- Sets `cache-control: no-store, no-transform`; bidirectional WebSocket frame pump.
- Path: `apps/server/src/device/DeviceHubProxy.ts`. **M.**

**G4. Device actions (`device.action`).** Typed actions executed with simctl, adb and serve-sim helpers, never via the hub's exec route: `setAppearance`, `setTextSize`, `setToggle` (reduceMotion, increaseContrast, reduceTransparency, showBorders, voiceOver, networkEnabled), `setLiquidGlass`, `setColorFilter`, `setOrientation`, `setLocation`, `clearLocation`, `setPermission`, `openUrl`, `launchApp`, `terminateApp`, `shake`, `sendPush`. Each returns settings read back from the device. Path: `apps/server/src/device/DeviceActions.ts`. **L.**

**G5. Agent-device CLI shim.** Writes `<stateDir>/device/bin/agent-device` (sh) or `.cmd`, plus `agent-device-launcher.mjs`. It enforces `--config`/`--session`, sets `ELECTRON_RUN_AS_NODE`, and the directory is prepended to the provider PATH. Paths: `apps/server/src/device/AgentDeviceShim.ts`, `AgentDeviceTarget.ts`. **S.**

### H. Background service, launcher self-update, and desktop update

**H1. Boot service install (systemd user unit or launchd agent).**
- systemd unit `~/.config/systemd/user/t3code.service`:
  - `ExecStart=<runtime>/t3 __service-launcher`, `Environment=T3CODE_HOME=…`, `T3_BOOT_SERVICE_UNIT=t3code.service`
  - `KillMode=mixed`, `OOMPolicy=continue`, `Restart=always`, `RestartSec=5`, `CPUWeight=1000`, `IOWeight=1000`, `ManagedOOMPreference=avoid`, `StartLimitIntervalSec=300`, `StartLimitBurst=5`
  - log appended to `<logs>/boot-service.log`
  - commands: `systemctl --user daemon-reload|enable|restart|disable --now`
- launchd plist `~/Library/LaunchAgents/com.t3tools.t3code.service.plist`: RunAtLoad, KeepAlive, ThrottleInterval 5, ExitTimeOut 90, ProcessType Interactive, PATH captured at install. Commands: `launchctl enable`, `bootstrap gui/<uid>`, `bootout --wait`.
- Linux prerequisites: `systemctl --user show-environment`; `loginctl show-user --property=Linger` with auto `enable-linger`.
- Status problems: `user-manager-unavailable`, `linger-unavailable`, `linger-disabled`, `service-disabled`, `service-stopped`, `restart-pending`.
- Downgrade refusal; durable fsync'd writes; `withStopped(op)` for doctor; 120-second stop timeout. Windows is unsupported.
- Path: `apps/server/src/cloud/bootService.ts`. **L.**

**H2. Service launcher (stable supervisor).** A standalone Node script, no Effect. Protocol 3. Key behavior:
- **State and runtimes.** Reads `<base>/runtime/service-state.json` (`{protocol, activeVersion, update?}`, validated strictly). Spawns `<base>/runtime/versions/<ver>/t3 serve` with env `T3_SERVICE_LAUNCHER_CONTEXT` (JSON) and an IPC channel. A runtime counts as present only if `.install-complete` matches the version.
- **Update request** (child to launcher `request-update{targetVersion, dbPath}`). Accepted only when all of these hold: the requester is the active child; nothing is pending; the target is an exact newer semver; dbPath is absolute; the runtime exists; and the disk-space check passes. The space check allows a clone backup (APFS `cp -c` or Linux `--reflink`) with headroom max(1 GiB, 10%), otherwise needs 1.05× the DB size.
- **Pending and trial.** The pending update (UUID) is written durably before replying `update-accepted`. After 2 seconds the old child is stopped (SIGTERM, then SIGKILL after 5 seconds). The DB, WAL and SHM are snapshotted once per update into `runtime/db-backup/<id>/` (staging dir, fsync, rename, dir fsync). The trial starts with a 30-minute prepared deadline.
- **Commit.** The trial sends `prepared{updateId}`; the launcher durably commits `activeVersion`, replies `committed` and discards the backup.
- **Rollback.** Triggers: trial exit, timeout, invalid prepared, backup failure, start failure or missing runtime. Steps: a durable `.restore-pending` marker, restore through symlinks, fsync, then the terminal state `rolled-back|failed` with a reason, then restart the old version. An interrupted restore finishes on the next launcher start.
- **Exit code 78** (state dir owned by another server): idle forever and mark a pending update `failed: state-dir-owned` without restoring.
- **Markers.** Explicit stop writes `runtime/.service-stopping` synchronously at signal receipt. `runtime/.restart-pending` (written by `t3 update`) is cleared when the awaited version starts.
- Paths: `apps/server/src/serviceLauncher.ts`, `apps/server/src/cloud/serviceProtocol.ts`, `apps/server/src/cli/serviceLauncher.ts`. **XL.**

**H3. Launcher client (in the server).** Detects launcher mode from `T3_SERVICE_LAUNCHER_CONTEXT` plus the IPC channel. `requestUpdate` has a 30-second timeout. `prepareTrial` sends `prepared` and awaits `committed`; it is called during startup right before welcome/ready, and the `updateOutcome` is surfaced in the lifecycle `ready` event. Path: `apps/server/src/cloud/serviceLauncherClient.ts`. **M.**

**H4. Pinned runtime install.**
- Downloads `t3-<ver>-<platformKey>.{tar.gz|zip}` plus `SHA256SUMS` from `https://github.com/pingdotgg/t3code/releases/download/v<ver>` (overridable with `T3CODE_RELEASE_BASE_URL`).
- Platform keys: darwin-arm64, linux-arm64, linux-x64, win32-arm64, win32-x64.
- Verifies sha256, extracts with `tar -xf --strip-components=1` (Windows uses System32 tar.exe), validates, writes the sentinel, then atomically renames the staging dir into `versions/<ver>`. 10-minute timeout.
- Paths: `apps/server/src/cloud/pinnedRuntime.ts`, `packages/shared/src/cliRelease.ts`. **M.**

**H5. Service preflight.** `t3 __service-preflight --database-path --launcher-protocol` prints JSON `{status: ready|blocked, version, launcherProtocol|reason}`. Run against the staged runtime before handing off, with a 30-second timeout. Paths: `apps/server/src/cloud/servicePreflight.ts`, `apps/server/src/cli/servicePreflight.ts`. **S.**

**H6. Server self-update RPC orchestration.**
- Capability is `desktop-managed` (desktop mode), `boot-service` (launcher-managed) or none.
- Flow: in-flight guard, then the `downloading` stage (install runtime + preflight), then `installing` (launcher `requestUpdate`, uninterruptible), returning `{targetVersion, method:"boot-service", updateId}`.
- With `continueRunningThreads`, restart-continuation threads are prepared at the `installing` stage and cleared on failure.
- The progress stream is detached, so a dropped socket does not abort the update.
- Paths: `apps/server/src/cloud/selfUpdate.ts`, `docs/internals/server-updates.md`. **M.**

**H7. Desktop app update handoff (two-phase).**
- The server talks to desktop main over the desktop telemetry control fd (newline-delimited JSON).
- Server to desktop: `requestDesktopUpdate{requestId}`, commit, cancel, `setDiagnosticsDemand`, `setHostPowerIntervals`.
- Desktop to server: `desktopUpdateStatus{requestId, outcome: ready-to-install|up-to-date|failed, state}`.
- Phase 1 (`server.updateServer`) returns `method:"desktop-app", desktopUpdateToken`; 20-minute timeout.
- Phase 2 (`server.commitDesktopUpdate{requestId}`) only returns if install fails or times out (2 minutes). On success the backend is killed.
- Paths: `apps/server/src/desktopUpdate/DesktopAppUpdate.ts`, `apps/server/src/resourceTelemetry/DesktopTelemetryReceiver.ts`, `packages/contracts/src/resourceTelemetry.ts`. **M.**

**H8. Installation kind detection.** Detects `npx`, `pnpm dlx`, `bunx` or `npm-global{prefix}` from the executable path so clients can show manual update commands (`serverInstallation` capability). Path: `apps/server/src/cli/invocation.ts`. **S.**

### I. Server lifecycle, startup and process plumbing

**I1. Single-server ownership lock.**
- `BEGIN EXCLUSIVE` on `<stateDir>/server-owner.sqlite` with busy_timeout 0, held for the process lifetime; the OS releases it on crash.
- A legacy `server-runtime.json` owner counts as live if the pid is alive and its start time from `ps -o lstart=` (or PowerShell) is not newer than the record.
- Error `ServerAlreadyRunningError` exits with code 78.
- Path: `apps/server/src/serverOwnership.ts`. **S.**

**I2. Runtime state file.** `server-runtime.json` (`{version:1, pid, ownerId, host, port, origin, devUrl?, startedAt, serviceManaged?, serverVersion}`), written atomically after activation and cleared on exit by its owner. Used by `t3 pair`, `t3 update`, `t3 project`, `connect` and `__ssh-helper`. Path: `apps/server/src/serverRuntimeState.ts`. **S.**

**I3. Activation gate and parked fibers.**
- Long-running roots park until activation (`forkParked`).
- The startup sequence:
  1. keybindings and settings start;
  2. legacy shell import, recovery, delegated-task recovery, effect-worker + relay start, optional auto-bootstrap;
  3. auto-pull projects (parked);
  4. HTTP listening;
  5. auxiliary roots parked;
  6. launcher `prepareTrial`;
  7. publish `welcome`;
  8. activate, then the command gate opens, then publish `ready`, then flush the compile cache.
- Failure aborts activation.
- Paths: `apps/server/src/serverActivation.ts`, `apps/server/src/serverRuntimeStartup.ts`, `server.ts`. **L** (ordering is subtle and matters for the update commit boundary).

**I4. Lifecycle events service.** Sequence-numbered `welcome`, `ready` and `legacyThreadMigration{running|complete,totalThreadCount}`, keeping the latest event per type for replay. Paths: `apps/server/src/serverLifecycleEvents.ts`, `packages/contracts/src/server.ts`. **S.**

**I5. Bootstrap envelope over fd.**
- `--bootstrap-fd` / `T3CODE_BOOTSTRAP_FD`: read one JSON line (`DesktopBackendBootstrap`) within 1 second. Uses `/dev/fd/N` or `/proc/self/fd/N` when available; EBADF/ENOENT is ignored.
- Fields: mode, noBrowser, port, t3Home, host, `desktopBootstrapToken`, `desktopBootstrapSecret`, `shellEnvironmentPrepared`, `tailscaleServe*`, OTLP URLs, telemetry/browser fds, `resourceMonitorPath`.
- Paths: `apps/server/src/bootstrap.ts`, `packages/contracts/src/desktopBootstrap.ts`. **S.**

**I6. Launcher-loss watch (desktop).** Polls `ppid` every 2 seconds and shuts down when the parent changes; not on Windows or when started under init. Path: `apps/server/src/launcherWatch.ts`. **S.**

**I7. HTTP serving guard.** 503 with `Retry-After` until handlers are attached; write-error guards; router `maxParamLength` 512; custom graceful-shutdown grace. Paths: `apps/server/src/httpResponseErrorGuard.ts`, `server.ts`. **S.**

**I8. Server config resolution.**
- Precedence: flags, then env, then bootstrap envelope, then persisted settings.
- Env vars:
  - `T3CODE_MODE|PORT|HOST|HOME|NO_BROWSER|BOOTSTRAP_FD|AUTO_BOOTSTRAP_PROJECT_FROM_CWD|LOG_WS_EVENTS|TAILSCALE_SERVE|TAILSCALE_SERVE_PORT`
  - `VITE_DEV_SERVER_URL`, `T3CODE_DEV_ALLOWED_ORIGINS`, `T3CODE_DEV_AUTH_TOKEN`
  - `T3CODE_LOG_LEVEL`, `T3CODE_TRACE_*`, `T3CODE_OTLP_*`
- Base dir defaults to `~/.t3` with `~` expansion. `rejectRunningServer` checks for an already-running server.
- Paths: `apps/server/src/config.ts`, `apps/server/src/cli/config.ts`, `apps/server/src/os-jank.ts`. **M.**

**I9. PATH/HOME hydration.** Login-shell PATH merge on macOS/Linux (keeps mise diff); Windows registry/PowerShell environment repair; skipped when the desktop already prepared it. Path: `apps/server/src/os-jank.ts` (`fixPath`). **M.**

**I10. Outbound HTTP hardening.** Forces undici onto HTTP/1.1 when Node bundles undici 8+ (keeps env proxy); Happy-Eyeballs attempt timeout of at least 1 second. Path: `apps/server/src/outboundHttp.ts`. **S** (in Rust: reqwest/hyper configuration).

**I11. Compile cache flush, entrypoint detection and ACP fast path.** `bin.ts` dispatches `acp-mcp-bridge`/`acp-mcp-call` before loading the CLI; `entrypoint.ts` handles symlinks; `compileCache.ts` flushes the Node compile cache. Paths: `apps/server/src/{bin,binCli,entrypoint,compileCache}.ts`. **S** (largely Node-specific; the Rust equivalent is trivial).

**I12. Process runner.** Spawn with timeout, stdin, max output bytes and env; typed errors (spawn, stdin, output limit, read, timeout); Windows command-not-found detection. Path: `apps/server/src/processRunner.ts`. **S.**

**I13. Heap snapshot on SIGUSR2.** Writes `<logs>/server-<pid>-<ts>.heapsnapshot`. Path: `apps/server/src/observability/HeapSnapshot.ts`. **S** (N/A for Rust).

### J. Settings, secrets, keybindings, themes

**J1. Server settings service.**
- `settings.json`: decode with defaults; legacy provider and source-control migrations; atomic writes; a file watcher (including symlink target) with 100 ms debounce; change stream.
- A file that fails to decode turns `continueThreadsAfterServerUpdate` off.
- Secrets are split out of settings into the secret store and redacted as `••••••` for clients.
- Provider-instance mutations are supported.
- Notable remote/device fields: `deviceHosts`, `enableDeviceSupport`, `enableAgentDeviceAccess`, `continueThreadsAfterServerUpdate`, `backgroundActivity*`, `storageCleanup`, `observability`, `environmentIcon`, `defaultTheme`.
- Paths: `apps/server/src/serverSettings.ts`, `apps/server/src/settingsSecrets.ts`, `packages/contracts/src/settings.ts`. **L.**

**J2. Secret store.**
- Directory `<stateDir>/secrets`, mode 0700; each secret is `<name>.bin`, mode 0600.
- Operations: get, set (temp file + rename), create (exclusive; the "already exists" error is used for replay guards), getOrCreateRandom, remove.
- Secret name inventory:
  - `server-signing-key`, `mcp-oauth-signing-key`
  - `cloud-link-ed25519-key-pair` (legacy `cloud-link-ed25519-private-key`/`-public-key`), `cloud-mint-ed25519-public-key`
  - `cloud-endpoint-runtime-config`, `cloud-endpoint-confirmed-origin`, `cloud-linked-user-id`, `cloud-relay-url`, `cloud-relay-issuer`, `cloud-relay-environment-credential`
  - `cloud-publish-agent-activity`, `cloud-hold-webhooks-while-offline`, `cloud-link-revoked`, `cloud-cli-desired-link`, `cloud-cli-oauth-token`
  - `dpop-proof-*`, `cloud-{mint,health}-{nonce,jti}-*`
  - `secret-request-salt`, `secret-request-<32hex>`
  - `provider-config-*`, `provider-env-*`, plus source-control host and usage-limit secrets
- Path: `apps/server/src/auth/ServerSecretStore.ts`. **S.**

**J3. Secret requests (agent asks the user for a secret).**
- `secrets.answerRequest{threadId, turnItemId, answer: save{secret}|decline}`.
- The ref is `secret-ref:` plus the first 32 hex characters of HMAC(salt, `threadId\0turnItemId`). The value is stored as `{projectId, value, savedAt}`.
- One-time consumption, scoped to the project, with a 24-hour TTL and an hourly sweep. Orchestration commands record the status.
- Failure reasons: `load_failed`, `not_found`, `already_answered`, `agent_stopped`, `store_failed`, `record_failed`, `invalid_ref`, `read_failed`, `ref_unavailable`, `ref_expired`, `consume_failed`.
- Paths: `apps/server/src/secrets/SecretRequests.ts`, `packages/contracts/src/secretRequest.ts`. **M.**

**J4. Keybindings.** `keybindings.json`: defaults plus "late defaults" sync on startup, upsert/remove rules, a `when`-expression grammar, limits (256 rules, value length 64, depth 64), file watcher, change stream, config issues reporting. Paths: `apps/server/src/keybindings.ts`, `packages/contracts/src/keybindings.ts`. **M.**

**J5. Environment themes.** `<stateDir>/themes/*.json`. Guarded reads (O_NOFOLLOW; limits of 32 files, 32 KiB each, 192 KiB total); watcher; streamed to opted-in config subscribers; `t3 theme set|clear|show` publishes atomically. Paths: `apps/server/src/environmentTheme.ts`, `apps/server/src/cli/theme.ts`. **M.**

### K. Background policy and host power

**K1. Client activity leases and background policy.**
- Leases per RPC client: up to 16 per client; TTL 45 seconds by default, 120 seconds max.
- Lease fields: visibility, focus, app state, battery, network, and scopes (`server-config`, `provider-status`, `vcs-status`, `git-refs`, `diagnostics`, `thread`).
- Derives `shouldRunOpportunisticWork` and scope demand used by background refreshers.
- Paths: `apps/server/src/background/BackgroundPolicy.ts`, `packages/contracts/src/background.ts`. **M.**

**K2. Host power monitor.** Snapshot fields: idle, locked, suspended, onBattery, lowPowerMode, thermal, stale. Fed by desktop telemetry or `server.reportHostPowerState`; out-of-order reports are ignored and only real changes are emitted. Path: `apps/server/src/background/HostPowerMonitor.ts`. **S.**

### L. Diagnostics

- **L1. Trace diagnostics.** Reads rotated `server.trace.ndjson` files and aggregates span counts, failures, slow spans and recent log events. Shared with `t3 trace summary [--since] [--limit]`. Paths: `apps/server/src/diagnostics/TraceDiagnostics.ts`, `apps/server/src/cli/trace.ts`. **M.**
- **L2. Process diagnostics and signal.** Child process tree listing; `server.signalProcess` sends SIGINT/SIGKILL guarded by pid and start time. Path: `apps/server/src/diagnostics/ProcessDiagnostics.ts`. **M.**
- **L3. Process resource history.** Bucketed CPU/memory history per process, from resource telemetry. Path: `apps/server/src/diagnostics/ProcessResourceMonitor.ts`. **M** (resource telemetry itself is out of slice).
- **L4. Browser OTLP trace proxy.** `/api/observability/v1/traces` records spans locally and forwards to `T3CODE_OTLP_TRACES_URL`. Path: `apps/server/src/http.ts`. **S.**

### M. Storage cleanup, database storage, doctor

- **M1. Storage cleanup.** Hourly plus manual runs. Removes idle or settled worktrees according to settings (keep-when, after N days, on merge, on delete, unchanged), browser artifacts and logs. Report fields: entries, counts, bytesFreed, omittedCount. Exposed via `server.runStorageCleanup` and `server.getStorageCleanupReport`. Paths: `apps/server/src/storageCleanup.ts`, `settings.ts` (`StorageCleanupSettings`, `StorageCleanupReport`). **L.**
- **M2. Database storage status.** Reports databaseBytes, reclaimableBytes, `incrementalVacuumEnabled` and `compactCommand` via `server.getDatabaseStorage`. Contract in `settings.ts` (`DatabaseStorageStatus`); implemented by the `DatabaseStorage` layer wired in `server.ts`. **M.**
- **M3. `t3 doctor [--fix] [--dry-run] [--base-dir]`.** Offline VACUUM to enable incremental auto_vacuum (needs about 2× DB size free), and optional deletion of the legacy pre-0.1.0 `state.sqlite` (never auto-selected). Runs inside `BootService.withStopped`. Path: `apps/server/src/cli/doctor.ts`. **M.**

### N. Process isolation and external launch

- **N1. Agent systemd scopes (Linux).**
  - Agents and terminals run via `systemd-run --user --scope --slice=app-t3code-agents.slice`, with unit names `t3code-<name>-<id>.scope`.
  - Per-state-dir owned slice: `app-t3code-agents-<hash12>.slice`. Leftovers from a previous server are stopped at startup.
  - Memory limits: MemoryHigh = max(total − 6 GiB, total/2); MemoryMax = max(total − 4 GiB, 0.75·total).
  - OOM-kill detection.
  - Path: `apps/server/src/process/agentScope.ts`. **M.**
- **N2. External launcher.** Open browser (`open` / `xdg-open` / Windows), launch editors (installed-editor discovery), reveal in file manager. Path: `apps/server/src/process/externalLauncher.ts`. **M.**

### O. Desktop-integration contracts touching the server

- **O1. Desktop app activation socket (used by `t3 app [path]`).**
  - Unix socket `<tmp>/t3code-<uid>/<sha256-12-bytes(stateDir) hex>.sock`, or named pipe `\\.\pipe\t3code-app-<hash>` on Windows.
  - Protocol: one JSON line `{version:1, requestId, type:"open-workspace", workspaceRoot, platform}`; reply is success `{projectId, threadId}` or failure `{code}`.
  - The CLI is the client; desktop main is the server.
  - Paths: `packages/shared/src/desktopAppControl.ts`, `packages/contracts/src/desktopAppActivation.ts`, `apps/server/src/cli/app.ts`. **S** (client side).
- **O2. Desktop browser channel (fd).** Events `attached`, `detached`, `cdp`; commands `cdp`, `release`, `pointer`. Path: `packages/contracts/src/desktopBrowser.ts`. **M** (only if the Rust server must drive desktop webviews).
- **O3. Desktop telemetry and control fd channel.** NDJSON hello, telemetry snapshots and update status; control messages as in H7. Paths: `apps/server/src/resourceTelemetry/DesktopTelemetryReceiver.ts`, `packages/contracts/src/resourceTelemetry.ts`. **M.**
- **O4. Desktop IPC contracts.** `packages/contracts/src/ipc.ts`: `DesktopEnvironmentBootstrap`, the SSH environment types, saved environment records (with `relayManaged`), server exposure mode `local-only`/`network-accessible`, WSL state. These belong to desktop main, not the server. **N/A for the server.**
- **O5. Browser profiles.** Types only: persistent/incognito, max 24, names up to 48 characters. Path: `packages/contracts/src/browserProfile.ts`. **S.**
- **O6. Application events.** Project created/meta-updated/deleted events plus `OrchestrationClientOrigin` (surface/appVersion recorded per connection). Path: `packages/contracts/src/applicationEvent.ts`. **S** (in this slice).

---

## 5. Complete `t3` CLI command tree

Root definition is in `apps/server/src/binCli.ts`. The root and the server commands share these flags: `--mode web|desktop`, `--port`, `--host`, `--base-dir`, `[cwd]`, `--dev-url`, `--no-browser`, `--bootstrap-fd`, `--auto-bootstrap-project-from-cwd`, `--log-websocket-events`/`--log-ws-events`, `--tailscale-serve`, `--tailscale-serve-port`.

| Command | Purpose | Source |
|---|---|---|
| `t3 [cwd]` | Run the server, print the pairing URL, open a browser | `cli/server.ts` `runDefaultServerCommand` |
| `t3 start` | Same as the default | `cli/server.ts` |
| `t3 serve` | Headless: no browser; prints connection string, token, pairing URL and QR | `cli/server.ts` |
| `t3 help` | Show help | `binCli.ts` |
| `t3 app [path]` | Open a project in the running desktop app via the activation socket | `cli/app.ts` |
| `t3 pair [--scope …] [--ttl] [--label] [--tailscale] [--tailscale-serve-port] [--base-dir]` | Mint a pairing link for the running server (userdata or dev variant), print URL + QR | `cli/pair.ts`, `cli/authScopes.ts` |
| `t3 auth pairing create [--scope …] [--ttl] [--label] [--base-url] [--json] [--base-dir] [--dev-url]` | Issue a one-time pairing token | `cli/auth.ts` |
| `t3 auth pairing list [--json]` | List active pairing tokens (no secrets) | `cli/auth.ts` |
| `t3 auth pairing revoke <id>` | Revoke a pairing token | `cli/auth.ts` |
| `t3 auth session issue [--scope …] [--ttl] [--label] [--subject] [--token-only] [--json]` | Issue a bearer session (default subject `cli-issued-session`, deviceType bot) | `cli/auth.ts` |
| `t3 auth session list [--json]` | List sessions | `cli/auth.ts` |
| `t3 auth session revoke <session-id>` | Revoke a session | `cli/auth.ts` |
| `t3 connect [--headless] [--base-dir]` | Full setup: authorize, acquire relay client (asks before download), set desired link, offer background service install | `cli/connect.ts` |
| `t3 connect login [--headless]` | Authorize only | `cli/connect.ts` |
| `t3 connect link [--headless] [--publish-only]` | Desire a link on next start (managed or publish_only) | `cli/connect.ts` |
| `t3 connect status [--json]` | Persisted state plus relay client status | `cli/connect.ts` |
| `t3 connect publish [--disable]` | Toggle agent-activity publishing | `cli/connect.ts` |
| `t3 connect unlink` | Disable exposure; unlink the live server and the relay environment; keep login | `cli/connect.ts` |
| `t3 connect logout` | Unlink and clear the CLI OAuth token | `cli/connect.ts` |
| `t3 project add <path> [--title]` / `remove <project> [--force]` / `rename <project> <title>` | Project management through the live server HTTP API | `cli/project.ts` |
| `t3 service install [--allow-downgrade] [--base-dir]` | Install/repair the boot service | `cli/service.ts` |
| `t3 service update` | Deprecated (use `t3 update`) | `cli/service.ts` |
| `t3 service restart` | Restart the service | `cli/service.ts` |
| `t3 service uninstall` | Stop and remove the service | `cli/service.ts` |
| `t3 service status` | Status, problems, log path | `cli/service.ts` |
| `t3 update [version] [--channel stable\|nightly\|preview] [--allow-downgrade] [-y\|--yes]` | Download a release, repoint the launcher/service, optionally restart (writes `.restart-pending`); queries the GitHub releases API by channel | `cli/update.ts`, `cli/updateProgress.ts`, `packages/shared/src/cliRelease.ts` |
| `t3 uninstall [-y]` | Remove service, launcher and downloaded versions; keep userdata | `cli/uninstall.ts` |
| `t3 doctor [--fix] [--dry-run]` | DB compaction / legacy DB removal | `cli/doctor.ts` |
| `t3 browser setup [--base-dir]` | Linux: AppArmor profile for Chrome sandbox, apt libraries and fonts | `cli/browser.ts` |
| `t3 theme set <theme> [--id]` / `clear` / `show` | Environment theme defaults | `cli/theme.ts` |
| `t3 trace summary [--since] [--limit]` | Summarize the local trace file | `cli/trace.ts` |
| `t3 triage [--agent claude\|codex] [--model]` | Investigate a problem with an agent CLI and draft an issue | `cli/triage.ts`, `cli/triagePrompt.ts` |
| `t3 acp-mcp-bridge` | Bridge `/mcp` to stdio for ACP agents (fast path in `bin.ts`) | `cli/acpMcpBridge.ts`, `mcp/AcpMcpStdioBridge.ts` |
| `t3 acp-mcp-call <tool> <arguments-json>` | One MCP tool call (fast path) | same |
| `t3 __service-launcher` | Hidden: run the stable launcher | `cli/serviceLauncher.ts` → `serviceLauncher.ts` |
| `t3 __service-preflight --database-path --launcher-protocol` | Hidden: preflight JSON | `cli/servicePreflight.ts` |
| `t3 __ssh-helper pick-port\|wait-ready\|runtime-port` | Hidden: SSH launch helpers | `cli/sshHelper.ts` |
| `t3 __claude-history <method> <session-id> [options]` | Hidden: Claude history worker | `cli/claudeHistory.ts` |

When the build lacks cloud public config, `connect` is replaced by an unlisted stub that shows help with an error. `cli/consoleStdin.ts` attaches stdin for the Windows desktop launcher. **Complexity for the CLI as a whole: L**; each command is S–M on top of the services above.

---

## 6. Complexity summary (rough Rust effort)

- **XL:** Effect RPC-over-WebSocket protocol compatibility; cloudflared connector runtime (D13); service launcher with trial/commit/rollback and DB snapshot (H2); device subsystem as a whole (G1); SSH remote launch (F1, if needed).
- **L:**
  - MCP OAuth (B1); CLI Clerk OAuth (D2); relay config/link/reconcile/recovery (D6, D10, D11); relay client install (D14); agent awareness publishing (D15); webhooks with relay proofs (D17).
  - Boot service (H1); startup ordering and activation (I3); settings service (J1); storage cleanup (M1); device actions (G4); CLI tree.
- **M:**
  - Scopes and RPC authorization (A1, A14); pairing store (A3); session store (A5); cookie auth (A6); token exchange (A7); DPoP (A8); access stream (A10); descriptor (C2); direct endpoints (C5).
  - Relay JWTs and proofs (D4, D5, D7, D8, D9, D12); Tailscale (E1, E3); hub proxy (G3); toolchain (G2); launcher client (H3); pinned runtime (H4); self-update (H6); desktop update (H7); config (I8); PATH hydration (I9).
  - Secret requests (J3); keybindings (J4); themes (J5); background policy (K1); diagnostics (L1–L3); DB storage and doctor (M2, M3); agent scopes (N1); external launcher (N2).
- **S:** the rest, including environment ID, ownership lock, runtime state file, lifecycle events, bootstrap fd, launcher watch, replay pruning, dev token, secret store, CORS, Tailscale Serve toggle, `__ssh-helper`, preflight.

## 7. Traps worth flagging for the port

- Pairing secrets go only in the URL fragment (`/pair#token=…`), never the query.
- DPoP `htu` and MCP issuer URLs are rebuilt from Host and `X-Forwarded-Proto`, so a proxy must preserve both.
- The link-proof endpoint must reject any `X-Forwarded-*` header and accept only loopback origins on the same port.
- Pairing-link consumption must put the scope/proof checks inside the atomic UPDATE.
- Desktop-bootstrap session replacement must happen in one DB transaction with the insert.
- Service-state writes need file fsync plus directory fsync. `service-state.json` validation is strict, and invalid state must stop startup.
- The `.service-stopping` marker must be written synchronously when the stop signal arrives. The managed tunnel must be kept across update handoffs; the desktop marker is valid for 1 minute and consumed on read.
- Every HTTP route, not just `/ws`, is blocked until command readiness.
- The WebSocket session must close when the session is revoked or expires.
- Secret-store "create exclusive" is the replay-guard primitive for both DPoP and relay proofs.