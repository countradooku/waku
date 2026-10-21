# waku-protocol

`waku-protocol` is Waku's versioned, transport-neutral contract: protocol v8
(RFC 0003) in `v8`, the orchestrator's domain model in `orchestration`, and
the shared data models. It contains no database, provider, filesystem, Git,
attachment, or socket implementation.

The v8 wire types derive a JSON Schema (the `schema` feature), from which
`waku-codegen` generates the checked-in TypeScript client types in
`packages/waku-client/src/v8/generated`:

```sh
bun run protocol:generate
```

CI and local validation detect stale generated files with
`bun run protocol:check`.
