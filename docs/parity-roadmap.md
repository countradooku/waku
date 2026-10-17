# Workflow parity: first increment and remaining work

This independent fork builds on **Waku by egoist and its contributors**.
Upstream Waku remains the foundation; T3 Code is a workflow reference.

The comparison uses Waku commit
[`10bcd728`](https://github.com/egoist/waku/tree/10bcd728c9b2f07d664cfece3c8c870d3418ddd2)
and T3 Code commit
[`812b7006`](https://github.com/pingdotgg/t3code/tree/812b7006f8a12c5c8e55ea65a9cfcebfd328872f).
This is an initial source audit, not an exhaustive specification or a promise
that every T3 Code feature has been implemented.

## Already provided by upstream Waku

- Native Rust/GPUI desktop, authenticated daemon, web and mobile clients.
- Multiple coding-agent integrations and streaming transcripts.
- Branch switching and isolated worktree creation.
- Commit, push, agent-generated commit messages, and checkpoint recovery.
- Review sources for unstaged, staged, uncommitted, committed, branch, and
  last-turn changes.

## Added in this increment

- Structured Git status and linked-worktree inspection in the shared protocol.
- Whole-file staging/unstaging with literal paths and fresh selection validation.
- Clean-tree, fast-forward-only pull with no rebase or autostash.
- Shared repository locks for daemon workspace mutation requests.
- Validated worktree base commits and nested-project checks before creation.
- Ignore-whitespace review with consistent patch and change-count filtering.
- Desktop, web, and mobile controls as documented in
  [source-control.md](source-control.md); their coverage differs.
- Bounded Git output, inherited-pipe timeout handling, and adaptive process
  polling for short commands.

## Remaining work

The requested first priority is Git, worktrees, and review. Before calling that
area complete, compare and implement the remaining behaviors with tests for
failure, cancellation, recovery, and concurrent clients:

1. Worktree reuse, ownership, setup tasks, and explicit cleanup/removal that
   protects active sessions, locks, dirty files, and unpushed commits. T3 Code
   references: `apps/web/src/worktreeCleanup.ts`,
   `packages/client-runtime/src/worktreeSetup.ts`.
2. Hosted pull-request discovery, creation, checkout, checks, reviews, and merge
   workflows, followed by additional supported forges. References:
   `packages/source-control-core/src/server/PullRequestProvider.ts` and the
   `packages/source-control-*` provider implementations.
3. Review conversations, viewed-file tracking, review guidance, and stacks.
   References: `apps/web/src/state/review.ts`,
   `apps/web/src/state/usePullRequestStack.ts`,
   `apps/web/src/reviewCommentContext.ts`.
4. Complete native and mobile coverage for worktree inspection and the richer
   review controls, with interactive keyboard and device validation.

Other T3 Code areas still need a detailed behavior-by-behavior comparison;
they have not been implemented by this increment:

- Scheduling and webhooks (`apps/server/src/scheduling/Scheduler.ts`,
  `apps/server/src/scheduledTasks/webhookRoute.ts`).
- Relay, remote environments, and portable task handoff
  (`apps/web/src/state/relay.ts`, `packages/client-runtime/src/handoff.ts`).
- Additional providers and account modes, including Antigravity
  (`apps/server/src/provider/AntigravityProvider.ts`).
- Prompt stash, snooze, and settle/advance workflows
  (`apps/web/src/promptStashStore.ts`, `apps/web/src/components/Sidebar.snooze.ts`,
  `apps/web/src/settleAndAdvance.ts`).
- MCP and ACP integration details
  (`packages/provider-core/src/server/McpProviderSessions.ts`,
  `packages/provider-acp/src/server/mcpOverAcpBridge.ts`).

These are investigation areas, not assertions that Waku lacks every related
capability. Check upstream again before implementing each item to avoid
duplicating work. Adapt behavior to Waku's daemon boundary and native controls.

## Validation and performance

The focused Rust tests cover literal/special filenames, unborn and detached
branches, indexed renames with later edits, nested repositories, symlink parent
escapes, worktree locks, dirty/divergent pull, whitespace filtering, and process
helpers that retain output pipes. Generated protocol types are checked against
Rust. Native compilation and shared-client/web/mobile type checks complement
the automated suites; interactive app and device validation remains outstanding.

Run the synthetic large-status benchmark with:

```sh
cargo test -p waku-core benchmark_large_status -- --ignored --nocapture
```

On the development Linux machine, ten warm samples over 10,000 untracked files
measured a median of 9 ms and maximum of 10 ms after adaptive polling, compared
with 81 ms and 82 ms using the earlier fixed polling interval. This measures
the daemon status helper in the optimized development profile. It is not an
end-to-end UI benchmark, a cross-platform result, or a comparison against T3
Code. Measure startup, memory, long transcripts, and large diffs separately
before making broader performance claims.
