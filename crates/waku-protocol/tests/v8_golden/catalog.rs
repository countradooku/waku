//! Golden samples of the non-orchestration catalog (#53 part 2): the params
//! and result of every method and the items of every stream it adds.
//!
//! Floating-point values are never integral, so the TypeScript codec (which
//! writes integral numbers as integers) re-encodes them byte for byte.

use std::collections::BTreeMap;

use uuid::Uuid;
use waku_protocol::orchestration::ModelSelection;
use waku_protocol::v8::error::VcsFailureKind;
use waku_protocol::v8::*;

const PROJECT: Uuid = Uuid::from_u128(0x9a0e_c700_0000_4000_8000_0000_0000_0003);
const THREAD: Uuid = Uuid::from_u128(0x7e8e_ad00_0000_4000_8000_0000_0000_0004);
const OP: Uuid = Uuid::from_u128(0x0b1d_0000_0000_4000_8000_0000_0000_0010);
const TERMINAL: Uuid = Uuid::from_u128(0x7e2a_0000_0000_4000_8000_0000_0000_0011);
const GENERATION: Uuid = Uuid::from_u128(0x6e4e_0000_0000_4000_8000_0000_0000_0012);
const BLOB: &str = "waku-blob:00f1e2d3c4b5a697.png";
const SHA256: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

/// A method call and its reply, with the call id that routes the reply.
pub struct CallSample {
    pub id: u32,
    pub method: Method,
    pub result: MethodResult,
}

/// A subscription and a batch of its items.
pub struct StreamSample {
    pub channel: u32,
    pub spec: StreamSpec,
    pub after: Option<StreamCursor>,
    pub window: Option<u32>,
    pub items: Vec<StreamItem>,
}

fn project_root() -> RootRef {
    RootRef::Project {
        project_id: PROJECT,
    }
}

fn status() -> GitStatus {
    GitStatus {
        repository: "/Users/dev/waku".into(),
        branch: Some("main".into()),
        head: Some("4a1f2c3".into()),
        upstream: Some("origin/main".into()),
        ahead: 1,
        behind: 0,
        files: vec![
            GitStatusFile {
                path: "src/main.rs".into(),
                previous_path: None,
                index_status: "M".into(),
                worktree_status: " ".into(),
            },
            GitStatusFile {
                path: "src/new.rs".into(),
                previous_path: Some("src/old.rs".into()),
                index_status: "R".into(),
                worktree_status: " ".into(),
            },
        ],
    }
}

fn branches() -> BranchSnapshot {
    BranchSnapshot {
        repository: "/Users/dev/waku".into(),
        current: Some("feature".into()),
        detached_head: None,
        default_branch: Some("main".into()),
        branches: vec![
            BranchEntry {
                name: "feature".into(),
                checked_out_elsewhere: false,
            },
            BranchEntry {
                name: "main".into(),
                checked_out_elsewhere: true,
            },
        ],
        additions: 12,
        deletions: 3,
    }
}

fn model() -> ProviderModel {
    ProviderModel {
        id: "gpt-5".into(),
        name: "GPT-5".into(),
        sub_provider: None,
        is_default: true,
        reasoning_efforts: vec![ModelOption {
            id: "high".into(),
            label: "High".into(),
            description: Some("Thinks longer".into()),
        }],
        default_reasoning_effort: Some("high".into()),
        service_tiers: Vec::new(),
        default_service_tier: None,
        context_windows: Vec::new(),
        default_context_window: None,
    }
}

fn provider_status() -> ProviderStatus {
    ProviderStatus {
        instance_id: "codex".into(),
        kind: "codex".into(),
        display_name: "Codex".into(),
        enabled: true,
        binary_override: Some("/usr/local/bin/codex".into()),
        installed: true,
        path: Some("/usr/local/bin/codex".into()),
        version: Some("0.50.0".into()),
        plan_usage: true,
    }
}

fn plan_usage() -> PlanUsage {
    PlanUsage {
        plan_label: Some("Pro".into()),
        windows: vec![PlanWindow {
            label: "5 hours".into(),
            percent: 42.5,
            resets_at: Some(1_760_003_600_000),
        }],
    }
}

fn draft() -> ComposerDraft {
    ComposerDraft {
        text: "Explain @src/main.rs".into(),
        attachments: vec![DraftAttachment {
            path: "/Users/dev/waku/src/main.rs".into(),
            mention: "@src/main.rs".into(),
            name: "main.rs".into(),
            is_dir: false,
            is_image: false,
            blob_reference: None,
        }],
    }
}

fn drafts() -> ComposerDrafts {
    ComposerDrafts {
        new_threads: BTreeMap::from([(PROJECT, draft())]),
        threads: BTreeMap::new(),
    }
}

fn storage() -> StorageSettings {
    StorageSettings {
        worktrees_directory: Some("/Volumes/fast/worktrees".into()),
        previous_worktrees_directories: vec!["/Users/dev/.waku-fork/worktrees".into()],
        worktree_cleanup: WorktreeCleanupSettings {
            inactive_after_days: Some(30),
            merged: true,
            ..WorktreeCleanupSettings::default()
        },
    }
}

fn report() -> WorktreeCleanupReport {
    WorktreeCleanupReport {
        trigger: CleanupTrigger::Manual,
        started_at: 1_760_000_000_000,
        finished_at: 1_760_000_002_000,
        entries: vec![
            WorktreeCleanupEntry {
                path: "/Volumes/fast/worktrees/waku/fix-1".into(),
                branch: Some("waku/fix-1".into()),
                thread_id: Some(THREAD),
                thread_title: Some("Fix the flaky test".into()),
                outcome: CleanupOutcome::Removed,
                reason: CleanupReason::Merged {
                    base: "main".into(),
                },
                branch_deleted: true,
            },
            WorktreeCleanupEntry {
                path: "/Volumes/fast/worktrees/waku/spike".into(),
                branch: Some("waku/spike".into()),
                thread_id: None,
                thread_title: None,
                outcome: CleanupOutcome::Kept,
                reason: CleanupReason::UncommittedChanges { files: 2 },
                branch_deleted: false,
            },
        ],
        removed: 1,
        kept: 1,
        failed: 0,
        omitted: 0,
    }
}

fn settings() -> Settings {
    Settings {
        computer_use_enabled: false,
        computer_use_allowed_apps: Vec::new(),
        disabled_providers: vec!["grok".into()],
        provider_binary_overrides: BTreeMap::new(),
        storage: storage(),
    }
}

fn terminal_info() -> TerminalInfo {
    TerminalInfo {
        terminal_id: TERMINAL,
        cwd: "/Users/dev/waku".into(),
        title: Some("shell".into()),
        cols: 120,
        rows: 32,
        generation: GENERATION,
        exited: false,
        created_at: 1_760_000_000_000,
    }
}

fn usage_history() -> UsageHistory {
    let part = |provider: &str, cost_usd| UsageProviderPart {
        provider: provider.into(),
        cost_usd,
        total_tokens: 1_200,
    };
    UsageHistory {
        window: UsageWindow::TrailingDays { days: 7 },
        since_day: "2025-10-01".into(),
        until_day: "2025-10-07".into(),
        totals: TokenTotals {
            uncached_input: 1_000,
            cached_input: 500,
            cache_creation: 100,
            output: 300,
            reasoning: 50,
        },
        total_tokens: 1_900,
        cost_usd: 1.25,
        records: 12,
        sessions: 3,
        providers: vec![UsageProviderSlice {
            provider: "codex".into(),
            cost_usd: 1.25,
            total_tokens: 1_900,
            cost_share: 0.5,
            token_share: 0.75,
        }],
        models: vec![UsageModelSlice {
            provider: "codex".into(),
            model: "gpt-5".into(),
            cost_usd: 1.25,
            total_tokens: 1_900,
            cost_share: 0.5,
        }],
        daily: vec![UsageDay {
            day: "2025-10-07".into(),
            cost_usd: 0.75,
            total_tokens: 1_200,
            by_provider: vec![part("claude", 0.25), part("codex", 0.5)],
        }],
        months: vec![UsageMonth {
            first_day: "2025-10-01".into(),
            cost_usd: 1.25,
            total_tokens: 1_900,
            by_provider: vec![part("codex", 1.25)],
            sessions: 3,
            active_days: 2,
            top_models: vec![ModelCost {
                model: "gpt-5".into(),
                cost_usd: 1.25,
            }],
        }],
        projects: vec![UsageProject {
            path: "/Users/dev/waku".into(),
            cost_usd: 1.25,
            total_tokens: 1_900,
            by_provider: vec![part("codex", 1.25)],
            sessions: 3,
            cost_share: 0.5,
            last_day: Some("2025-10-07".into()),
            top_models: Vec::new(),
        }],
        quality: CostQuality {
            provider_reported_share: 0.25,
            model_priced_share: 0.5,
            unpriced_share: 0.25,
            cache_savings_usd: 0.125,
        },
        pricing: PricingStatus::Cached,
        scanned_files: 40,
        skipped_files: 2,
        errors: vec!["~/.codex/sessions/bad.jsonl: unexpected end of input".into()],
        scan_duration_ms: 85,
    }
}

/// Every catalog method, once, with ids from 10.
pub fn calls() -> Vec<CallSample> {
    use MethodResult as R;
    let samples = vec![
        (
            Method::DaemonInfoGet(NoParams {}),
            R::DaemonInfoGet(DaemonInfo {
                version: "0.1.20".into(),
                environment: EnvironmentInfo {
                    id: Uuid::from_u128(0x0e0e_0e0e_0e0e_4e0e_8e0e_0e0e_0e0e_0e0e),
                    name: "studio.local".into(),
                    host_os: "macos".into(),
                    host_arch: "aarch64".into(),
                    store_id: Uuid::from_u128(0x5705_e000_0000_4000_8000_0000_0000_0001),
                },
                features: Vec::new(),
                uptime_ms: 86_400_000,
                default_cwd: "/Users/dev".into(),
                projectless_root: Some("/Users/dev/.waku-fork/scratch".into()),
                home: Some("/Users/dev".into()),
            }),
        ),
        (
            Method::DaemonShutdown(DaemonShutdownParams {
                reason: "app quit".into(),
            }),
            R::DaemonShutdown(Ack {}),
        ),
        (
            Method::FilesTree(FilesTreeParams {
                root: project_root(),
                expanded: vec!["src".into()],
            }),
            R::FilesTree(vec![
                TreeEntry {
                    path: "src".into(),
                    name: "src".into(),
                    is_dir: true,
                    expanded: true,
                    depth: 0,
                },
                TreeEntry {
                    path: "src/main.rs".into(),
                    name: "main.rs".into(),
                    is_dir: false,
                    expanded: false,
                    depth: 1,
                },
            ]),
        ),
        (
            Method::FilesSearchEntries(FilesSearchEntriesParams {
                root: project_root(),
                query: "main".into(),
                limit: Some(50),
            }),
            R::FilesSearchEntries(vec![
                FileEntry {
                    path: "src/".into(),
                    is_dir: true,
                },
                FileEntry {
                    path: "src/main.rs".into(),
                    is_dir: false,
                },
            ]),
        ),
        (
            Method::FilesSearchContents(FilesSearchContentsParams {
                root: RootRef::Thread { thread_id: THREAD },
                query: r"fn \w+".into(),
                regex: true,
                case_sensitive: true,
                limit: None,
            }),
            R::FilesSearchContents(vec![ContentHit {
                path: "src/main.rs".into(),
                line: 1,
                column: 1,
                preview: "fn main() {".into(),
            }]),
        ),
        (
            Method::FilesWrite(FilesWriteParams {
                root: project_root(),
                path: "README.md".into(),
                content: "# waku\n".into(),
                base_revision: Some("sha256:4a1f".into()),
                op_id: OP,
            }),
            R::FilesWrite(FileRevision {
                revision: "sha256:9c2e".into(),
            }),
        ),
        (
            Method::FsBrowse(FsBrowseParams {
                path: Some("/Users/dev".into()),
            }),
            R::FsBrowse(Directory {
                path: "/Users/dev".into(),
                parent: Some("/Users".into()),
                home: "/Users/dev".into(),
                filesystem_root: "/".into(),
                entries: vec![DirectoryEntry {
                    name: "waku".into(),
                    path: "/Users/dev/waku".into(),
                    is_dir: true,
                }],
            }),
        ),
        (
            Method::ComposerSlashCommands(ComposerSlashCommandsParams {
                root: project_root(),
                instance_id: Some("claude".into()),
                thread_id: Some(THREAD),
            }),
            R::ComposerSlashCommands(vec![SlashCommand {
                name: "review".into(),
                description: "Review the diff".into(),
                scope: CommandScope::Project,
                argument_hint: Some("[focus]".into()),
                template: Some("Review $ARGUMENTS".into()),
            }]),
        ),
        (
            Method::SkillsList(SkillsListParams {
                project_ids: vec![PROJECT],
            }),
            R::SkillsList(SkillsCatalog {
                skills: vec![Skill {
                    name: "deploy".into(),
                    description: "Ship it".into(),
                    scope: SkillScope::Project,
                    project: Some("waku".into()),
                    installs: vec![SkillInstall {
                        source: "claude".into(),
                        dir: "/Users/dev/waku/.claude/skills/deploy".into(),
                        skill_file: "/Users/dev/waku/.claude/skills/deploy/SKILL.md".into(),
                        enabled: true,
                    }],
                    enabled: true,
                    allowed_tools: Some("Bash".into()),
                    body: "Run the deploy.".into(),
                    supporting_files: 2,
                    total_bytes: 4_096,
                    modified_at: Some(1_760_000_000_000),
                    duplicates: 0,
                    row_key: 9_007_199_254_740_991,
                }],
            }),
        ),
        (
            Method::SkillsSetEnabled(SkillsSetEnabledParams {
                dirs: vec!["/Users/dev/waku/.claude/skills/deploy".into()],
                enabled: false,
                op_id: OP,
            }),
            R::SkillsSetEnabled(Ack {}),
        ),
        (
            Method::SkillsTrash(SkillsTrashParams {
                dirs: vec!["/Users/dev/.claude/skills/old".into()],
                op_id: OP,
            }),
            R::SkillsTrash(Ack {}),
        ),
        (
            Method::DraftsGet(NoParams {}),
            R::DraftsGet(DraftsSnapshot {
                revision: 77,
                drafts: drafts(),
            }),
        ),
        (
            Method::DraftsApply(DraftsApplyParams {
                base_revision: Some(77),
                changes: vec![
                    DraftChange {
                        target: DraftTarget::NewThread {
                            project_id: PROJECT,
                        },
                        draft: Some(draft()),
                    },
                    DraftChange {
                        target: DraftTarget::Thread { thread_id: THREAD },
                        draft: None,
                    },
                ],
                op_id: OP,
            }),
            R::DraftsApply(DraftsRevision { revision: 78 }),
        ),
        (
            Method::BlobsPut(BlobsPutParams {
                channel: 40,
                size: 300_000,
                mime: "image/png".into(),
                name: Some("screenshot.png".into()),
                sha256: Some(SHA256.into()),
            }),
            R::BlobsPut(BlobPut {
                reference: BLOB.into(),
                size: 300_000,
                sha256: SHA256.into(),
                deduplicated: false,
            }),
        ),
        (
            Method::BlobsStat(BlobsStatParams {
                reference: "waku-attachment:3f2a0000-0000-4000-8000-000000000013".into(),
                path: Some("/Users/dev/.waku-fork/attachments/3f2a/notes/a.txt".into()),
            }),
            R::BlobsStat(BlobStat {
                size: 4,
                mime: "text/plain".into(),
                sha256: SHA256.into(),
                local_path: Some("/Users/dev/.waku-fork/attachments/3f2a/notes/a.txt".into()),
            }),
        ),
        (
            Method::AttachmentsImport(AttachmentsImportParams {
                name: "notes".into(),
                kind: AttachmentKind::Directory,
                parts: vec![AttachmentPart {
                    relative_path: "a.txt".into(),
                    blob: BLOB.into(),
                }],
                op_id: OP,
            }),
            R::AttachmentsImport(StoredAttachment {
                reference: "waku-attachment:3f2a0000-0000-4000-8000-000000000013".into(),
                path: "/Users/dev/.waku-fork/attachments/3f2a/notes".into(),
                name: "notes".into(),
                is_dir: true,
            }),
        ),
        (
            Method::AttachmentsImportPath(AttachmentsImportPathParams {
                path: "/Users/dev/Desktop/spec.pdf".into(),
                op_id: OP,
            }),
            R::AttachmentsImportPath(StoredAttachment {
                reference: "waku-attachment:3f2a0000-0000-4000-8000-000000000014".into(),
                path: "/Users/dev/.waku-fork/attachments/3f2b/spec.pdf".into(),
                name: "spec.pdf".into(),
                is_dir: false,
            }),
        ),
        (
            Method::VcsStatus(RootParams {
                root: project_root(),
            }),
            R::VcsStatus(status()),
        ),
        (
            Method::VcsRefs(RootParams {
                root: RootRef::Path {
                    path: "/Users/dev/waku".into(),
                },
            }),
            R::VcsRefs(Some(branches())),
        ),
        (
            Method::VcsSwitchRef(VcsSwitchRefParams {
                root: project_root(),
                name: "feature".into(),
                create: true,
                op_id: OP,
            }),
            R::VcsSwitchRef(branches()),
        ),
        (
            Method::VcsWorktrees(RootParams {
                root: project_root(),
            }),
            R::VcsWorktrees(vec![GitWorktree {
                path: "/Users/dev/waku".into(),
                head: "4a1f2c3".into(),
                branch: Some("refs/heads/main".into()),
                bare: false,
                locked: false,
                prunable: false,
            }]),
        ),
        (
            Method::VcsCreateWorktree(VcsCreateWorktreeParams {
                project_id: PROJECT,
                base_ref: Some("main".into()),
                branch_hint: "Fix the flaky test".into(),
                thread_id: Some(THREAD),
                op_id: OP,
            }),
            R::VcsCreateWorktree(CreatedWorktree {
                path: "/Users/dev/.waku-fork/worktrees/waku/fix-the-flaky-test".into(),
                branch: "waku/fix-the-flaky-test".into(),
            }),
        ),
        (
            Method::VcsRemoveWorktree(VcsRemoveWorktreeParams {
                path: "/Users/dev/.waku-fork/worktrees/waku/fix-the-flaky-test".into(),
                force: false,
                op_id: OP,
            }),
            R::VcsRemoveWorktree(Ack {}),
        ),
        (
            Method::VcsCommitPreview(RootParams {
                root: project_root(),
            }),
            R::VcsCommitPreview(CommitSnapshot {
                branch: "main".into(),
                additions: 12,
                deletions: 3,
                staged_additions: 10,
                staged_deletions: 1,
                has_staged: true,
                has_unstaged: true,
                can_push: true,
            }),
        ),
        (
            Method::VcsStage(VcsStageParams {
                root: project_root(),
                paths: vec!["src/main.rs".into()],
                stage: true,
                op_id: OP,
            }),
            R::VcsStage(status()),
        ),
        (
            Method::VcsGenerateCommitMessage(VcsGenerateCommitMessageParams {
                root: project_root(),
                include_unstaged: false,
                selection: ModelSelection::new("codex"),
            }),
            R::VcsGenerateCommitMessage(CommitMessage {
                message: "Fix the flaky test".into(),
            }),
        ),
        (
            Method::VcsCommit(VcsCommitParams {
                root: project_root(),
                message: "Fix the flaky test".into(),
                include_unstaged: false,
                push: true,
                op_id: OP,
            }),
            R::VcsCommit(CommitResult {
                sha: "9c2e4a1f2c3d4e5f60718293a4b5c6d7e8f90a1b".into(),
                pushed: true,
            }),
        ),
        (
            Method::VcsPush(RootOpParams {
                root: project_root(),
                op_id: OP,
            }),
            R::VcsPush(Ack {}),
        ),
        (
            Method::VcsPull(RootOpParams {
                root: project_root(),
                op_id: OP,
            }),
            R::VcsPull(PullResult {
                updated: true,
                conflicts: vec!["src/main.rs".into()],
            }),
        ),
        (
            Method::ReviewDiffGet(ReviewDiffParams {
                root: project_root(),
                source: ReviewDiffSource::Uncommitted,
                ignore_whitespace: true,
            }),
            R::ReviewDiffGet(ReviewDiff {
                source: ReviewDiffSource::Uncommitted,
                numstat: "10\t1\tsrc/main.rs\n-\t-\tlogo.png\n".into(),
                files: vec![
                    FileDiffSummary {
                        path: "src/main.rs".into(),
                        additions: Some(10),
                        deletions: Some(1),
                        binary: false,
                    },
                    FileDiffSummary {
                        path: "logo.png".into(),
                        additions: None,
                        deletions: None,
                        binary: true,
                    },
                ],
                patch: TextPayload::Blob {
                    reference: "waku-blob:0a1b2c3d4e5f6071.bin".into(),
                    size: 2_000_000,
                },
                complete_context: false,
            }),
        ),
        (
            Method::TerminalOpen(TerminalOpenParams {
                root: project_root(),
                cols: 120,
                rows: 32,
                title: Some("shell".into()),
                op_id: OP,
            }),
            R::TerminalOpen(TerminalOpened {
                terminal_id: TERMINAL,
                generation: GENERATION,
            }),
        ),
        (
            Method::TerminalResize(TerminalResizeParams {
                terminal_id: TERMINAL,
                cols: 100,
                rows: 30,
            }),
            R::TerminalResize(Ack {}),
        ),
        (
            Method::TerminalClear(TerminalParams {
                terminal_id: TERMINAL,
            }),
            R::TerminalClear(Ack {}),
        ),
        (
            Method::TerminalRestart(TerminalParams {
                terminal_id: TERMINAL,
            }),
            R::TerminalRestart(TerminalGeneration {
                generation: GENERATION,
            }),
        ),
        (
            Method::TerminalClose(TerminalParams {
                terminal_id: TERMINAL,
            }),
            R::TerminalClose(Ack {}),
        ),
        (
            Method::ProvidersList(NoParams {}),
            R::ProvidersList(vec![provider_status()]),
        ),
        (
            Method::ProvidersProbe(ProvidersProbeParams {
                instance_id: "codex".into(),
                discover_models: true,
                probe_version: true,
            }),
            R::ProvidersProbe(ProviderProbeResult {
                probe: ProviderProbe {
                    instance_id: "codex".into(),
                    installed: true,
                    path: Some("/usr/local/bin/codex".into()),
                    models: vec![model()],
                    agent_presets: Vec::new(),
                },
                version: Some("0.50.0".into()),
            }),
        ),
        (
            Method::ProvidersModels(ProvidersModelsParams {
                instance_id: "deepseek".into(),
                refresh: false,
            }),
            R::ProvidersModels(ProviderModels {
                models: vec![model()],
                agent_presets: vec![AgentPreset {
                    id: "code".into(),
                    name: "Code".into(),
                    description: None,
                    is_default: true,
                    is_custom: false,
                }],
                source: ModelCatalogSource::Cached,
            }),
        ),
        (
            Method::ProvidersPlanUsage(InstanceParams {
                instance_id: "claude".into(),
            }),
            R::ProvidersPlanUsage(Some(plan_usage())),
        ),
        (
            Method::UsageHistoryGet(UsageHistoryParams {
                window: UsageWindow::TrailingDays { days: 7 },
                project_ids: vec![PROJECT],
            }),
            R::UsageHistoryGet(usage_history()),
        ),
        (
            Method::SettingsUpdate(SettingsUpdateParams {
                base_revision: Some(43),
                patch: SettingsPatch {
                    disabled_providers: Some(vec!["grok".into()]),
                    storage: Some(storage()),
                    ..SettingsPatch::default()
                },
                op_id: OP,
            }),
            R::SettingsUpdate(SettingsSnapshot {
                revision: 44,
                settings: settings(),
            }),
        ),
        (
            Method::StorageWorktreeCleanupStatus(NoParams {}),
            R::StorageWorktreeCleanupStatus(WorktreeCleanupStatus {
                last_run_at: Some(1_760_000_002_000),
                last_report: Some(report()),
                next_run_at: Some(1_760_086_402_000),
                running: false,
            }),
        ),
        (
            Method::StorageRunWorktreeCleanup(StorageRunCleanupParams { op_id: OP }),
            R::StorageRunWorktreeCleanup(report()),
        ),
        (
            Method::ComputerUsePermissions(ComputerUsePermissionsParams { prompt: false }),
            R::ComputerUsePermissions(ComputerPermissions {
                screen_recording: true,
                accessibility: false,
            }),
        ),
        (
            Method::ProviderSessionsList(ProviderSessionsListParams {
                instance_id: "codex".into(),
                limit: Some(50),
            }),
            R::ProviderSessionsList(vec![ProviderSessionSummary {
                instance_id: "codex".into(),
                native_id: "01900000-0000-7000-8000-000000000001".into(),
                resume_cursor: waku_protocol::orchestration::JsonText(
                    r#"{"provider":"codex","threadId":"01900000-0000-7000-8000-000000000001"}"#
                        .into(),
                ),
                title: "Refactor the parser".into(),
                cwd: "/Users/dev/waku".into(),
                created_at: 1_760_000_000_000,
                updated_at: 1_760_000_500_000,
            }]),
        ),
    ];
    samples
        .into_iter()
        .zip(10..)
        .map(|((method, result), id)| CallSample { id, method, result })
        .collect()
}

/// Every catalog stream, once, with channels from 10.
pub fn streams() -> Vec<StreamSample> {
    vec![
        StreamSample {
            channel: 10,
            spec: StreamSpec::Drafts(DraftsSpec {}),
            after: Some(StreamCursor::Revision { revision: 77 }),
            window: None,
            items: vec![
                StreamItem::Drafts(DraftsItem::Snapshot {
                    revision: 78,
                    drafts: drafts(),
                }),
                StreamItem::Drafts(DraftsItem::Synchronized {
                    cursor: StreamCursor::Revision { revision: 78 },
                }),
                StreamItem::Drafts(DraftsItem::Changed {
                    revision: 79,
                    changes: vec![DraftChange {
                        target: DraftTarget::NewThread {
                            project_id: PROJECT,
                        },
                        draft: None,
                    }],
                }),
            ],
        },
        StreamSample {
            channel: 11,
            spec: StreamSpec::Providers(ProvidersSpec {}),
            after: None,
            window: None,
            items: vec![
                StreamItem::Providers(ProvidersItem::Snapshot {
                    revision: 5,
                    statuses: vec![provider_status()],
                }),
                StreamItem::Providers(ProvidersItem::Synchronized {
                    cursor: StreamCursor::Revision { revision: 5 },
                }),
                StreamItem::Providers(ProvidersItem::StatusUpdated {
                    revision: 6,
                    status: ProviderStatus {
                        enabled: false,
                        ..provider_status()
                    },
                }),
                StreamItem::Providers(ProvidersItem::PlanUsageUpdated {
                    instance_id: "claude".into(),
                    usage: Some(plan_usage()),
                }),
            ],
        },
        StreamSample {
            channel: 12,
            spec: StreamSpec::Terminal(TerminalSpec {
                terminal_id: TERMINAL,
                interactive: true,
            }),
            after: Some(StreamCursor::Offset {
                generation: GENERATION,
                offset: 4_096,
            }),
            window: Some(DEFAULT_DATA_WINDOW),
            items: vec![
                StreamItem::Terminal(TerminalItem::Attached {
                    generation: GENERATION,
                    cols: 120,
                    rows: 32,
                    offset: 8_192,
                    replay: 2_048,
                    truncated: true,
                    exited: false,
                    exit_code: None,
                    title: Some("shell".into()),
                }),
                StreamItem::Terminal(TerminalItem::Resized {
                    cols: 100,
                    rows: 30,
                }),
                StreamItem::Terminal(TerminalItem::Reset {
                    generation: GENERATION,
                    offset: 9_000,
                    replay: 0,
                }),
                StreamItem::Terminal(TerminalItem::Exited { code: Some(130) }),
                StreamItem::Terminal(TerminalItem::Error {
                    message: "the PTY closed".into(),
                }),
            ],
        },
        StreamSample {
            channel: 13,
            spec: StreamSpec::TerminalList(TerminalListSpec {
                root: Some(project_root()),
            }),
            after: None,
            window: None,
            items: vec![
                StreamItem::TerminalList(TerminalListItem::Snapshot {
                    terminals: vec![terminal_info()],
                }),
                StreamItem::TerminalList(TerminalListItem::Upserted {
                    terminal: TerminalInfo {
                        exited: true,
                        ..terminal_info()
                    },
                }),
                StreamItem::TerminalList(TerminalListItem::Removed {
                    terminal_id: TERMINAL,
                }),
            ],
        },
        StreamSample {
            channel: 14,
            spec: StreamSpec::VcsStatusWatch(VcsStatusSpec {
                root: project_root(),
            }),
            after: None,
            window: None,
            items: vec![StreamItem::VcsStatusWatch(VcsStatusItem::Status {
                status: status(),
                at: 1_760_000_000_000,
            })],
        },
    ]
}

/// An error with typed VCS details, as `vcs.*` answers.
pub fn vcs_error() -> Error {
    Error::new(
        ErrorCode::Vcs,
        "Git command failed: rejected (non-fast-forward)",
    )
    .with_details(ErrorDetails::Vcs {
        kind: VcsFailureKind::NonFastForward,
    })
}

/// A `revisionConflict`, as `files.write`, `drafts.apply` and
/// `settings.update` answer a stale base revision.
pub fn conflict_error() -> Error {
    Error::new(ErrorCode::Conflict, "the drafts changed since revision 77").with_details(
        ErrorDetails::RevisionConflict {
            current: "78".into(),
        },
    )
}
