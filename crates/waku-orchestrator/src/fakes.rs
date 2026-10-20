//! In-memory ports and executors for tests (and for wiring the orchestrator
//! up before the real implementations exist).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use waku_protocol::orchestration::*;

use crate::command::{CapturedCheckpoint, PreparePhase, PreparedWorkspace};
use crate::decide::CapabilitySource;
use crate::effects::EffectType;
use crate::effects::executor::{
    CheckpointPort, ClaimedEffect, CleanupPort, EffectContext, EffectError, EffectExecutor,
    PortError, Ports, PrepareRequest, TitlePort, WorkspacePort,
};

/// Capabilities per instance, set by the test.
#[derive(Default)]
pub struct StaticCapabilities(pub Mutex<HashMap<ProviderInstanceId, Capabilities>>);

impl StaticCapabilities {
    pub fn with(instance: &str, capabilities: Capabilities) -> Self {
        let map = HashMap::from([(ProviderInstanceId::new(instance), capabilities)]);
        Self(Mutex::new(map))
    }
}

impl CapabilitySource for StaticCapabilities {
    fn for_instance(&self, instance: &ProviderInstanceId) -> Option<Capabilities> {
        self.0.lock().get(instance).cloned()
    }
}

/// Records every call; `prepare` succeeds with a worktree under `/tmp/wt`
/// unless `fail` is set.
#[derive(Default)]
pub struct FakeWorkspace {
    pub calls: Mutex<Vec<String>>,
    pub fail: Mutex<Option<PortError>>,
}

impl WorkspacePort for FakeWorkspace {
    fn prepare(
        &self,
        request: &PrepareRequest<'_>,
        progress: &dyn Fn(PreparePhase),
    ) -> Result<PreparedWorkspace, PortError> {
        let strategy = request.strategy;
        self.calls.lock().push(format!(
            "prepare {} {strategy:?}",
            request.project_root.display()
        ));
        progress(PreparePhase::Worktree);
        if let Some(error) = self.fail.lock().clone() {
            return Err(error);
        }
        progress(PreparePhase::Setup);
        let branch = match strategy {
            WorkspaceStrategy::NewWorktree { branch, .. } => branch.clone(),
            _ => None,
        };
        Ok(PreparedWorkspace {
            cwd: PathBuf::from("/tmp/wt"),
            branch: branch.or_else(|| Some("waku/fake".to_owned())),
            worktree_path: Some(PathBuf::from("/tmp/wt")),
            owned: true,
        })
    }

    fn remove_worktree(&self, path: &Path, _branch: Option<&str>) -> Result<(), PortError> {
        self.calls.lock().push(format!("remove {}", path.display()));
        Ok(())
    }
}

/// Captures always succeed with one changed file.
#[derive(Default)]
pub struct FakeCheckpoints {
    pub calls: Mutex<Vec<String>>,
}

impl CheckpointPort for FakeCheckpoints {
    fn capture(
        &self,
        _cwd: &Path,
        git_ref: &str,
        _base_ref: Option<&str>,
    ) -> Result<CapturedCheckpoint, PortError> {
        self.calls.lock().push(format!("capture {git_ref}"));
        Ok(CapturedCheckpoint {
            git_ref: git_ref.to_owned(),
            files: vec![CheckpointFile {
                path: "src/lib.rs".to_owned(),
                kind: "modified".to_owned(),
                additions: 1,
                deletions: 0,
            }],
        })
    }

    fn restore(&self, _cwd: &Path, git_ref: &str) -> Result<(), PortError> {
        self.calls.lock().push(format!("restore {git_ref}"));
        Ok(())
    }
}

/// Titles are `"Title: <first 20 chars of the first line>"`.
#[derive(Default)]
pub struct FakeTitles {
    pub calls: Mutex<Vec<String>>,
}

impl TitlePort for FakeTitles {
    fn generate(
        &self,
        _selection: &ModelSelection,
        prompt: &str,
    ) -> Result<Option<String>, PortError> {
        self.calls.lock().push(prompt.to_owned());
        let line = prompt.lines().next().unwrap_or_default();
        Ok(Some(format!(
            "Title: {}",
            line.chars().take(20).collect::<String>()
        )))
    }
}

#[derive(Default)]
pub struct FakeCleanup {
    pub calls: Mutex<Vec<String>>,
}

impl CleanupPort for FakeCleanup {
    fn terminals(&self, thread_id: ThreadId) -> Result<(), PortError> {
        self.calls.lock().push(format!("terminals {thread_id}"));
        Ok(())
    }

    fn attachments(&self, ids: &[String]) -> Result<(), PortError> {
        self.calls
            .lock()
            .push(format!("attachments {}", ids.join(",")));
        Ok(())
    }
}

/// Fake ports, kept addressable for assertions.
#[derive(Default)]
pub struct FakePorts {
    pub workspace: Arc<FakeWorkspace>,
    pub checkpoints: Arc<FakeCheckpoints>,
    pub titles: Arc<FakeTitles>,
    pub cleanup: Arc<FakeCleanup>,
}

impl FakePorts {
    pub fn ports(&self) -> Ports {
        Ports {
            workspace: self.workspace.clone(),
            checkpoints: self.checkpoints.clone(),
            titles: self.titles.clone(),
            cleanup: self.cleanup.clone(),
        }
    }
}

/// One execution seen by a [`ScriptedExecutor`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execution {
    pub effect_id: EffectId,
    pub effect_type: EffectType,
    pub thread_id: Option<ThreadId>,
    pub attempt: u32,
    pub started: Instant,
    pub finished: Instant,
}

/// How a [`ScriptedExecutor`] treats an effect type.
#[derive(Clone, Debug, Default)]
pub struct Script {
    /// Block until `release` is called (or the effect is cancelled).
    pub gate: bool,
    /// Fail with `Retryable` this many times first.
    pub retryable_failures: u32,
    /// Always fail with `Permanent`.
    pub permanent: bool,
    /// Sleep this long before finishing.
    pub delay: Duration,
}

/// A provider stand-in that records executions and follows per-type scripts.
#[derive(Default)]
pub struct ScriptedExecutor {
    scripts: Mutex<HashMap<EffectType, Script>>,
    failures: Mutex<HashMap<EffectId, u32>>,
    executions: Mutex<Vec<Execution>>,
    started: Mutex<Vec<EffectId>>,
    started_types: Mutex<Vec<(EffectType, Option<ThreadId>)>>,
    released: Mutex<bool>,
    signal: Condvar,
}

impl ScriptedExecutor {
    pub fn script(&self, effect_type: EffectType, script: Script) {
        self.scripts.lock().insert(effect_type, script);
    }

    /// Opens every gate.
    pub fn release(&self) {
        *self.released.lock() = true;
        self.signal.notify_all();
    }

    pub fn executions(&self) -> Vec<Execution> {
        self.executions.lock().clone()
    }

    /// Effects that started (including ones still running).
    pub fn started(&self) -> Vec<EffectId> {
        self.started.lock().clone()
    }

    /// Waits until `count` executions finished, or `timeout`.
    pub fn wait_for(&self, count: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.executions.lock().len() >= count {
                return true;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        self.executions.lock().len() >= count
    }

    /// Waits until an effect of `effect_type` has started.
    pub fn wait_started(
        &self,
        effect_type: EffectType,
        timeout: Duration,
        thread: Option<ThreadId>,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self
                .started_types
                .lock()
                .iter()
                .any(|(kind, id)| *kind == effect_type && (thread.is_none() || *id == thread))
            {
                return true;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        false
    }
}

impl EffectExecutor for ScriptedExecutor {
    fn execute(&self, claimed: &ClaimedEffect, cx: &EffectContext<'_>) -> Result<(), EffectError> {
        let effect_type = claimed.effect.effect_type();
        let started = Instant::now();
        self.started.lock().push(claimed.id);
        self.started_types
            .lock()
            .push((effect_type, claimed.thread_id));
        let script = self
            .scripts
            .lock()
            .get(&effect_type)
            .cloned()
            .unwrap_or_default();
        let record = |this: &Self| {
            this.executions.lock().push(Execution {
                effect_id: claimed.id,
                effect_type,
                thread_id: claimed.thread_id,
                attempt: claimed.attempt,
                started,
                finished: Instant::now(),
            });
        };
        if script.gate {
            let mut released = self.released.lock();
            while !*released {
                if cx.cancel.is_cancelled() {
                    drop(released);
                    record(self);
                    return Err(EffectError::Cancelled);
                }
                self.signal
                    .wait_for(&mut released, Duration::from_millis(5));
            }
        }
        if !script.delay.is_zero() && cx.cancel.wait_timeout(script.delay) {
            record(self);
            return Err(EffectError::Cancelled);
        }
        record(self);
        if script.permanent {
            return Err(EffectError::Permanent("scripted failure".to_owned()));
        }
        let mut failures = self.failures.lock();
        let failed = failures.entry(claimed.id).or_default();
        if *failed < script.retryable_failures {
            *failed += 1;
            return Err(EffectError::Retryable(format!("scripted failure {failed}")));
        }
        Ok(())
    }
}
