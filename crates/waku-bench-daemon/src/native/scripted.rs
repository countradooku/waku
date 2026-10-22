//! Prompt directives for end-to-end tests (#180): lines a test writes into a
//! prompt make the fake CLIs change the working tree or keep a process
//! running, so the daemon's checkpoints, turn diffs, rewinds and background
//! work can be driven without a real provider.
//!
//! - `waku-fake:write <path> <text>` (both fakes): before the turn streams,
//!   write `<text>` (`\n` stands for a newline) to `<path>`, relative to the
//!   session's working directory;
//! - `waku-fake:background <command>` (`fake-codex`): the turn is that one
//!   command, which prints a line and keeps running after the turn completes
//!   as a background terminal (`thread/backgroundTerminals/list` lists it,
//!   `thread/backgroundTerminals/terminate` ends it).

use std::path::{Component, Path};

use parking_lot::Mutex;
use serde_json::{Value, json};

use super::{Output, now_ms, now_secs};

const WRITE: &str = "waku-fake:write ";
const BACKGROUND: &str = "waku-fake:background ";

/// Applies the prompt's `waku-fake:write` lines under `cwd`. Paths that
/// would leave it are ignored.
pub fn apply_writes(prompt: &str, cwd: &Path) {
    for line in prompt.lines() {
        let Some(rest) = line.trim().strip_prefix(WRITE) else {
            continue;
        };
        let (path, text) = rest.split_once(' ').unwrap_or((rest, ""));
        let relative = Path::new(path);
        if path.is_empty()
            || !relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
        {
            continue;
        }
        let target = cwd.join(relative);
        if let Some(parent) = target.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(target, text.replace("\\n", "\n"));
    }
}

/// The command of the prompt's `waku-fake:background` line.
pub fn background_command(prompt: &str) -> Option<String> {
    prompt
        .lines()
        .find_map(|line| line.trim().strip_prefix(BACKGROUND))
        .map(|command| command.trim().to_owned())
        .filter(|command| !command.is_empty())
}

struct Terminal {
    thread_id: String,
    process_id: String,
    item_id: String,
    command: String,
}

/// The background terminals this fake-codex process keeps.
static TERMINALS: Mutex<Vec<Terminal>> = Mutex::new(Vec::new());

/// A turn that starts `command` and completes while it keeps running.
pub fn codex_background_turn(output: &Output, thread_id: &str, turn_id: &str, command: &str) {
    let suffix = &turn_id[..8.min(turn_id.len())];
    let item_id = format!("bg_{suffix}");
    let process_id = format!("proc_{suffix}");
    let notify = |method: &str, mut params: Value| {
        params["threadId"] = json!(thread_id);
        params["turnId"] = json!(turn_id);
        let _ = output.send(&json!({"method": method, "params": params}));
    };
    let _ = output.send(&json!({
        "method": "turn/started",
        "params": {
            "threadId": thread_id,
            "turn": {"id": turn_id, "items": [], "status": "inProgress", "startedAt": now_secs()}
        }
    }));
    notify(
        "item/started",
        json!({"startedAtMs": now_ms(), "item": {
            "type": "commandExecution", "id": item_id, "command": command,
            "commandActions": [], "cwd": ".", "status": "inProgress", "processId": process_id,
        }}),
    );
    notify(
        "item/commandExecution/outputDelta",
        json!({"itemId": item_id, "delta": format!("{command}: started\n")}),
    );
    TERMINALS.lock().push(Terminal {
        thread_id: thread_id.to_owned(),
        process_id,
        item_id: item_id.clone(),
        command: command.to_owned(),
    });
    let _ = output.send(&json!({
        "method": "turn/completed",
        "params": {
            "threadId": thread_id,
            "turn": {"id": turn_id, "items": [], "status": "completed", "completedAt": now_secs()}
        }
    }));
    // Output after the turn: the process is still running.
    let _ = output.send(&json!({
        "method": "item/commandExecution/outputDelta",
        "params": {"threadId": thread_id, "turnId": turn_id, "itemId": item_id,
                   "delta": format!("{command}: still running\n")}
    }));
}

/// `thread/backgroundTerminals/list`.
pub fn codex_list(params: &Value) -> Value {
    let thread_id = params.get("threadId").and_then(Value::as_str);
    let data = TERMINALS
        .lock()
        .iter()
        .filter(|terminal| thread_id.is_none_or(|id| id == terminal.thread_id))
        .map(|terminal| {
            json!({
                "processId": terminal.process_id,
                "itemId": terminal.item_id,
                "command": terminal.command,
                "cwd": ".",
            })
        })
        .collect::<Vec<_>>();
    json!({"data": data, "nextCursor": null})
}

/// `thread/backgroundTerminals/terminate`: ends the process; its item
/// completes with exit code 143.
pub fn codex_terminate(output: &Output, params: &Value) -> Value {
    let process_id = params.get("processId").and_then(Value::as_str);
    let removed = {
        let mut terminals = TERMINALS.lock();
        let index = terminals
            .iter()
            .position(|terminal| Some(terminal.process_id.as_str()) == process_id);
        index.map(|index| terminals.remove(index))
    };
    let Some(terminal) = removed else {
        return json!({"terminated": false});
    };
    let _ = output.send(&json!({
        "method": "item/completed",
        "params": {"threadId": terminal.thread_id, "completedAtMs": now_ms(), "item": {
            "type": "commandExecution", "id": terminal.item_id, "command": terminal.command,
            "commandActions": [], "cwd": ".", "status": "completed",
            "processId": terminal.process_id, "exitCode": 143,
            "aggregatedOutput": format!("{0}: started\n{0}: still running\n", terminal.command),
        }}
    }));
    json!({"terminated": true})
}

/// The turns of each thread this fake-codex process ran, oldest first, for
/// `thread/turns/list` and `thread/revert` (a rewind's provider half).
static TURNS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// Records a turn of `thread_id`.
pub fn codex_turn_started(thread_id: &str, turn_id: &str) {
    TURNS
        .lock()
        .push((thread_id.to_owned(), turn_id.to_owned()));
}

/// `thread/turns/list`, newest first, one page.
pub fn codex_turns_list(params: &Value) -> Value {
    let thread_id = params.get("threadId").and_then(Value::as_str);
    let limit = params
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(100)
        .max(1) as usize;
    let data = TURNS
        .lock()
        .iter()
        .rev()
        .filter(|(thread, _)| Some(thread.as_str()) == thread_id)
        .take(limit)
        .map(|(_, turn)| json!({"id": turn, "items": [], "status": "completed"}))
        .collect::<Vec<_>>();
    json!({"data": data, "nextCursor": null})
}

/// `thread/revert`: drops `beforeTurnId` and every later turn; the thread
/// keeps its id.
pub fn codex_revert(params: &Value) -> Value {
    let thread_id = params
        .get("threadId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let before = params.get("beforeTurnId").and_then(Value::as_str);
    let mut turns = TURNS.lock();
    if let Some(index) = turns
        .iter()
        .position(|(thread, turn)| *thread == thread_id && Some(turn.as_str()) == before)
    {
        let mut kept = turns.drain(..).enumerate().collect::<Vec<_>>();
        kept.retain(|(at, (thread, _))| *at < index || *thread != thread_id);
        turns.extend(kept.into_iter().map(|(_, turn)| turn));
    }
    json!({"thread": {"id": thread_id}})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_stay_inside_the_working_directory() {
        let root = std::env::temp_dir().join(format!("wk-scripted-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        apply_writes(
            "please\nwaku-fake:write notes/a.txt hello\\nworld\nwaku-fake:write ../escape x",
            &root,
        );
        assert_eq!(
            std::fs::read_to_string(root.join("notes/a.txt")).unwrap(),
            "hello\nworld"
        );
        assert!(!root.parent().unwrap().join("escape").exists());
        assert_eq!(
            background_command("x\nwaku-fake:background npm run dev\n"),
            Some("npm run dev".into())
        );
        assert_eq!(background_command("nothing"), None);
        let _ = std::fs::remove_dir_all(root);
    }
}
