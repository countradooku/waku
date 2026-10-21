//! Daemon terminals (RFC 0003 §5.2 "Terminal", §5.4 `terminal` and
//! `terminalList`; the PTY host of #60).
//!
//! `terminal.open` starts a shell on the daemon host; the `terminal` stream
//! attaches to it. Output arrives as DATA on the subscription's channel under
//! credit, input goes the other way as DATA on an `interactive`
//! subscription's channel. Offsets count every raw byte the PTY produced
//! since the terminal opened, across restarts; a `generation` names one shell
//! process and changes on `terminal.restart`.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::RootRef;

/// `terminal.open` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalOpenParams {
    /// The shell's working directory.
    pub root: RootRef,
    pub cols: u16,
    pub rows: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub op_id: Uuid,
}

/// `terminal.open` result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalOpened {
    pub terminal_id: Uuid,
    pub generation: Uuid,
}

/// `terminal.resize` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalResizeParams {
    pub terminal_id: Uuid,
    pub cols: u16,
    pub rows: u16,
}

/// `terminal.clear`, `terminal.restart` and `terminal.close` params.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalParams {
    pub terminal_id: Uuid,
}

/// `terminal.restart` result: the new shell's generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalGeneration {
    pub generation: Uuid,
}

/// `terminal` spec. `interactive` (which needs the `terminal` scope) makes
/// DATA the client sends on the channel the terminal's input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalSpec {
    pub terminal_id: Uuid,
    #[serde(default)]
    pub interactive: bool,
}

/// `terminal` items. The stream starts with `attached`; then output arrives
/// as DATA, the first `replay` bytes of it being the retained tail that ends
/// at `offset`. A client's cursor is `offset{generation, offset + live
/// bytes received after the replay}`; resuming from it gets only the output
/// it missed when that is still retained, else a fresh tail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TerminalItem {
    Attached {
        generation: Uuid,
        cols: u16,
        rows: u16,
        /// The raw output offset the replay ends at; live output follows.
        offset: u64,
        /// How many DATA bytes of retained output follow before live output
        /// (0 when the cursor was current).
        replay: u64,
        /// Earlier output exists that the replay does not contain.
        truncated: bool,
        exited: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
    },
    Resized {
        cols: u16,
        rows: u16,
    },
    /// Clear the screen and scrollback: the terminal was cleared, restarted
    /// (a new `generation`), or this subscriber fell behind what the daemon
    /// retains. `replay` DATA bytes of retained output follow, then live
    /// output from `offset`.
    Reset {
        generation: Uuid,
        offset: u64,
        replay: u64,
    },
    /// The shell exited. The terminal stays open with its output until it is
    /// restarted or closed.
    Exited {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<i32>,
    },
    Error {
        message: String,
    },
    #[serde(other)]
    Unknown,
}

/// `terminalList` spec: every terminal, or those whose working directory is
/// `root`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalListSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<RootRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct TerminalInfo {
    pub terminal_id: Uuid,
    /// The shell's working directory on the daemon host.
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub cols: u16,
    pub rows: u16,
    pub generation: Uuid,
    pub exited: bool,
    /// Milliseconds since the Unix epoch.
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TerminalListItem {
    Snapshot {
        terminals: Vec<TerminalInfo>,
    },
    Upserted {
        terminal: TerminalInfo,
    },
    Removed {
        terminal_id: Uuid,
    },
    #[serde(other)]
    Unknown,
}
