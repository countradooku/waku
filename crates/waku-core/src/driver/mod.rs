//! Provider drivers, all on `DriverV2` (RFC 0002, decision D6), and the
//! helpers they share.

pub mod acp;
mod activity;
pub mod amp;
pub mod claude;
pub mod codex;
mod computer_use;
pub mod deepseek;
mod history;
pub mod opencode;
pub mod pi;
mod support;
mod tool_items;
pub mod v2;

/// One-shot title generation for the orchestrator's `TitlePort` (RFC 0001).
pub(crate) use codex::generate_codex_title;

pub(crate) use acp::catalog_agent;
