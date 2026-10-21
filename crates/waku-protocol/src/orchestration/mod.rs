//! Orchestration types shared by the daemon and every client (RFC 0001 R17).
//!
//! Everything here crosses the wire or reaches disk (`state.sqlite`), so it
//! follows the forward-compatibility rules of RFC 0001 R20 and RFC 0003 §1.1:
//!
//! - structs serialize as maps with `camelCase` field names and never deny
//!   unknown fields; optional fields are omitted when `None`;
//! - fieldless enums serialize as `snake_case` strings and decode an unknown
//!   value to `Unknown` (or to their weakest variant, see [`open_enum!`]);
//! - data-carrying unions are internally tagged (`"type"`) or adjacently
//!   tagged (`DomainEvent`) and end in `#[serde(other)] Unknown`;
//! - no `serde_json::Value`, `untagged` or `flatten`: opaque provider JSON
//!   travels as [`JsonText`], and maps are never `HashMap`s.
//!
//! The module is self-contained: it does not reuse the types in
//! [`crate::model`], whose enums reject unknown values. `legacy` holds the
//! conversions from them.

#[macro_use]
mod macros;

pub mod capabilities;
pub mod command;
pub mod entities;
pub mod event;
pub mod ids;
pub mod legacy;
pub mod turn_item;
pub mod values;

pub use capabilities::*;
pub use command::*;
pub use entities::*;
pub use event::*;
pub use ids::*;
pub use turn_item::*;
pub use values::*;

#[cfg(test)]
mod tests;
