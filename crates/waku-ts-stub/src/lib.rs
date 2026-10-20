//! `#[derive(TS)]` that generates nothing (#30).
//!
//! `waku-protocol` derives ts-rs's `TS` on every wire type, but only the
//! `export_types` binary uses the result: it writes the web client's
//! TypeScript bindings. The app and daemon never call into it, yet compiled
//! ts-rs, its proc macro and ~130 derive expansions on every build.
//!
//! Without its `ts` feature, `waku-protocol` derives this `TS` instead. It
//! registers the same `#[ts(...)]` helper attribute, so the type definitions
//! stay identical in both builds, and expands to nothing.

use proc_macro::TokenStream;

#[proc_macro_derive(TS, attributes(ts))]
pub fn derive_ts(_item: TokenStream) -> TokenStream {
    TokenStream::new()
}
