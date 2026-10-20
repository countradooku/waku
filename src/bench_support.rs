//! Window-free entry points for `benches/desktop.rs` and the streaming
//! linearity test.
//!
//! The modules behind these are private to the app. This facade re-exports
//! only pure functions, plus thin wrappers that supply the theme-derived inputs
//! a frame would, so benchmarks never need a GPUI context. It adds no
//! behaviour of its own and is not a supported API.

use gpui::FontWeight;

use crate::md::render::{self, MONO_FAMILY, Palette};
use crate::theme::Theme;

pub use crate::md::highlight::{Lang, lang_for_tag, tokenize};
pub use crate::md::parser::{Block, IncrementalParser, InlineRun, InlineStyle, TopBlock, parse};
pub use crate::review_diff::{Snapshot as ReviewDiff, Source as ReviewDiffSource, parse_collected};
pub use crate::terminal::SnapshotBench as TerminalSnapshotBench;

/// `md::render::flatten` with the dark theme's palette, as a paragraph renders.
/// Returns the run count so the work is observable.
pub fn flatten(runs: &[InlineRun]) -> usize {
    let palette = Palette::from_theme(&Theme::dark());
    render::flatten(runs, &palette, FontWeight::NORMAL, palette.text)
        .runs
        .len()
}

/// The highlighted `TextRun`s of a fenced code block, as the renderer builds
/// them on a cache miss. Returns the run count.
pub fn code_runs(code: &str, lang: Option<Lang>) -> usize {
    let palette = Palette::from_theme(&Theme::dark());
    let mut code_font = gpui::font(MONO_FAMILY);
    code_font.weight = FontWeight::NORMAL;
    render::code_runs(code, lang, &code_font, &palette).len()
}
