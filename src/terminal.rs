use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use crate::ui::menu::{ContextMenuHandle, MenuItem, context_menu};
use alacritty_terminal::event::{Event, EventListener, VoidListener, WindowSize};
use alacritty_terminal::event_loop::{EventLoop, EventLoopSender, Msg};
use alacritty_terminal::grid::{BidirectionalIterator, Dimensions, Row, Scroll};
use alacritty_terminal::index::{Boundary, Column, Direction, Line, Point as TerminalPoint, Side};
use alacritty_terminal::selection::{Selection, SelectionRange, SelectionType};
use alacritty_terminal::sync::FairMutex;
use alacritty_terminal::term::cell::{Cell as TermCell, Flags};
use alacritty_terminal::term::color::Colors;
use alacritty_terminal::term::search::{Match, RegexIter, RegexSearch};
use alacritty_terminal::term::{Config, Term, TermDamage, TermMode};
use alacritty_terminal::tty::{self, Shell};
use alacritty_terminal::vte::ansi::{Color, CursorShape, NamedColor, Processor, Rgb};
use anyhow::{Context as _, Result};
use crossbeam_channel::{Receiver, Sender, unbounded};
use gpui::{
    App, AppContext, Bounds, ClipboardItem, ContentMask, Context, Element, ElementId, FocusHandle,
    Focusable, FontFallbacks, FontStyle, FontWeight, GlobalElementId, Hsla, InspectorElementId,
    InteractiveElement, IntoElement, KeyDownEvent, Keystroke, LayoutId, LineLayout, Modifiers,
    ModifiersChangedEvent, MouseButton, MouseDownEvent, MouseExitEvent, MouseMoveEvent,
    MouseUpEvent, ParentElement, Pixels, Point, Render, ScrollDelta, ScrollWheelEvent, ShapedLine,
    SharedString, StrikethroughStyle, Style, Styled, Subscription, Task, TextAlign, TextRun,
    UnderlineStyle, Window, canvas, div, fill, font, point, px, relative, rgb, size,
};
use parking_lot::Mutex;

use crate::persistence::DEFAULT_RIGHT_PANEL_WIDTH;
use crate::theme::{Theme, sp};
use crate::ui::scrollbar::{self, ScrollbarState};

/// Fallback advance width, used only until the font has been measured.
const TERMINAL_CELL_WIDTH: f32 = 7.8;
const TERMINAL_CELL_HEIGHT: f32 = 18.0;
const TERMINAL_FONT_SIZE: f32 = 12.5;

#[inline]
fn primary_modifier_pressed(modifiers: &Modifiers) -> bool {
    modifiers.secondary()
}

#[inline]
fn terminal_clipboard_modifier_pressed(modifiers: &Modifiers) -> bool {
    if cfg!(target_os = "macos") {
        modifiers.secondary() && !modifiers.control && !modifiers.alt
    } else {
        // Preserve Ctrl+C for SIGINT and follow Linux terminal convention.
        modifiers.control && modifiers.shift && !modifiers.alt && !modifiers.platform
    }
}
const TERMINAL_PADDING_X: f32 = 10.0;
const TERMINAL_PADDING_Y: f32 = 8.0;
const TERMINAL_TOOLBAR_HEIGHT: f32 = 34.0;
const TERMINAL_MIN_COLUMNS: usize = 20;
const TERMINAL_MIN_ROWS: usize = 8;
const TERMINAL_SCROLLBACK_LINES: usize = 10_000;
const TERMINAL_CURSOR_BLINK_INTERVAL: Duration = Duration::from_millis(500);
const TERMINAL_CURSOR_BLINK_PAUSE: Duration = Duration::from_millis(300);

/// Alacritty's default URL hint plus local file paths containing a slash.
/// Requiring a slash keeps ordinary dotted words from becoming links.
#[rustfmt::skip]
const TERMINAL_LINK_REGEX: &str = "((ipfs:|ipns:|magnet:|mailto:|gemini://|gopher://|https://|http://|news:|file:|git://|ssh:|ftp://)|\
                                    (/|~/|\\./|\\.\\./|[A-Za-z0-9._@%+~-]+/))\
                                   [^\u{0000}-\u{001F}\u{007F}-\u{009F}<>\"\\s{-}\\^⟨⟩`\\\\]+";
const MAX_TERMINAL_LINK_SEARCH_LINES: i32 = 100;

/// Icon glyphs (nerd-font private-use codepoints) resolve through CoreText's
/// cascade rather than run splitting: JetBrains Mono itself keeps the
/// Powerline range it covers, everything else falls through to the bundled
/// symbols face registered in [`crate::assets::register_fonts`].
static TERMINAL_FONT_FALLBACKS: LazyLock<FontFallbacks> = LazyLock::new(|| {
    FontFallbacks::from_fonts(vec![crate::assets::SYMBOLS_FONT_FAMILY.to_owned()])
});

fn terminal_font() -> gpui::Font {
    let mut terminal_font = font("JetBrains Mono");
    terminal_font.fallbacks = Some(TERMINAL_FONT_FALLBACKS.clone());
    terminal_font
}

/// The four face variants a terminal run can use, built once: regular, bold,
/// italic, bold italic. Indexed by [`TerminalRunStyle::font_index`].
static TERMINAL_FONTS: LazyLock<[gpui::Font; 4]> = LazyLock::new(|| {
    let regular = terminal_font();
    let mut bold = regular.clone();
    bold.weight = FontWeight::BOLD;
    let mut italic = regular.clone();
    italic.style = FontStyle::Italic;
    let mut bold_italic = bold.clone();
    bold_italic.style = FontStyle::Italic;
    [regular, bold, italic, bold_italic]
});

enum TerminalUiEvent {
    Title(String),
    ResetTitle,
    ClipboardStore(String),
    ClipboardLoad(Arc<dyn Fn(&str) -> String + Send + Sync>),
    Exited,
}

#[derive(Clone)]
struct TerminalEventProxy {
    dirty: Arc<AtomicBool>,
    /// Wakes the view's UI task. Bounded to one slot, so a burst of PTY reads
    /// queues at most one wakeup and the sender never blocks or allocates.
    wakeup: smol::channel::Sender<()>,
    /// Cleared while the view is not on screen: content changes then only set
    /// `dirty`, and the next render picks them up through the damage state.
    visible: Arc<AtomicBool>,
    sender: Arc<OnceLock<EventLoopSender>>,
    ui_events: Sender<TerminalUiEvent>,
    window_size: Arc<Mutex<WindowSize>>,
    /// Palette OSC replies run on the PTY thread; `snapshot` refreshes this
    /// so they track the active theme.
    dark_theme: Arc<AtomicBool>,
}

impl TerminalEventProxy {
    fn write_pty(&self, bytes: impl Into<Cow<'static, [u8]>>) {
        if let Some(sender) = self.sender.get() {
            let _ = sender.send(Msg::Input(bytes.into()));
        }
    }

    /// Grid content changed: mark it and wake the view only if it is shown.
    fn content_changed(&self) {
        self.dirty.store(true, Ordering::Release);
        if self.visible.load(Ordering::SeqCst) {
            let _ = self.wakeup.try_send(());
        }
    }

    /// Queue an event the view must handle even while hidden (titles,
    /// clipboard requests the shell is waiting on, exit).
    fn send_ui_event(&self, event: TerminalUiEvent) {
        let _ = self.ui_events.send(event);
        let _ = self.wakeup.try_send(());
    }
}

impl EventListener for TerminalEventProxy {
    fn send_event(&self, event: Event) {
        match event {
            Event::Wakeup | Event::MouseCursorDirty | Event::CursorBlinkingChange => {
                self.content_changed();
            }
            Event::Title(title) => self.send_ui_event(TerminalUiEvent::Title(title)),
            Event::ResetTitle => self.send_ui_event(TerminalUiEvent::ResetTitle),
            Event::ClipboardStore(_, text) => {
                self.send_ui_event(TerminalUiEvent::ClipboardStore(text));
            }
            Event::ClipboardLoad(_, formatter) => {
                self.send_ui_event(TerminalUiEvent::ClipboardLoad(formatter));
            }
            Event::PtyWrite(text) => self.write_pty(text.into_bytes()),
            Event::ColorRequest(index, formatter) => {
                let is_dark = self.dark_theme.load(Ordering::Acquire);
                self.write_pty(formatter(terminal_rgb(index, is_dark)).into_bytes());
            }
            Event::TextAreaSizeRequest(formatter) => {
                self.write_pty(formatter(*self.window_size.lock()).into_bytes());
            }
            Event::Bell => {}
            Event::Exit | Event::ChildExit(_) => {
                self.dirty.store(true, Ordering::Release);
                self.send_ui_event(TerminalUiEvent::Exited);
            }
        }
    }
}

struct TerminalDimensions {
    columns: usize,
    rows: usize,
}

impl Dimensions for TerminalDimensions {
    fn total_lines(&self) -> usize {
        self.rows
    }

    fn screen_lines(&self) -> usize {
        self.rows
    }

    fn columns(&self) -> usize {
        self.columns
    }
}

struct TerminalSession {
    term: Arc<FairMutex<Term<TerminalEventProxy>>>,
    sender: EventLoopSender,
    dirty: Arc<AtomicBool>,
    visible: Arc<AtomicBool>,
    ui_events: Receiver<TerminalUiEvent>,
    window_size: Arc<Mutex<WindowSize>>,
    grid_size: (usize, usize),
    dark_theme: Arc<AtomicBool>,
    url_regex: RegexSearch,
}

impl TerminalSession {
    fn new(
        working_directory: &Path,
        columns: usize,
        rows: usize,
        wakeup: smol::channel::Sender<()>,
    ) -> Result<Self> {
        let columns = columns.max(TERMINAL_MIN_COLUMNS);
        let rows = rows.max(TERMINAL_MIN_ROWS);
        let window_size = WindowSize {
            num_lines: rows.min(u16::MAX as usize) as u16,
            num_cols: columns.min(u16::MAX as usize) as u16,
            cell_width: TERMINAL_CELL_WIDTH.round() as u16,
            cell_height: TERMINAL_CELL_HEIGHT.round() as u16,
        };
        let shared_window_size = Arc::new(Mutex::new(window_size));
        let dirty = Arc::new(AtomicBool::new(true));
        let visible = Arc::new(AtomicBool::new(true));
        let sender_slot = Arc::new(OnceLock::new());
        let dark_theme = Arc::new(AtomicBool::new(true));
        let (ui_event_tx, ui_events) = unbounded();
        let url_regex = RegexSearch::new(TERMINAL_LINK_REGEX)
            .map_err(|error| anyhow::anyhow!("compile terminal link regex: {error}"))?;
        let proxy = TerminalEventProxy {
            dirty: dirty.clone(),
            wakeup,
            visible: visible.clone(),
            sender: sender_slot.clone(),
            ui_events: ui_event_tx,
            window_size: shared_window_size.clone(),
            dark_theme: dark_theme.clone(),
        };

        let config = Config {
            scrolling_history: TERMINAL_SCROLLBACK_LINES,
            ..Default::default()
        };
        let dimensions = TerminalDimensions { columns, rows };
        let term = Arc::new(FairMutex::new(Term::new(
            config,
            &dimensions,
            proxy.clone(),
        )));

        let shell = crate::command_env::default_terminal_shell();
        let shell_args = crate::command_env::default_terminal_shell_args(&shell);
        let mut options = tty::Options {
            shell: Some(Shell::new(shell.to_string_lossy().into_owned(), shell_args)),
            working_directory: Some(working_directory.to_path_buf()),
            drain_on_exit: false,
            ..Default::default()
        };
        options.env.insert("TERM".into(), "xterm-256color".into());
        options.env.insert("COLORTERM".into(), "truecolor".into());
        if let Some(path) = crate::command_env::executable_search_path() {
            options
                .env
                .insert("PATH".into(), path.to_string_lossy().into_owned());
        }

        let pty = tty::new(&options, window_size, 0)
            .with_context(|| format!("spawn terminal in {}", working_directory.display()))?;
        let event_loop = EventLoop::new(term.clone(), proxy, pty, false, false)
            .context("create Alacritty PTY event loop")?;
        let sender = event_loop.channel();
        sender_slot
            .set(sender.clone())
            .map_err(|_| anyhow::anyhow!("initialize Alacritty PTY sender"))?;
        event_loop.spawn();

        Ok(Self {
            term,
            sender,
            dirty,
            visible,
            ui_events,
            window_size: shared_window_size,
            grid_size: (columns, rows),
            dark_theme,
            url_regex,
        })
    }

    fn write(&self, bytes: impl Into<Cow<'static, [u8]>>) {
        let bytes = bytes.into();
        if !bytes.is_empty() {
            let _ = self.sender.send(Msg::Input(bytes));
        }
    }

    fn resize(&mut self, columns: usize, rows: usize, cell_width: f32) {
        let columns = columns.max(TERMINAL_MIN_COLUMNS);
        let rows = rows.max(TERMINAL_MIN_ROWS);
        if self.grid_size == (columns, rows) {
            return;
        }

        self.grid_size = (columns, rows);
        let dimensions = TerminalDimensions { columns, rows };
        self.term.lock().resize(dimensions);
        let size = WindowSize {
            num_lines: rows.min(u16::MAX as usize) as u16,
            num_cols: columns.min(u16::MAX as usize) as u16,
            cell_width: cell_width.round() as u16,
            cell_height: TERMINAL_CELL_HEIGHT.round() as u16,
        };
        *self.window_size.lock() = size;
        let _ = self.sender.send(Msg::Resize(size));
        self.dirty.store(true, Ordering::Release);
    }

    fn mode(&self) -> TermMode {
        *self.term.lock().mode()
    }

    fn scroll(&self, lines: i32) {
        if lines == 0 {
            return;
        }
        self.term.lock().scroll_display(Scroll::Delta(lines));
        self.dirty.store(true, Ordering::Release);
    }

    fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    fn link_at(&mut self, point: TerminalPoint) -> Option<TerminalLink> {
        let TerminalSession {
            term, url_regex, ..
        } = self;
        let term = term.lock();
        let (value, bounds) =
            hyperlink_at(&term, point).or_else(|| plain_link_at(&term, url_regex, point))?;
        Some(TerminalLink { value, bounds })
    }

    /// Bring `grid` up to date with the terminal, rebuilding only the rows
    /// that changed since the previous call.
    fn snapshot(
        &self,
        grid: &mut TerminalGrid,
        theme: Theme,
        selection_color: Hsla,
        cursor_style: TerminalCursorStyle,
        hovered_link: Option<&Match>,
    ) {
        self.dark_theme.store(theme.is_dark, Ordering::Release);
        let mut term = self.term.lock();
        grid.update(
            &mut term,
            self.grid_size,
            theme,
            selection_color,
            cursor_style,
            hovered_link,
        );
    }
}

/// Grid-wide inputs to row colors that alacritty's damage does not track. Any
/// change to them invalidates every cached row.
#[derive(Clone, Copy, PartialEq)]
struct GridStyle {
    text: Hsla,
    background: Hsla,
    selection: Hsla,
    is_dark: bool,
}

/// One visible row: its text and styled runs, plus the shaped line and merged
/// background quads derived from them the first time the row is painted.
#[derive(Default)]
struct GridRow {
    text: String,
    runs: Vec<TerminalRun>,
    /// The row must be rebuilt from the terminal before it is painted again.
    stale: bool,
    paint: Option<RowPaint>,
}

struct RowPaint {
    line: ShapedLine,
    /// Merged non-default backgrounds as `(left, right, color)`, relative to
    /// the row origin.
    backgrounds: Vec<(Pixels, Pixels, Hsla)>,
}

/// Paint-ready rows for the visible grid, kept across frames.
///
/// A row is rebuilt only when alacritty's damage reports it changed, or when
/// something damage does not cover touches it: the selection, the hovered
/// link, the cursor style, the scroll position or the theme. Pure over the
/// terminal state, so its cost is benchmarked without a PTY (see
/// [`SnapshotBench`]).
struct TerminalGrid {
    columns: usize,
    rows: Vec<GridRow>,
    style: Option<GridStyle>,
    display_offset: usize,
    selection: Option<SelectionRange>,
    hovered_link: Option<Match>,
    /// Viewport row, column and drawn style of the cursor as last built.
    cursor: Option<(i32, usize, TerminalCursorStyle)>,
    outline_cursor: Option<(usize, usize)>,
    /// Rows the last [`TerminalGrid::update`] rebuilt: a frame's grid cost is
    /// proportional to this, not to the grid size.
    rebuilt_rows: usize,
}

impl TerminalGrid {
    fn new() -> Self {
        Self {
            columns: 0,
            rows: Vec::new(),
            style: None,
            display_offset: 0,
            selection: None,
            hovered_link: None,
            cursor: None,
            outline_cursor: None,
            rebuilt_rows: 0,
        }
    }

    fn invalidate_all(&mut self) {
        for row in &mut self.rows {
            row.stale = true;
        }
    }

    fn invalidate_row(&mut self, row: i32) {
        if let Some(row) = usize::try_from(row)
            .ok()
            .and_then(|row| self.rows.get_mut(row))
        {
            row.stale = true;
        }
    }

    /// Invalidate the viewport rows that show grid lines `start..=end`.
    fn invalidate_lines(&mut self, start: Line, end: Line) {
        let offset = self.display_offset as i32;
        let last = self.rows.len() as i32 - 1;
        for row in (start.0 + offset).max(0)..=(end.0 + offset).min(last) {
            self.rows[row as usize].stale = true;
        }
    }

    fn update<T: EventListener>(
        &mut self,
        term: &mut Term<T>,
        (columns, rows): (usize, usize),
        theme: Theme,
        selection_color: Hsla,
        cursor_style: TerminalCursorStyle,
        hovered_link: Option<&Match>,
    ) {
        if self.columns != columns || self.rows.len() != rows {
            self.columns = columns;
            self.rows.resize_with(rows, GridRow::default);
            self.invalidate_all();
        }
        // Read and reset the damage on every update, so it always covers
        // exactly the changes since the rows were last built.
        match term.damage() {
            TermDamage::Full => self.invalidate_all(),
            TermDamage::Partial(lines) => {
                for damage in lines {
                    if let Some(row) = self.rows.get_mut(damage.line) {
                        row.stale = true;
                    }
                }
            }
        }
        term.reset_damage();

        let style = GridStyle {
            text: theme.text,
            background: theme.terminal,
            selection: selection_color,
            is_dark: theme.is_dark,
        };
        if self.style != Some(style) {
            self.style = Some(style);
            self.invalidate_all();
        }

        let content = term.renderable_content();
        if self.display_offset != content.display_offset {
            self.display_offset = content.display_offset;
            self.invalidate_all();
        }
        if self.selection != content.selection {
            for range in [self.selection, content.selection].into_iter().flatten() {
                self.invalidate_lines(range.start.line, range.end.line);
            }
            self.selection = content.selection;
        }
        if self.hovered_link.as_ref() != hovered_link {
            let lines = [self.hovered_link.as_ref(), hovered_link]
                .map(|link| link.map(|bounds| (bounds.start().line, bounds.end().line)));
            for (start, end) in lines.into_iter().flatten() {
                self.invalidate_lines(start, end);
            }
            self.hovered_link = hovered_link.cloned();
        }

        let cursor_row = content.cursor.point.line.0 + content.display_offset as i32;
        let cursor_column = content.cursor.point.column.0;
        let cursor_style = if matches!(content.cursor.shape, CursorShape::Hidden) {
            TerminalCursorStyle::Hidden
        } else {
            cursor_style
        };
        let cursor = (cursor_row, cursor_column, cursor_style);
        if self.cursor != Some(cursor) {
            if let Some((row, _, _)) = self.cursor {
                self.invalidate_row(row);
            }
            self.invalidate_row(cursor_row);
            self.cursor = Some(cursor);
        }
        self.outline_cursor = (cursor_style == TerminalCursorStyle::Outline
            && (0..rows as i32).contains(&cursor_row)
            && cursor_column < columns)
            .then_some((cursor_row as usize, cursor_column));

        let context = RowContext {
            theme,
            selection_color,
            colors: content.colors,
            selection: self.selection,
            hovered_link: self.hovered_link.as_ref(),
            columns,
            term_columns: term.columns(),
        };
        let grid = term.grid();
        let screen_lines = term.screen_lines() as i32;
        let display_offset = self.display_offset as i32;
        let mut rebuilt = 0;
        for (index, row) in self.rows.iter_mut().enumerate() {
            if !row.stale {
                continue;
            }
            let line = Line(index as i32 - display_offset);
            let cells = (line.0 < screen_lines).then(|| &grid[line]);
            let solid_cursor = (cursor_style == TerminalCursorStyle::Solid
                && index as i32 == cursor_row)
                .then_some(cursor_column);
            context.build(row, cells, line, solid_cursor);
            row.stale = false;
            row.paint = None;
            rebuilt += 1;
        }
        self.rebuilt_rows = rebuilt;
    }
}

/// Everything a row rebuild reads besides the row's own cells.
struct RowContext<'a> {
    theme: Theme,
    selection_color: Hsla,
    colors: &'a Colors,
    selection: Option<SelectionRange>,
    hovered_link: Option<&'a Match>,
    columns: usize,
    term_columns: usize,
}

impl RowContext<'_> {
    /// Rebuild `row` from the cells of grid line `line`, reusing its buffers:
    /// each cell's character goes straight into the row string.
    fn build(
        &self,
        row: &mut GridRow,
        cells: Option<&Row<TermCell>>,
        line: Line,
        solid_cursor: Option<usize>,
    ) {
        let theme = self.theme;
        row.text.clear();
        row.runs.clear();
        row.text.reserve(self.columns);
        let selection = self
            .selection
            .filter(|range| range.start.line <= line && line <= range.end.line);
        let hovered_link = self
            .hovered_link
            .filter(|bounds| bounds.start().line <= line && line <= bounds.end().line);
        let blank = TerminalRunStyle {
            foreground: theme.text,
            background: theme.terminal,
            bold: false,
            italic: false,
            underline: false,
            strikeout: false,
        };
        // Neighbouring cells usually share colors; skip resolving them again.
        let mut last_foreground = None;
        let mut last_background = None;

        for column in 0..self.columns {
            let Some(cell) = cells
                .filter(|_| column < self.term_columns)
                .map(|cells| &cells[Column(column)])
            else {
                row.text.push(' ');
                push_run(&mut row.runs, 1, blank);
                continue;
            };
            let start = row.text.len();
            if cell.flags.intersects(
                Flags::WIDE_CHAR_SPACER | Flags::LEADING_WIDE_CHAR_SPACER | Flags::HIDDEN,
            ) {
                row.text.push(' ');
            } else {
                row.text.push(cell.c);
            }
            if let Some(zerowidth) = cell.zerowidth() {
                row.text.extend(zerowidth);
            }
            let len = row.text.len() - start;

            let mut foreground = resolve_cached(&mut last_foreground, cell.fg, |color| {
                resolve_color(color, self.colors, theme, true)
            });
            let mut background = resolve_cached(&mut last_background, cell.bg, |color| {
                resolve_color(color, self.colors, theme, false)
            });
            if cell.flags.contains(Flags::INVERSE) {
                std::mem::swap(&mut foreground, &mut background);
            }
            // `Flags::DIM_BOLD` and `Flags::BOLD_ITALIC` are unions of the
            // individual bits, so testing them with `intersects` would also
            // match plain bold or italic cells.
            if cell.flags.contains(Flags::DIM) {
                if theme.is_dark {
                    foreground.l *= 0.7;
                } else {
                    foreground.l = 1.0 - (1.0 - foreground.l) * 0.7;
                }
            }
            let point = TerminalPoint::new(line, Column(column));
            if selection.is_some_and(|selection| selection.contains(point)) {
                background = self.selection_color;
                foreground = theme.text;
            }
            if solid_cursor == Some(column) {
                background = theme.text;
                foreground = theme.terminal;
            }

            push_run(
                &mut row.runs,
                len,
                TerminalRunStyle {
                    foreground,
                    background,
                    bold: cell.flags.contains(Flags::BOLD),
                    italic: cell.flags.contains(Flags::ITALIC),
                    underline: cell.flags.intersects(Flags::ALL_UNDERLINES)
                        || hovered_link.is_some_and(|bounds| bounds.contains(&point)),
                    strikeout: cell.flags.contains(Flags::STRIKEOUT),
                },
            );
        }
    }
}

fn push_run(runs: &mut Vec<TerminalRun>, len: usize, style: TerminalRunStyle) {
    match runs.last_mut() {
        Some(run) if run.style == style => run.len += len,
        _ => runs.push(TerminalRun { len, style }),
    }
}

fn resolve_cached(
    cache: &mut Option<(Color, Hsla)>,
    color: Color,
    resolve: impl FnOnce(Color) -> Hsla,
) -> Hsla {
    match *cache {
        Some((cached, value)) if cached == color => value,
        _ => {
            let value = resolve(color);
            *cache = Some((color, value));
            value
        }
    }
}

/// A PTY-less grid for the `desktop` microbenchmark: the snapshot pass a frame
/// runs, over a terminal fed from recorded output instead of a live shell.
#[doc(hidden)]
pub struct SnapshotBench {
    term: Term<VoidListener>,
    grid: TerminalGrid,
    grid_size: (usize, usize),
    processor: Processor,
    /// Rewrites one row in place; its last byte changes on every edit.
    edit: Vec<u8>,
    edits: u8,
}

impl SnapshotBench {
    pub fn new(columns: usize, rows: usize, output: &[u8]) -> Self {
        let dimensions = TerminalDimensions { columns, rows };
        let config = Config {
            scrolling_history: TERMINAL_SCROLLBACK_LINES,
            ..Default::default()
        };
        let mut term = Term::new(config, &dimensions, VoidListener);
        let mut processor: Processor = Processor::new();
        processor.advance(&mut term, output);
        let mut edit = format!("\x1b[{};1H", rows / 2 + 1).into_bytes();
        edit.extend_from_slice(b"incremental row update, frame x");
        let mut bench = Self {
            term,
            grid: TerminalGrid::new(),
            grid_size: (columns, rows),
            processor,
            edit,
            edits: 0,
        };
        bench.update();
        bench
    }

    fn update(&mut self) {
        let theme = Theme::dark();
        self.grid.update(
            &mut self.term,
            self.grid_size,
            theme,
            theme.selection,
            TerminalCursorStyle::Solid,
            None,
        );
    }

    /// One full-grid rebuild, as a frame after a scroll, resize or theme
    /// change runs. Returns the styled run count so the work is observable.
    pub fn snapshot(&mut self) -> usize {
        self.grid.invalidate_all();
        self.update();
        self.grid.rows.iter().map(|row| row.runs.len()).sum()
    }

    /// One frame after output rewrote a single row in place: parse the edit,
    /// then update the grid. Returns the number of rows rebuilt.
    pub fn incremental(&mut self) -> usize {
        self.edits = (self.edits + 1) % 26;
        if let Some(last) = self.edit.last_mut() {
            *last = b'a' + self.edits;
        }
        self.processor.advance(&mut self.term, &self.edit);
        self.update();
        self.grid.rebuilt_rows
    }
}

impl Drop for TerminalSession {
    fn drop(&mut self) {
        let _ = self.sender.send(Msg::Shutdown);
    }
}

/// Adapts the alacritty grid to the overlay scrollbar: the scrollback history
/// is the content above the viewport, and `display_offset` is how far back up
/// into it the view currently sits (0 = pinned to the live bottom).
#[derive(Clone)]
struct TerminalScrollbarTarget {
    term: Arc<FairMutex<Term<TerminalEventProxy>>>,
    dirty: Arc<AtomicBool>,
    viewport_rows: usize,
}

impl scrollbar::Scrollable for TerminalScrollbarTarget {
    fn viewport_height(&self) -> Pixels {
        px(self.viewport_rows as f32 * TERMINAL_CELL_HEIGHT)
    }

    fn max_offset(&self) -> Pixels {
        px(self.term.lock().grid().history_size() as f32 * TERMINAL_CELL_HEIGHT)
    }

    fn scrolled(&self) -> Pixels {
        let term = self.term.lock();
        let grid = term.grid();
        let lines_above = grid.history_size().saturating_sub(grid.display_offset());
        px(lines_above as f32 * TERMINAL_CELL_HEIGHT)
    }

    fn scroll_to(&self, offset: Pixels) {
        let mut term = self.term.lock();
        let target_offset = (term.grid().history_size() as f32
            - f32::from(offset) / TERMINAL_CELL_HEIGHT)
            .round()
            .max(0.0) as usize;
        let delta = target_offset as i32 - term.grid().display_offset() as i32;
        if delta != 0 {
            term.scroll_display(Scroll::Delta(delta));
            self.dirty.store(true, Ordering::Release);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TerminalRunStyle {
    foreground: Hsla,
    background: Hsla,
    bold: bool,
    italic: bool,
    underline: bool,
    strikeout: bool,
}

impl TerminalRunStyle {
    /// The shaping run for `len` bytes in this style. Backgrounds are painted
    /// separately as merged quads, so the run carries none.
    fn text_run(&self, len: usize) -> TextRun {
        let font = &TERMINAL_FONTS[usize::from(self.bold) | (usize::from(self.italic) << 1)];
        TextRun {
            len,
            font: font.clone(),
            color: self.foreground,
            background_color: None,
            underline: self.underline.then_some(UnderlineStyle {
                thickness: px(1.0),
                color: Some(self.foreground),
                wavy: false,
            }),
            strikethrough: self.strikeout.then_some(StrikethroughStyle {
                thickness: px(1.0),
                color: Some(self.foreground),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TerminalRun {
    len: usize,
    style: TerminalRunStyle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TerminalLink {
    value: String,
    bounds: Match,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TerminalLinkTarget {
    Url(String),
    File(PathBuf),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TerminalCursorStyle {
    Solid,
    Outline,
    Hidden,
}

struct TerminalCursorBlink {
    visible: bool,
    enabled: bool,
    epoch: usize,
    _task: Task<()>,
}

impl TerminalCursorBlink {
    fn new() -> Self {
        Self {
            visible: true,
            enabled: false,
            epoch: 0,
            _task: Task::ready(()),
        }
    }

    fn start(&mut self, cx: &mut Context<Self>) {
        if self.enabled {
            return;
        }

        self.enabled = true;
        self.visible = true;
        let epoch = self.next_epoch();
        self.schedule_blink(epoch, TERMINAL_CURSOR_BLINK_INTERVAL, cx);
        cx.notify();
    }

    fn stop(&mut self, cx: &mut Context<Self>) {
        if !self.enabled && self.visible {
            return;
        }

        self.enabled = false;
        self.visible = true;
        self.next_epoch();
        cx.notify();
    }

    fn pause(&mut self, cx: &mut Context<Self>) {
        if !self.enabled {
            return;
        }

        self.visible = true;
        let epoch = self.next_epoch();
        self.schedule_blink(epoch, TERMINAL_CURSOR_BLINK_PAUSE, cx);
        cx.notify();
    }

    fn blink(&mut self, epoch: usize, cx: &mut Context<Self>) {
        if !self.enabled || epoch != self.epoch {
            return;
        }

        self.visible = !self.visible;
        let epoch = self.next_epoch();
        self.schedule_blink(epoch, TERMINAL_CURSOR_BLINK_INTERVAL, cx);
        cx.notify();
    }

    fn schedule_blink(&mut self, epoch: usize, delay: Duration, cx: &mut Context<Self>) {
        self._task = cx.spawn(async move |this, cx| {
            cx.background_executor().timer(delay).await;
            if let Some(this) = this.upgrade() {
                this.update(cx, |this, cx| this.blink(epoch, cx));
            }
        });
    }

    fn next_epoch(&mut self) -> usize {
        self.epoch = self.epoch.wrapping_add(1);
        self.epoch
    }

    fn visible(&self) -> bool {
        self.visible
    }
}

pub struct TerminalView {
    session: Option<TerminalSession>,
    error: Option<String>,
    focus_handle: FocusHandle,
    working_directory: PathBuf,
    title: String,
    exited: bool,
    scroll_accumulator: f32,
    panel_width: f32,
    /// Advance width of one grid cell, measured from the terminal font on
    /// first render so grid math matches what the shaped rows lay out.
    measured_cell_width: Option<f32>,
    scrollbar_state: Rc<ScrollbarState>,
    grid_bounds: Rc<Cell<Option<Bounds<Pixels>>>>,
    /// Built rows, shared with the grid element that shapes and paints them.
    grid: Rc<RefCell<TerminalGrid>>,
    selecting: bool,
    hovered_link: Option<TerminalLink>,
    cursor_blink: gpui::Entity<TerminalCursorBlink>,
    cursor_focus_tracking_started: bool,
    context_menu: ContextMenuHandle,
    /// Receives the PTY thread's wakeups until the first render with a window
    /// moves it into `_wakeup_task`.
    wakeups: Option<smol::channel::Receiver<()>>,
    _wakeup_task: Task<()>,
    /// A frame callback is already scheduled; later wakeups coalesce into it.
    frame_scheduled: bool,
    /// A UI event (title, exit) changed what the view shows.
    ui_changed: bool,
    /// The frame callback notified and the view has not rendered since. Frame
    /// callbacks run right before that frame's draw, so finding this still set
    /// in a later callback means a draw passed without rendering the terminal.
    frame_notify_unrendered: bool,
    /// The cursor blink notified and the view has not rendered since.
    blink_notify_unrendered: bool,
    /// Not on screen: PTY output only sets the dirty flag and the cursor does
    /// not blink until the next render.
    hidden: bool,
    _subscriptions: Vec<Subscription>,
}

impl TerminalView {
    pub fn new(working_directory: PathBuf, cx: &mut Context<Self>) -> Self {
        let terminal_cwd = working_directory.clone();
        let (wakeup_tx, wakeups) = smol::channel::bounded(1);
        cx.spawn(async move |this, cx| {
            let started = cx
                .background_executor()
                .spawn(async move { TerminalSession::new(&terminal_cwd, 52, 36, wakeup_tx) })
                .await;
            let _ = this.update(cx, |this, cx| {
                match started {
                    Ok(session) => this.session = Some(session),
                    Err(error) => this.error = Some(error.to_string()),
                }
                cx.notify();
            });
        })
        .detach();

        let context_menu = ContextMenuHandle::new(cx);
        let cursor_blink = cx.new(|_| TerminalCursorBlink::new());
        let subscriptions = vec![cx.observe(&cursor_blink, |this, _, cx| this.on_blink(cx))];

        Self {
            session: None,
            error: None,
            focus_handle: cx.focus_handle(),
            title: tr!("right_panel.terminal"),
            working_directory,
            exited: false,
            scroll_accumulator: 0.0,
            panel_width: DEFAULT_RIGHT_PANEL_WIDTH,
            measured_cell_width: None,
            scrollbar_state: ScrollbarState::new(),
            grid_bounds: Rc::new(Cell::new(None)),
            grid: Rc::new(RefCell::new(TerminalGrid::new())),
            selecting: false,
            hovered_link: None,
            cursor_blink,
            cursor_focus_tracking_started: false,
            context_menu,
            wakeups: Some(wakeups),
            _wakeup_task: Task::ready(()),
            frame_scheduled: false,
            ui_changed: false,
            frame_notify_unrendered: false,
            blink_notify_unrendered: false,
            hidden: false,
            _subscriptions: subscriptions,
        }
    }

    pub fn working_directory(&self) -> &Path {
        &self.working_directory
    }

    pub fn set_panel_width(&mut self, width: f32) {
        self.panel_width = width;
    }

    pub fn refresh_localized_text(&mut self, cx: &mut Context<Self>) {
        if matches!(self.title.as_str(), "Terminal" | "终端") {
            self.title = tr!("right_panel.terminal");
            cx.notify();
        }
    }

    /// Start the wakeup loop once a session and a window exist. The loop
    /// sleeps on the channel, so an idle terminal costs nothing.
    fn ensure_wakeup_loop(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.session.is_none() {
            return;
        }
        let Some(wakeups) = self.wakeups.take() else {
            return;
        };
        self._wakeup_task = cx.spawn_in(window, async move |this, cx| {
            while wakeups.recv().await.is_ok() {
                if this
                    .update_in(cx, |this, window, cx| this.on_wakeup(window, cx))
                    .is_err()
                {
                    break;
                }
            }
        });
    }

    /// The PTY thread changed something. UI events are handled right away
    /// (the shell may be waiting on a clipboard reply); redraws coalesce into
    /// one notify per display frame.
    fn on_wakeup(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.ui_changed |= self.drain_ui_events(cx);
        if self.hidden || self.frame_scheduled {
            return;
        }
        self.frame_scheduled = true;
        cx.on_next_frame(window, |this, _, cx| this.on_frame(cx));
    }

    fn on_frame(&mut self, cx: &mut Context<Self>) {
        self.frame_scheduled = false;
        if self.frame_notify_unrendered {
            self.set_hidden(cx);
            return;
        }
        let dirty = self
            .session
            .as_ref()
            .is_some_and(TerminalSession::take_dirty);
        if dirty || std::mem::take(&mut self.ui_changed) {
            self.frame_notify_unrendered = true;
            cx.notify();
        }
    }

    fn on_blink(&mut self, cx: &mut Context<Self>) {
        if self.hidden {
            return;
        }
        if self.blink_notify_unrendered || self.frame_notify_unrendered {
            self.set_hidden(cx);
            return;
        }
        self.blink_notify_unrendered = true;
        cx.notify();
    }

    /// The view was notified but not drawn: it is not on screen. Stop waking
    /// for output and stop blinking until it renders again.
    fn set_hidden(&mut self, cx: &mut Context<Self>) {
        self.hidden = true;
        if let Some(session) = &self.session {
            session.visible.store(false, Ordering::SeqCst);
        }
        self.cursor_blink.update(cx, |cursor, cx| cursor.stop(cx));
    }

    /// Called from render: the view is on screen.
    fn mark_rendered(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.frame_notify_unrendered = false;
        self.blink_notify_unrendered = false;
        self.ui_changed = false;
        if let Some(session) = &self.session {
            // Render reads the whole grid state, so pending changes are seen.
            session.take_dirty();
        }
        if self.hidden {
            self.hidden = false;
            if let Some(session) = &self.session {
                session.visible.store(true, Ordering::SeqCst);
            }
            self.update_cursor_blinking(window, cx);
        }
    }

    fn drain_ui_events(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(session) = &self.session else {
            return false;
        };
        let mut changed = false;
        while let Ok(event) = session.ui_events.try_recv() {
            changed = true;
            match event {
                TerminalUiEvent::Title(title) => self.title = title,
                TerminalUiEvent::ResetTitle => self.title = tr!("right_panel.terminal"),
                TerminalUiEvent::ClipboardStore(text) => {
                    cx.write_to_clipboard(gpui::ClipboardItem::new_string(text));
                }
                TerminalUiEvent::ClipboardLoad(formatter) => {
                    let text = cx
                        .read_from_clipboard()
                        .and_then(|item| item.text())
                        .unwrap_or_default();
                    session.write(formatter(&text).into_bytes());
                }
                TerminalUiEvent::Exited => self.exited = true,
            }
        }
        changed
    }

    fn on_key_down(&mut self, event: &KeyDownEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.pause_cursor_blink(cx);
        let keystroke = &event.keystroke;
        if terminal_clipboard_modifier_pressed(&keystroke.modifiers)
            && keystroke.key.eq_ignore_ascii_case("c")
        {
            self.copy_selection(cx);
            window.prevent_default();
            cx.stop_propagation();
            return;
        }
        if terminal_clipboard_modifier_pressed(&keystroke.modifiers)
            && keystroke.key.eq_ignore_ascii_case("a")
        {
            self.select_all(cx);
            window.prevent_default();
            cx.stop_propagation();
            return;
        }

        let Some(session) = &self.session else {
            return;
        };
        if terminal_clipboard_modifier_pressed(&keystroke.modifiers)
            && keystroke.key.eq_ignore_ascii_case("v")
        {
            if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
                session.term.lock().selection = None;
                let bytes = bracketed_paste(text, session.mode());
                session.write(bytes);
                session.dirty.store(true, Ordering::Release);
                cx.notify();
                window.prevent_default();
                cx.stop_propagation();
            }
            return;
        }

        if let Some(bytes) = terminal_key_bytes(keystroke, session.mode()) {
            session.term.lock().selection = None;
            session.write(bytes);
            session.dirty.store(true, Ordering::Release);
            cx.notify();
            window.prevent_default();
            cx.stop_propagation();
        }
    }

    fn cell_width(&self) -> f32 {
        self.measured_cell_width.unwrap_or(TERMINAL_CELL_WIDTH)
    }

    fn grid_point_for_position(
        &self,
        position: Point<Pixels>,
        clamp_to_grid: bool,
    ) -> Option<(TerminalPoint, Side)> {
        let bounds = self.grid_bounds.get()?;
        let session = self.session.as_ref()?;
        let display_offset = session.term.lock().grid().display_offset();
        terminal_grid_point(
            bounds,
            position,
            self.cell_width(),
            session.grid_size.0,
            session.grid_size.1,
            display_offset,
            clamp_to_grid,
        )
    }

    fn on_mouse_down(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        // The overlay scrollbar's listeners already ran (bubble order is
        // reverse registration and it paints above the grid) but do not stop
        // propagation; a grab or track click must not also start a selection
        // underneath the bar.
        if self.scrollbar_state.engaged() {
            return;
        }
        window.focus(&self.focus_handle, cx);
        let Some((point, side)) = self.grid_point_for_position(event.position, false) else {
            return;
        };

        if primary_modifier_pressed(&event.modifiers)
            && let Some(link) = self
                .session
                .as_mut()
                .and_then(|session| session.link_at(point))
            && let Some(target) =
                terminal_link_target(&link.value, self.working_directory.as_path())
        {
            self.selecting = false;
            self.hovered_link = Some(link);
            match target {
                TerminalLinkTarget::Url(url) => cx.open_url(&url),
                TerminalLinkTarget::File(path) => {
                    crate::platform::reveal_in_file_manager(&path, cx)
                }
            }
            window.prevent_default();
            cx.stop_propagation();
            cx.notify();
            return;
        }

        let Some(session) = &self.session else {
            return;
        };

        let mut term = session.term.lock();
        if event.modifiers.shift
            && let Some(selection) = term.selection.as_mut()
        {
            selection.update(point, side);
        } else {
            let selection_type = match event.click_count {
                2 => SelectionType::Semantic,
                count if count >= 3 => SelectionType::Lines,
                _ => SelectionType::Simple,
            };
            term.selection = Some(Selection::new(selection_type, point, side));
        }
        drop(term);

        self.selecting = true;
        session.dirty.store(true, Ordering::Release);
        cx.stop_propagation();
        cx.notify();
    }

    fn on_mouse_move(&mut self, event: &MouseMoveEvent, _: &mut Window, cx: &mut Context<Self>) {
        let hover_changed = if self.selecting {
            self.set_hovered_link(None)
        } else {
            self.refresh_hovered_link(primary_modifier_pressed(&event.modifiers), event.position)
        };
        if !self.selecting || event.pressed_button != Some(MouseButton::Left) {
            if hover_changed {
                cx.notify();
            }
            return;
        }
        let Some((point, side)) = self.grid_point_for_position(event.position, true) else {
            return;
        };
        let Some(session) = &self.session else {
            return;
        };

        if let Some(selection) = session.term.lock().selection.as_mut() {
            selection.update(point, side);
            session.dirty.store(true, Ordering::Release);
            cx.stop_propagation();
            cx.notify();
        } else if hover_changed {
            cx.notify();
        }
    }

    fn on_mouse_up(&mut self, _: &MouseUpEvent, _: &mut Window, _: &mut Context<Self>) {
        self.selecting = false;
    }

    fn on_mouse_exit(&mut self, _: &MouseExitEvent, _: &mut Window, cx: &mut Context<Self>) {
        if self.set_hovered_link(None) {
            cx.notify();
        }
    }

    fn on_modifiers_changed(
        &mut self,
        event: &ModifiersChangedEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.refresh_hovered_link(
            primary_modifier_pressed(&event.modifiers),
            window.mouse_position(),
        ) {
            cx.notify();
        }
    }

    fn refresh_hovered_link(&mut self, command_pressed: bool, position: Point<Pixels>) -> bool {
        let link = if command_pressed {
            self.grid_point_for_position(position, false)
                .and_then(|(point, _)| self.session.as_mut()?.link_at(point))
        } else {
            None
        };
        self.set_hovered_link(link)
    }

    fn set_hovered_link(&mut self, link: Option<TerminalLink>) -> bool {
        if self.hovered_link == link {
            return false;
        }
        self.hovered_link = link;
        true
    }

    fn selected_text(&self) -> Option<String> {
        self.session
            .as_ref()?
            .term
            .lock()
            .selection_to_string()
            .filter(|text| !text.is_empty())
    }

    fn copy_selection(&self, cx: &mut App) {
        if let Some(text) = self.selected_text() {
            cx.write_to_clipboard(ClipboardItem::new_string(text));
        }
    }

    fn paste(&mut self, text: String, cx: &mut Context<Self>) {
        self.pause_cursor_blink(cx);
        let Some(session) = &self.session else {
            return;
        };
        session.term.lock().selection = None;
        session.write(bracketed_paste(text, session.mode()));
        session.dirty.store(true, Ordering::Release);
        cx.notify();
    }

    fn select_all(&mut self, cx: &mut Context<Self>) {
        let Some(session) = &self.session else {
            return;
        };
        let mut term = session.term.lock();
        let start = TerminalPoint::new(term.topmost_line(), Column(0));
        let end = TerminalPoint::new(term.bottommost_line(), term.last_column());
        let mut selection = Selection::new(SelectionType::Simple, start, Side::Left);
        selection.update(end, Side::Right);
        term.selection = Some(selection);
        drop(term);
        session.dirty.store(true, Ordering::Release);
        cx.notify();
    }

    fn on_scroll_wheel(
        &mut self,
        event: &ScrollWheelEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = &self.session else {
            return;
        };
        let delta = match event.delta {
            ScrollDelta::Pixels(delta) => f32::from(delta.y) / TERMINAL_CELL_HEIGHT,
            ScrollDelta::Lines(delta) => delta.y,
        };
        self.scroll_accumulator += delta;
        let lines = self.scroll_accumulator.trunc() as i32;
        if lines != 0 {
            self.scroll_accumulator -= lines as f32;
            session.scroll(lines);
            cx.stop_propagation();
            cx.notify();
        }
    }

    fn ensure_cursor_focus_tracking(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.cursor_focus_tracking_started {
            return;
        }
        self.cursor_focus_tracking_started = true;

        let focus_handle = self.focus_handle.clone();
        self._subscriptions.extend([
            cx.observe_window_activation(window, |terminal, window, cx| {
                terminal.update_cursor_blinking(window, cx);
            }),
            cx.on_focus(&focus_handle, window, |terminal, window, cx| {
                terminal.update_cursor_blinking(window, cx);
            }),
            cx.on_blur(&focus_handle, window, |terminal, window, cx| {
                terminal.update_cursor_blinking(window, cx);
            }),
        ]);
        self.update_cursor_blinking(window, cx);
    }

    fn update_cursor_blinking(&mut self, window: &Window, cx: &mut Context<Self>) {
        let focused = window.is_window_active() && self.focus_handle.is_focused(window);
        self.cursor_blink.update(cx, |cursor, cx| {
            if focused {
                cursor.start(cx);
            } else {
                cursor.stop(cx);
            }
        });
    }

    fn pause_cursor_blink(&mut self, cx: &mut Context<Self>) {
        self.cursor_blink.update(cx, |cursor, cx| cursor.pause(cx));
    }
}

impl Focusable for TerminalView {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Render for TerminalView {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.mark_rendered(window, cx);
        self.ensure_wakeup_loop(window, cx);
        self.ensure_cursor_focus_tracking(window, cx);
        let theme = Theme::current(cx);
        let selection_color = theme.selection;
        let viewport = window.viewport_size();
        let panel_width = self.panel_width;
        let body_height = (f32::from(viewport.height) - 48.0 - TERMINAL_TOOLBAR_HEIGHT).max(120.0);
        // The rows are shaped at the font's own advance, so the grid must be
        // sized from that same measured advance or the text runs short of
        // (or past) the panel edge.
        let cell_width = *self.measured_cell_width.get_or_insert_with(|| {
            let text_system = cx.text_system();
            let font_id = text_system.resolve_font(&TERMINAL_FONTS[0]);
            text_system
                .advance(font_id, px(TERMINAL_FONT_SIZE), 'm')
                .map_or(TERMINAL_CELL_WIDTH, |advance| f32::from(advance.width))
        });
        let columns = ((panel_width - TERMINAL_PADDING_X * 2.0) / cell_width)
            .floor()
            .max(TERMINAL_MIN_COLUMNS as f32) as usize;
        let rows = ((body_height - TERMINAL_PADDING_Y * 2.0) / TERMINAL_CELL_HEIGHT)
            .floor()
            .max(TERMINAL_MIN_ROWS as f32) as usize;

        let terminal_focused = window.is_window_active() && self.focus_handle.is_focused(window);
        let cursor_style =
            terminal_cursor_style(terminal_focused, self.cursor_blink.read(cx).visible());
        if let Some(session) = self.session.as_mut() {
            session.resize(columns, rows, cell_width);
        }
        if self.selecting {
            self.set_hovered_link(None);
        } else {
            self.refresh_hovered_link(
                primary_modifier_pressed(&window.modifiers()),
                window.mouse_position(),
            );
        }
        let hovered_link = self.hovered_link.as_ref().map(|link| &link.bounds);
        let has_session = if let Some(session) = self.session.as_ref() {
            session.snapshot(
                &mut self.grid.borrow_mut(),
                theme,
                selection_color,
                cursor_style,
                hovered_link,
            );
            true
        } else {
            false
        };
        let title = if self.title.trim().is_empty() {
            tr!("right_panel.terminal")
        } else {
            self.title.clone()
        };
        let directory = self
            .working_directory
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_owned)
            .unwrap_or_else(|| tr!("workspace.workspace"));

        let mut screen = div()
            .flex_1()
            .min_h_0()
            .min_w_0()
            .overflow_hidden()
            .flex()
            .flex_col()
            .relative()
            .cursor_text()
            .on_mouse_down(MouseButton::Left, cx.listener(Self::on_mouse_down))
            .on_mouse_up(MouseButton::Left, cx.listener(Self::on_mouse_up))
            .on_mouse_up_out(MouseButton::Left, cx.listener(Self::on_mouse_up))
            .on_mouse_move(cx.listener(Self::on_mouse_move))
            .on_mouse_exit(cx.listener(Self::on_mouse_exit))
            .on_modifiers_changed(cx.listener(Self::on_modifiers_changed));

        if self.hovered_link.is_some() {
            screen = screen.cursor_pointer();
        }

        if has_session {
            let (grid_rows, outline_cursor) = {
                let grid = self.grid.borrow();
                (grid.rows.len(), grid.outline_cursor)
            };
            screen = screen.child(TerminalGridElement {
                grid: self.grid.clone(),
                rows: grid_rows,
            });
            if let Some((row, column)) = outline_cursor {
                screen = screen.child(
                    div()
                        .absolute()
                        .left(px(column as f32 * cell_width))
                        .top(px(row as f32 * TERMINAL_CELL_HEIGHT))
                        .w(px(cell_width))
                        .h(px(TERMINAL_CELL_HEIGHT))
                        .border_1()
                        .border_color(theme.text),
                );
            }
        } else {
            screen = screen.child(
                div()
                    .p(px(12.0))
                    .text_size(sp(12.5))
                    .line_height(sp(17.0))
                    .text_color(if self.error.is_some() {
                        theme.danger
                    } else {
                        theme.text_tertiary
                    })
                    .child(
                        self.error
                            .clone()
                            .unwrap_or_else(|| tr!("terminal.starting")),
                    ),
            );
        }

        let grid_bounds = self.grid_bounds.clone();
        screen = screen.child(
            canvas(
                move |bounds, _, _| grid_bounds.set(Some(bounds)),
                |_, _, _, _| {},
            )
            .absolute()
            .inset_0(),
        );

        let context_terminal = cx.entity();
        let screen = context_menu(
            div().size_full().child(screen),
            "terminal-context-menu",
            &self.context_menu,
            move |cx| {
                let has_selection = context_terminal.read(cx).selected_text().is_some();
                let can_paste = cx
                    .read_from_clipboard()
                    .and_then(|item| item.text())
                    .is_some_and(|text| !text.is_empty());
                let has_session = context_terminal.read(cx).session.is_some();

                let copy_terminal = context_terminal.clone();
                let paste_terminal = context_terminal.clone();
                let select_all_terminal = context_terminal.clone();
                vec![
                    MenuItem::new(tr!("menu.copy"), move |_, cx| {
                        let selected_text = { copy_terminal.read(cx).selected_text() };
                        if let Some(text) = selected_text {
                            cx.write_to_clipboard(ClipboardItem::new_string(text));
                        }
                    })
                    .disabled(!has_selection),
                    MenuItem::new(tr!("menu.paste"), move |_, cx| {
                        let Some(text) = cx.read_from_clipboard().and_then(|item| item.text())
                        else {
                            return;
                        };
                        paste_terminal.update(cx, |terminal, cx| terminal.paste(text, cx));
                    })
                    .disabled(!can_paste),
                    MenuItem::Separator,
                    MenuItem::new(tr!("menu.select_all"), move |_, cx| {
                        select_all_terminal.update(cx, |terminal, cx| terminal.select_all(cx));
                    })
                    .disabled(!has_session),
                ]
            },
        );

        let scrollbar = self.session.as_ref().map(|session| {
            scrollbar::vertical(
                &TerminalScrollbarTarget {
                    term: session.term.clone(),
                    dirty: session.dirty.clone(),
                    viewport_rows: session.grid_size.1,
                },
                &self.scrollbar_state,
            )
        });

        let grid = div()
            .flex_1()
            .min_h_0()
            .min_w_0()
            .px(px(TERMINAL_PADDING_X))
            .py(px(TERMINAL_PADDING_Y))
            .bg(theme.terminal)
            .overflow_hidden()
            .flex()
            .flex_col()
            .relative()
            .child(screen)
            .children(scrollbar);

        div()
            .id("alacritty-terminal")
            .key_context("Terminal")
            .track_focus(&self.focus_handle)
            .size_full()
            .min_h_0()
            .min_w_0()
            .flex()
            .flex_col()
            .bg(theme.terminal)
            .child(
                div()
                    .h(px(TERMINAL_TOOLBAR_HEIGHT))
                    .flex_none()
                    .px(px(10.0))
                    .flex()
                    .items_center()
                    .gap(px(7.0))
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.surface)
                    .child(
                        div()
                            .w(px(6.0))
                            .h(px(6.0))
                            .rounded_full()
                            .bg(if self.exited {
                                theme.danger
                            } else {
                                theme.accent
                            }),
                    )
                    .child(
                        div()
                            .min_w_0()
                            .flex_1()
                            .truncate()
                            .text_size(sp(12.5))
                            .text_color(theme.text_secondary)
                            .child(SharedString::from(title.to_owned())),
                    )
                    .child(
                        div()
                            .flex_none()
                            .text_size(sp(12.5))
                            .text_color(theme.text_tertiary)
                            .child(directory),
                    ),
            )
            .child(grid)
            .on_key_down(cx.listener(Self::on_key_down))
            .on_scroll_wheel(cx.listener(Self::on_scroll_wheel))
    }
}

/// Paints the cached grid rows: merged background quads, then one shaped line
/// per row. Only rows rebuilt since their last paint are reshaped.
struct TerminalGridElement {
    grid: Rc<RefCell<TerminalGrid>>,
    rows: usize,
}

impl IntoElement for TerminalGridElement {
    type Element = Self;

    fn into_element(self) -> Self {
        self
    }
}

impl Element for TerminalGridElement {
    type RequestLayoutState = ();
    type PrepaintState = ();

    fn id(&self) -> Option<ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static std::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _: Option<&GlobalElementId>,
        _: Option<&InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, ()) {
        let mut style = Style::default();
        style.size.width = relative(1.).into();
        style.size.height = px(self.rows as f32 * TERMINAL_CELL_HEIGHT).into();
        style.flex_shrink = 0.;
        (window.request_layout(style, [], cx), ())
    }

    fn prepaint(
        &mut self,
        _: Option<&GlobalElementId>,
        _: Option<&InspectorElementId>,
        _: Bounds<Pixels>,
        _: &mut (),
        window: &mut Window,
        _: &mut App,
    ) {
        let mut grid = self.grid.borrow_mut();
        let Some(style) = grid.style else {
            return;
        };
        let text_system = window.text_system().clone();
        let mut text_runs = Vec::new();
        for row in &mut grid.rows {
            if row.paint.is_some() {
                continue;
            }
            text_runs.clear();
            text_runs.extend(row.runs.iter().map(|run| run.style.text_run(run.len)));
            let line = text_system.shape_line(
                SharedString::from(row.text.clone()),
                px(TERMINAL_FONT_SIZE),
                &text_runs,
                None,
            );
            let backgrounds = merged_backgrounds(&line, &row.runs, style.background);
            row.paint = Some(RowPaint { line, backgrounds });
        }
    }

    fn paint(
        &mut self,
        _: Option<&GlobalElementId>,
        _: Option<&InspectorElementId>,
        bounds: Bounds<Pixels>,
        _: &mut (),
        _: &mut (),
        window: &mut Window,
        cx: &mut App,
    ) {
        let grid = self.grid.borrow();
        let line_height = px(TERMINAL_CELL_HEIGHT);
        let row_origin = |index: usize| {
            point(
                bounds.origin.x,
                bounds.origin.y + line_height * index as f32,
            )
        };
        window.with_content_mask(Some(ContentMask { bounds }), |window| {
            for (index, row) in grid.rows.iter().enumerate() {
                let Some(paint) = &row.paint else {
                    continue;
                };
                let origin = row_origin(index);
                for &(left, right, color) in &paint.backgrounds {
                    window.paint_quad(fill(
                        Bounds::new(
                            point(origin.x + left, origin.y),
                            size(right - left, line_height),
                        ),
                        color,
                    ));
                }
            }
            for (index, row) in grid.rows.iter().enumerate() {
                if let Some(paint) = &row.paint {
                    let _ = paint.line.paint(
                        row_origin(index),
                        line_height,
                        TextAlign::Left,
                        None,
                        window,
                        cx,
                    );
                }
            }
        });
    }
}

/// Background quads for a shaped row: adjacent runs sharing a background
/// merge into one quad, and the terminal's own background (painted by the
/// container) is skipped. Edges follow the shaped glyph positions, as a
/// `StyledText` run background would.
fn merged_backgrounds(
    layout: &LineLayout,
    runs: &[TerminalRun],
    default_background: Hsla,
) -> Vec<(Pixels, Pixels, Hsla)> {
    let mut glyphs = layout
        .runs
        .iter()
        .flat_map(|run| run.glyphs.iter())
        .peekable();
    // Byte offsets arrive in increasing order, so one forward walk over the
    // glyphs serves every lookup.
    let mut x_for_index = |index: usize| {
        while let Some(glyph) = glyphs.peek() {
            if glyph.index >= index {
                return glyph.position.x;
            }
            glyphs.next();
        }
        layout.width
    };

    let mut spans = Vec::new();
    let mut pending: Option<(usize, usize, Hsla)> = None;
    let mut start = 0;
    for run in runs {
        let end = start + run.len;
        let background = run.style.background;
        match pending.as_mut() {
            Some((_, pending_end, color)) if *color == background => *pending_end = end,
            _ => {
                if let Some((from, to, color)) = pending.take() {
                    spans.push((x_for_index(from), x_for_index(to), color));
                }
                if background != default_background {
                    pending = Some((start, end, background));
                }
            }
        }
        start = end;
    }
    if let Some((from, to, color)) = pending {
        spans.push((x_for_index(from), x_for_index(to), color));
    }
    spans
}

fn terminal_cursor_style(focused: bool, blink_visible: bool) -> TerminalCursorStyle {
    if !focused {
        TerminalCursorStyle::Outline
    } else if blink_visible {
        TerminalCursorStyle::Solid
    } else {
        TerminalCursorStyle::Hidden
    }
}

fn terminal_grid_point(
    bounds: Bounds<Pixels>,
    position: Point<Pixels>,
    cell_width: f32,
    columns: usize,
    rows: usize,
    display_offset: usize,
    clamp_to_grid: bool,
) -> Option<(TerminalPoint, Side)> {
    if columns == 0 || rows == 0 || (!clamp_to_grid && !bounds.contains(&position)) {
        return None;
    }

    let x = f32::from(position.x - bounds.origin.x);
    let y = f32::from(position.y - bounds.origin.y);
    let max_x = columns as f32 * cell_width;
    let max_y = rows as f32 * TERMINAL_CELL_HEIGHT;
    let x = x.clamp(0.0, max_x);
    let y = y.clamp(0.0, max_y);
    let column = ((x / cell_width).floor() as usize).min(columns - 1);
    let viewport_row = ((y / TERMINAL_CELL_HEIGHT).floor() as usize).min(rows - 1) as i32;
    let side = if x >= max_x || x % cell_width >= cell_width / 2.0 {
        Side::Right
    } else {
        Side::Left
    };
    let line = Line(viewport_row - display_offset.min(i32::MAX as usize) as i32);

    Some((TerminalPoint::new(line, Column(column)), side))
}

/// Apply the same delimiter heuristics as Alacritty's hint system.
fn post_process_terminal_link<T: EventListener>(
    term: &Term<T>,
    regex_match: &Match,
) -> Option<Match> {
    let mut iter = term.grid().iter_from(*regex_match.start());
    let mut character = iter.cell().c;
    let end = *regex_match.end();
    let mut open_parens = 0;
    let mut open_brackets = 0;

    loop {
        match character {
            '(' => open_parens += 1,
            '[' => open_brackets += 1,
            ')' if open_parens == 0 => {
                iter.prev();
                break;
            }
            ')' => open_parens -= 1,
            ']' if open_brackets == 0 => {
                iter.prev();
                break;
            }
            ']' => open_brackets -= 1,
            _ => {}
        }

        if iter.point() == end {
            break;
        }
        let Some(indexed) = iter.next() else {
            break;
        };
        character = indexed.cell.c;
    }

    let start = *regex_match.start();
    while iter.point() != start {
        if !matches!(
            character,
            '.' | ',' | ':' | ';' | '?' | '!' | '(' | '[' | '\''
        ) {
            break;
        }
        let Some(indexed) = iter.prev() else {
            break;
        };
        character = indexed.cell.c;
    }

    (start <= iter.point()).then(|| start..=iter.point())
}

fn plain_link_at<T: EventListener>(
    term: &Term<T>,
    regex: &mut RegexSearch,
    point: TerminalPoint,
) -> Option<(String, Match)> {
    let mut start = term.line_search_left(point);
    let mut end = term.line_search_right(point);
    start.line = start.line.max(point.line - MAX_TERMINAL_LINK_SEARCH_LINES);
    end.line = end.line.min(point.line + MAX_TERMINAL_LINK_SEARCH_LINES);

    let raw_match = RegexIter::new(start, end, Direction::Right, term, regex)
        .find(|bounds| bounds.contains(&point))?;
    let raw_end = *raw_match.end();
    let mut next_match = Some(raw_match);

    // Post-processing can split a greedy match at an unmatched closing
    // bracket. Continue inside the original range in case the clicked URL is
    // a later segment of that match.
    while let Some(regex_match) = next_match {
        let processed = post_process_terminal_link(term, &regex_match);
        if processed
            .as_ref()
            .is_some_and(|bounds| bounds.contains(&point))
        {
            let bounds = processed.unwrap();
            let value = term.bounds_to_string(*bounds.start(), *bounds.end());
            return Some((value, bounds));
        }

        let next_start = processed
            .as_ref()
            .map_or_else(|| *regex_match.start(), |bounds| *bounds.end())
            .add(term, Boundary::Grid, 1);
        if next_start > raw_end {
            return None;
        }
        next_match = term.regex_search_right(regex, next_start, raw_end);
    }

    None
}

fn hyperlink_at<T: EventListener>(term: &Term<T>, point: TerminalPoint) -> Option<(String, Match)> {
    let hyperlink = term.grid()[point].hyperlink()?;
    let grid = term.grid();

    let mut end = point;
    for cell in grid.iter_from(point) {
        if cell.hyperlink().as_ref() == Some(&hyperlink) {
            end = cell.point;
        } else {
            break;
        }
    }

    let mut start = point;
    let mut iter = grid.iter_from(point);
    while let Some(cell) = iter.prev() {
        if cell.hyperlink().as_ref() == Some(&hyperlink) {
            start = cell.point;
        } else {
            break;
        }
    }

    Some((hyperlink.uri().to_owned(), start..=end))
}

fn terminal_link_target(value: &str, working_directory: &Path) -> Option<TerminalLinkTarget> {
    if let Some(path) = existing_terminal_file_path(value, working_directory) {
        return Some(TerminalLinkTarget::File(path));
    }

    let url = url::Url::parse(value).ok()?;
    if url.scheme() == "file" {
        return None;
    }
    Some(TerminalLinkTarget::Url(url.to_string()))
}

fn existing_terminal_file_path(value: &str, working_directory: &Path) -> Option<PathBuf> {
    let mut path = match url::Url::parse(value) {
        Ok(url) if url.scheme() == "file" => url.to_file_path().ok()?,
        Ok(_) => return None,
        Err(_) => {
            if let Some(relative) = value.strip_prefix("~/") {
                dirs::home_dir()?.join(relative)
            } else {
                let path = Path::new(value);
                if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    working_directory.join(path)
                }
            }
        }
    };

    loop {
        if path.exists() {
            return Some(path);
        }
        let value = path.to_string_lossy();
        let (prefix, suffix) = value.rsplit_once(':')?;
        if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        path = PathBuf::from(prefix);
    }
}

fn bracketed_paste(text: String, mode: TermMode) -> Vec<u8> {
    if mode.contains(TermMode::BRACKETED_PASTE) {
        format!("\x1b[200~{text}\x1b[201~").into_bytes()
    } else {
        text.into_bytes()
    }
}

fn terminal_key_bytes(keystroke: &Keystroke, mode: TermMode) -> Option<Vec<u8>> {
    let modifiers = keystroke.modifiers;
    let key = keystroke.key.as_str();

    if modifiers.platform {
        #[cfg(target_os = "macos")]
        return match key {
            "left" => Some(vec![0x01]),
            "right" => Some(vec![0x05]),
            "backspace" => Some(vec![0x15]),
            _ => None,
        };

        #[cfg(not(target_os = "macos"))]
        return None;
    }

    let modifier = 1
        + u8::from(modifiers.shift)
        + u8::from(modifiers.alt) * 2
        + u8::from(modifiers.control) * 4;
    let app_cursor = mode.contains(TermMode::APP_CURSOR);
    let special = match key {
        "enter" | "return" => Some("\r".to_owned()),
        "tab" if modifiers.shift => Some("\x1b[Z".to_owned()),
        "tab" => Some("\t".to_owned()),
        "backspace" => Some("\x7f".to_owned()),
        "escape" => Some("\x1b".to_owned()),
        "up" => Some(cursor_sequence('A', modifier, app_cursor)),
        "down" => Some(cursor_sequence('B', modifier, app_cursor)),
        "right" => Some(cursor_sequence('C', modifier, app_cursor)),
        "left" => Some(cursor_sequence('D', modifier, app_cursor)),
        "home" => Some(csi_sequence('H', modifier)),
        "end" => Some(csi_sequence('F', modifier)),
        "insert" => Some(tilde_sequence(2, modifier)),
        "delete" | "forwarddelete" => Some(tilde_sequence(3, modifier)),
        "pageup" => Some(tilde_sequence(5, modifier)),
        "pagedown" => Some(tilde_sequence(6, modifier)),
        "f1" => Some(function_sequence('P', modifier)),
        "f2" => Some(function_sequence('Q', modifier)),
        "f3" => Some(function_sequence('R', modifier)),
        "f4" => Some(function_sequence('S', modifier)),
        "f5" => Some(tilde_sequence(15, modifier)),
        "f6" => Some(tilde_sequence(17, modifier)),
        "f7" => Some(tilde_sequence(18, modifier)),
        "f8" => Some(tilde_sequence(19, modifier)),
        "f9" => Some(tilde_sequence(20, modifier)),
        "f10" => Some(tilde_sequence(21, modifier)),
        "f11" => Some(tilde_sequence(23, modifier)),
        "f12" => Some(tilde_sequence(24, modifier)),
        _ => None,
    };
    if let Some(special) = special {
        return Some(special.into_bytes());
    }

    let text = keystroke
        .key_char
        .as_deref()
        .or_else(|| (key.chars().count() == 1).then_some(key))?;
    let mut bytes = if modifiers.control {
        control_bytes(text)?
    } else {
        text.as_bytes().to_vec()
    };
    if modifiers.alt {
        bytes.insert(0, 0x1b);
    }
    Some(bytes)
}

fn control_bytes(text: &str) -> Option<Vec<u8>> {
    let character = text.chars().next()?.to_ascii_lowercase();
    let byte = match character {
        ' ' | '@' => 0,
        'a'..='z' => character as u8 - b'a' + 1,
        '[' => 27,
        '\\' => 28,
        ']' => 29,
        '^' => 30,
        '_' => 31,
        '?' => 127,
        _ => return None,
    };
    Some(vec![byte])
}

fn cursor_sequence(final_byte: char, modifier: u8, app_cursor: bool) -> String {
    if modifier == 1 {
        format!("\x1b{}{}", if app_cursor { 'O' } else { '[' }, final_byte)
    } else {
        format!("\x1b[1;{modifier}{final_byte}")
    }
}

fn csi_sequence(final_byte: char, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1b[{final_byte}")
    } else {
        format!("\x1b[1;{modifier}{final_byte}")
    }
}

fn function_sequence(final_byte: char, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1bO{final_byte}")
    } else {
        format!("\x1b[1;{modifier}{final_byte}")
    }
}

fn tilde_sequence(number: u8, modifier: u8) -> String {
    if modifier == 1 {
        format!("\x1b[{number}~")
    } else {
        format!("\x1b[{number};{modifier}~")
    }
}

fn resolve_color(
    color: Color,
    colors: &alacritty_terminal::term::color::Colors,
    theme: Theme,
    foreground: bool,
) -> Hsla {
    let rgb = match color {
        Color::Spec(color) => Some(color),
        Color::Indexed(index) => {
            colors[index as usize].or_else(|| Some(terminal_rgb(index as usize, theme.is_dark)))
        }
        Color::Named(NamedColor::Foreground | NamedColor::BrightForeground) => None,
        Color::Named(NamedColor::Background) => return theme.terminal,
        Color::Named(NamedColor::Cursor) => return theme.text,
        Color::Named(named) => {
            colors[named as usize].or_else(|| Some(terminal_rgb(named as usize, theme.is_dark)))
        }
    };
    rgb.map(rgb_to_hsla).unwrap_or(if foreground {
        theme.text
    } else {
        theme.terminal
    })
}

fn rgb_to_hsla(color: Rgb) -> Hsla {
    rgb((u32::from(color.r) << 16) | (u32::from(color.g) << 8) | u32::from(color.b)).into()
}

fn terminal_rgb(index: usize, is_dark: bool) -> Rgb {
    // Tomorrow Night on dark surfaces, Tomorrow on light — same hues tuned
    // for the opposite background so ANSI text stays legible on white.
    const ANSI_DARK: [u32; 16] = [
        0x1d1f21, 0xcc6666, 0xb5bd68, 0xf0c674, 0x81a2be, 0xb294bb, 0x8abeb7, 0xc5c8c6, 0x666666,
        0xd54e53, 0xb9ca4a, 0xe7c547, 0x7aa6da, 0xc397d8, 0x70c0b1, 0xeaeaea,
    ];
    const ANSI_LIGHT: [u32; 16] = [
        0x000000, 0xc82829, 0x718c00, 0xeab700, 0x4271ae, 0x8959a8, 0x3e999f, 0xc7c7c7, 0x8e908c,
        0xc82829, 0x718c00, 0xeab700, 0x4271ae, 0x8959a8, 0x3e999f, 0xffffff,
    ];
    let ansi = if is_dark { &ANSI_DARK } else { &ANSI_LIGHT };
    let value = match index {
        0..=15 => ansi[index],
        16..=231 => {
            let index = index - 16;
            let channel = |value: usize| {
                if value == 0 {
                    0
                } else {
                    55 + value as u32 * 40
                }
            };
            let red = channel(index / 36);
            let green = channel((index / 6) % 6);
            let blue = channel(index % 6);
            (red << 16) | (green << 8) | blue
        }
        232..=255 => {
            let value = 8 + (index as u32 - 232) * 10;
            (value << 16) | (value << 8) | value
        }
        value
            if value >= NamedColor::DimBlack as usize && value <= NamedColor::DimWhite as usize =>
        {
            let base = ansi[value - NamedColor::DimBlack as usize];
            // Faint fades toward the surface: black on dark, white on light.
            let dim = |channel: u32| {
                if is_dark {
                    channel * 2 / 3
                } else {
                    channel + (255 - channel) / 3
                }
            };
            (dim((base >> 16) & 0xff) << 16) | (dim((base >> 8) & 0xff) << 8) | dim(base & 0xff)
        }
        _ => {
            if is_dark {
                0xe5e5e5
            } else {
                0x242424
            }
        }
    };
    Rgb {
        r: (value >> 16) as u8,
        g: (value >> 8) as u8,
        b: value as u8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{Modifiers, point, size};

    fn key(key: &str, key_char: Option<&str>, modifiers: Modifiers) -> Keystroke {
        Keystroke {
            key: key.into(),
            key_char: key_char.map(str::to_owned),
            modifiers,
        }
    }

    fn parse_terminal(input: &[u8]) -> Term<VoidListener> {
        let dimensions = TerminalDimensions {
            columns: 40,
            rows: 3,
        };
        let mut term = Term::new(Config::default(), &dimensions, VoidListener);
        let mut processor: Processor = Processor::new();
        processor.advance(&mut term, input);
        term
    }

    fn terminal_point_for(content: &str, needle: &str) -> TerminalPoint {
        let offset = content.find(needle).unwrap();
        TerminalPoint::new(Line((offset / 40) as i32), Column(offset % 40))
    }

    fn plain_link_value(term: &Term<VoidListener>, point: TerminalPoint) -> Option<String> {
        let mut regex = RegexSearch::new(TERMINAL_LINK_REGEX).unwrap();
        plain_link_at(term, &mut regex, point).map(|(value, _)| value)
    }

    #[test]
    fn detects_plain_and_osc8_terminal_links() {
        let content = "visit (https://example.com/docs). next";
        let term = parse_terminal(content.as_bytes());
        assert_eq!(
            plain_link_value(&term, terminal_point_for(content, "example")),
            Some("https://example.com/docs".to_owned())
        );
        assert_eq!(
            plain_link_value(&term, terminal_point_for(content, ").")),
            None
        );

        let term = parse_terminal(b"x\x1b]8;;https://waku.gg\x1b\\Waku\x1b]8;;\x1b\\ y");
        let (value, bounds) = hyperlink_at(&term, TerminalPoint::new(Line(0), Column(2))).unwrap();
        assert_eq!(value, "https://waku.gg");
        assert_eq!(
            bounds,
            TerminalPoint::new(Line(0), Column(1))..=TerminalPoint::new(Line(0), Column(4))
        );
    }

    #[test]
    fn detects_links_across_soft_wrapped_lines() {
        let content = "prefix https://example.com/a/very/long/path/that/wraps suffix";
        let term = parse_terminal(content.as_bytes());
        let mut regex = RegexSearch::new(TERMINAL_LINK_REGEX).unwrap();
        let (value, bounds) =
            plain_link_at(&term, &mut regex, terminal_point_for(content, "that")).unwrap();

        assert_eq!(value, "https://example.com/a/very/long/path/that/wraps");
        assert_eq!(*bounds.start(), terminal_point_for(content, "https"));
        assert_eq!(bounds.end().line, Line(1));
    }

    #[test]
    fn resolves_terminal_urls_and_existing_project_paths() {
        let working_directory = Path::new(env!("CARGO_MANIFEST_DIR"));
        assert_eq!(
            terminal_link_target("https://example.com/docs", working_directory),
            Some(TerminalLinkTarget::Url(
                "https://example.com/docs".to_owned()
            ))
        );
        assert_eq!(
            terminal_link_target("src/terminal.rs:42:8", working_directory),
            Some(TerminalLinkTarget::File(
                working_directory.join("src/terminal.rs")
            ))
        );
        assert_eq!(
            terminal_link_target("src/does-not-exist.rs", working_directory),
            None
        );
    }

    #[test]
    fn encodes_terminal_control_and_cursor_keys() {
        assert_eq!(
            terminal_key_bytes(
                &key(
                    "c",
                    Some("c"),
                    Modifiers {
                        control: true,
                        ..Default::default()
                    }
                ),
                TermMode::empty()
            ),
            Some(vec![3])
        );
        assert_eq!(
            terminal_key_bytes(&key("up", None, Modifiers::default()), TermMode::APP_CURSOR),
            Some(b"\x1bOA".to_vec())
        );
        assert_eq!(
            terminal_key_bytes(
                &key(
                    "left",
                    None,
                    Modifiers {
                        control: true,
                        ..Default::default()
                    }
                ),
                TermMode::empty()
            ),
            Some(b"\x1b[1;5D".to_vec())
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_terminal_clipboard_shortcuts_preserve_ctrl_c_for_sigint() {
        let control = Modifiers {
            control: true,
            ..Default::default()
        };
        let control_shift = Modifiers {
            control: true,
            shift: true,
            ..Default::default()
        };

        assert!(primary_modifier_pressed(&control));
        assert!(!terminal_clipboard_modifier_pressed(&control));
        assert!(terminal_clipboard_modifier_pressed(&control_shift));
    }

    #[test]
    fn wraps_bracketed_paste_only_when_requested() {
        assert_eq!(
            bracketed_paste("hello".into(), TermMode::BRACKETED_PASTE),
            b"\x1b[200~hello\x1b[201~"
        );
        assert_eq!(bracketed_paste("hello".into(), TermMode::empty()), b"hello");
    }

    #[test]
    fn cursor_blinks_while_focused_and_outlines_when_unfocused() {
        assert_eq!(
            terminal_cursor_style(false, false),
            TerminalCursorStyle::Outline
        );
        assert_eq!(
            terminal_cursor_style(false, true),
            TerminalCursorStyle::Outline
        );
        assert_eq!(
            terminal_cursor_style(true, false),
            TerminalCursorStyle::Hidden
        );
        assert_eq!(
            terminal_cursor_style(true, true),
            TerminalCursorStyle::Solid
        );
    }

    #[test]
    fn maps_pointer_positions_into_scrollback_grid_coordinates() {
        let bounds = Bounds::new(point(px(10.0), px(20.0)), size(px(72.0), px(64.0)));
        let position = point(
            px(10.0 + TERMINAL_CELL_WIDTH * 2.0 + 5.0),
            px(20.0 + TERMINAL_CELL_HEIGHT + 8.0),
        );

        assert_eq!(
            terminal_grid_point(bounds, position, TERMINAL_CELL_WIDTH, 10, 4, 3, false),
            Some((TerminalPoint::new(Line(-2), Column(2)), Side::Right))
        );
        assert_eq!(
            terminal_grid_point(
                bounds,
                point(px(0.0), px(0.0)),
                TERMINAL_CELL_WIDTH,
                10,
                4,
                3,
                false
            ),
            None
        );
    }

    #[test]
    fn clamps_selection_drags_to_the_terminal_grid_edges() {
        let bounds = Bounds::new(point(px(10.0), px(20.0)), size(px(72.0), px(64.0)));

        assert_eq!(
            terminal_grid_point(
                bounds,
                point(px(500.0), px(500.0)),
                TERMINAL_CELL_WIDTH,
                10,
                4,
                0,
                true
            ),
            Some((TerminalPoint::new(Line(3), Column(9)), Side::Right))
        );
        assert_eq!(
            terminal_grid_point(
                bounds,
                point(px(-50.0), px(-50.0)),
                TERMINAL_CELL_WIDTH,
                10,
                4,
                3,
                true
            ),
            Some((TerminalPoint::new(Line(-3), Column(0)), Side::Left))
        );
    }

    const GRID_COLUMNS: usize = 40;
    const GRID_ROWS: usize = 10;

    fn grid_terminal() -> Term<VoidListener> {
        let dimensions = TerminalDimensions {
            columns: GRID_COLUMNS,
            rows: GRID_ROWS,
        };
        let config = Config {
            scrolling_history: 100,
            ..Default::default()
        };
        Term::new(config, &dimensions, VoidListener)
    }

    fn update_grid(
        grid: &mut TerminalGrid,
        term: &mut Term<VoidListener>,
        theme: Theme,
        hovered_link: Option<&Match>,
    ) {
        grid.update(
            term,
            (GRID_COLUMNS, GRID_ROWS),
            theme,
            theme.selection,
            TerminalCursorStyle::Solid,
            hovered_link,
        );
    }

    /// The rows rebuilt since the previous call, read from which rows lost
    /// their paint data. Marks every row painted again afterwards.
    fn rebuilt(grid: &mut TerminalGrid) -> Vec<usize> {
        let rows = grid
            .rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.paint.is_none())
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), grid.rebuilt_rows);
        for row in &mut grid.rows {
            row.paint = Some(RowPaint {
                line: ShapedLine::default(),
                backgrounds: Vec::new(),
            });
        }
        rows
    }

    #[test]
    fn grid_rebuilds_only_damaged_rows() {
        let theme = Theme::dark();
        let mut term = grid_terminal();
        let mut processor: Processor = Processor::new();
        processor.advance(&mut term, b"first\r\nsecond");
        let mut grid = TerminalGrid::new();

        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(rebuilt(&mut grid), (0..GRID_ROWS).collect::<Vec<_>>());

        // Nothing changed: only the cursor row, which alacritty always
        // reports damaged.
        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(rebuilt(&mut grid), vec![1]);

        // Writing on row 5 damages it and the row the cursor left.
        processor.advance(&mut term, b"\x1b[6;1Hhello");
        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(rebuilt(&mut grid), vec![1, 5]);
        assert!(grid.rows[5].text.starts_with("hello"));
        assert!(grid.rows[0].text.starts_with("first"));
    }

    #[test]
    fn grid_invalidates_rows_damage_does_not_cover() {
        let theme = Theme::dark();
        let mut term = grid_terminal();
        let mut processor: Processor = Processor::new();
        processor.advance(&mut term, b"\x1b[6;1Hcursor here");
        let mut grid = TerminalGrid::new();
        update_grid(&mut grid, &mut term, theme, None);
        rebuilt(&mut grid);

        // Selection is not part of alacritty's damage: the selected rows and
        // the always-damaged cursor row are rebuilt, nothing else.
        let mut selection = Selection::new(
            SelectionType::Simple,
            TerminalPoint::new(Line(2), Column(0)),
            Side::Left,
        );
        selection.update(TerminalPoint::new(Line(3), Column(5)), Side::Right);
        term.selection = Some(selection);
        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(rebuilt(&mut grid), vec![2, 3, 5]);
        assert_eq!(grid.rows[2].runs[0].style.background, theme.selection);

        term.selection = None;
        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(rebuilt(&mut grid), vec![2, 3, 5]);
        assert_eq!(grid.rows[2].runs[0].style.background, theme.terminal);

        // A hovered link underlines its rows only.
        let link = TerminalPoint::new(Line(7), Column(0))..=TerminalPoint::new(Line(7), Column(4));
        update_grid(&mut grid, &mut term, theme, Some(&link));
        assert_eq!(rebuilt(&mut grid), vec![5, 7]);
        assert!(grid.rows[7].runs[0].style.underline);
        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(rebuilt(&mut grid), vec![5, 7]);

        // A cursor style change touches the cursor row only.
        grid.update(
            &mut term,
            (GRID_COLUMNS, GRID_ROWS),
            theme,
            theme.selection,
            TerminalCursorStyle::Outline,
            None,
        );
        assert_eq!(rebuilt(&mut grid), vec![5]);
        assert_eq!(grid.outline_cursor, Some((5, 11)));

        // Theme and grid size are grid-wide.
        update_grid(&mut grid, &mut term, Theme::light(), None);
        assert_eq!(grid.rebuilt_rows, GRID_ROWS);
        rebuilt(&mut grid);
        grid.update(
            &mut term,
            (GRID_COLUMNS, GRID_ROWS - 1),
            Theme::light(),
            Theme::light().selection,
            TerminalCursorStyle::Solid,
            None,
        );
        assert_eq!(grid.rebuilt_rows, GRID_ROWS - 1);
    }

    #[test]
    fn grid_rebuilds_everything_when_the_view_scrolls() {
        let theme = Theme::dark();
        let mut term = grid_terminal();
        let mut processor: Processor = Processor::new();
        for line in 0..30 {
            processor.advance(&mut term, format!("line {line}\r\n").as_bytes());
        }
        let mut grid = TerminalGrid::new();
        update_grid(&mut grid, &mut term, theme, None);
        rebuilt(&mut grid);

        term.scroll_display(Scroll::Delta(3));
        update_grid(&mut grid, &mut term, theme, None);
        assert_eq!(grid.rebuilt_rows, GRID_ROWS);
        assert!(grid.rows[0].text.starts_with("line 18"));
    }

    #[test]
    fn incremental_rows_match_a_full_rebuild() {
        let theme = Theme::dark();
        let mut term = grid_terminal();
        let mut processor: Processor = Processor::new();
        let mut grid = TerminalGrid::new();
        let steps: [&[u8]; 6] = [
            b"\x1b[31mred\x1b[0m plain\r\n",
            b"\x1b[3;5H\x1b[1;44mbold on blue\x1b[0m \x1b[7minverse\x1b[0m",
            "e\u{301} wide \u{4e2d} end \x1b[2;3mdim italic\x1b[0m".as_bytes(),
            b"\x1b[5;1H\x1b[2K\x1b[4munderlined\x1b[24m \x1b[9mstruck\x1b[0m",
            b"\x1b[38;2;10;200;30mtruecolor\x1b[48;5;100m indexed\x1b[0m",
            b"\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\nscrolled off the top",
        ];
        for (index, step) in steps.into_iter().enumerate() {
            processor.advance(&mut term, step);
            update_grid(&mut grid, &mut term, theme, None);
            let mut fresh = TerminalGrid::new();
            update_grid(&mut fresh, &mut term, theme, None);
            for (row, (cached, full)) in grid.rows.iter().zip(&fresh.rows).enumerate() {
                assert_eq!(cached.text, full.text, "row {row} text");
                assert_eq!(cached.runs, full.runs, "row {row} runs");
            }
            if index == 2 {
                // Zero-width marks stay with their base character, and a
                // wide character's spacer cell keeps the row on the grid.
                assert!(
                    grid.rows[2].text.contains("e\u{301} wide \u{4e2d}  end"),
                    "{:?}",
                    grid.rows[2].text
                );
            }
        }
    }

    #[test]
    fn merged_backgrounds_skip_default_and_join_neighbours() {
        let theme = Theme::dark();
        let style = |background| TerminalRunStyle {
            foreground: theme.text,
            background,
            bold: false,
            italic: false,
            underline: false,
            strikeout: false,
        };
        let mut bold = style(theme.selection);
        bold.bold = true;
        let runs = [
            TerminalRun {
                len: 2,
                style: style(theme.terminal),
            },
            TerminalRun {
                len: 3,
                style: style(theme.selection),
            },
            TerminalRun {
                len: 1,
                style: bold,
            },
            TerminalRun {
                len: 4,
                style: style(theme.terminal),
            },
        ];
        // An unshaped layout puts every edge at its width, so check the span
        // structure rather than positions.
        let spans = merged_backgrounds(&LineLayout::default(), &runs, theme.terminal);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].2, theme.selection);
    }

    #[test]
    fn hidden_terminal_output_sets_dirty_without_waking() {
        let (wakeup, wakeups) = smol::channel::bounded(1);
        let (ui_events, _ui_events) = unbounded();
        let proxy = TerminalEventProxy {
            dirty: Arc::new(AtomicBool::new(false)),
            wakeup,
            visible: Arc::new(AtomicBool::new(false)),
            sender: Arc::new(OnceLock::new()),
            ui_events,
            window_size: Arc::new(Mutex::new(WindowSize {
                num_lines: 10,
                num_cols: 40,
                cell_width: 8,
                cell_height: 18,
            })),
            dark_theme: Arc::new(AtomicBool::new(true)),
        };

        for _ in 0..100 {
            proxy.send_event(Event::Wakeup);
        }
        assert!(proxy.dirty.load(Ordering::Acquire));
        assert!(wakeups.try_recv().is_err());

        // Visible: a burst coalesces into one queued wakeup.
        proxy.visible.store(true, Ordering::SeqCst);
        for _ in 0..100 {
            proxy.send_event(Event::Wakeup);
        }
        assert!(wakeups.try_recv().is_ok());
        assert!(wakeups.try_recv().is_err());

        // UI events still reach a hidden view.
        proxy.visible.store(false, Ordering::SeqCst);
        proxy.send_event(Event::Title("vim".into()));
        assert!(wakeups.try_recv().is_ok());
    }
}
