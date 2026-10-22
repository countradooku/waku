//! A daemon terminal's retained output (RFC 0003 §5.4).
//!
//! Every byte the PTY produces is broadcast live, unchanged. A copy also goes
//! into [`TerminalHistory`], through [`QueryFilter`], so a client attaching
//! later can replay a bounded tail into a fresh emulator:
//!
//! - The history keeps at most [`TERMINAL_RETAINED_LINES`] lines and
//!   [`TERMINAL_RETAINED_BYTES`] bytes. Eviction cuts on a line boundary
//!   when one is close, and otherwise on a UTF-8 boundary, so it never splits
//!   a code point.
//! - Terminal queries (device attributes, cursor position, colour and
//!   clipboard reads, mode and setting requests) and the replies a terminal
//!   sends back are stripped. Replayed, a query would make the attaching
//!   emulator answer it again, typing the reply into whatever the shell runs
//!   now; a reply echoed by the shell would print as garbage. Clipboard
//!   writes (OSC 52) and APC/PM/SOS strings go too: a replay must not touch
//!   the viewer's clipboard, and nothing the client renders uses them.

use std::collections::VecDeque;

use waku_protocol::{TERMINAL_RETAINED_BYTES, TERMINAL_RETAINED_LINES};

const ESC: u8 = 0x1b;
const BEL: u8 = 0x07;
const CAN: u8 = 0x18;
const SUB: u8 = 0x1a;

/// How far past the byte cut point eviction (or a replay cut) looks for a
/// line boundary before settling for a UTF-8 boundary.
const LINE_BOUNDARY_SLACK: u64 = 64 * 1024;
/// A CSI sequence longer than this is passed through undecided.
const MAX_CSI_BYTES: usize = 64;
/// An escape sequence with intermediates longer than this is passed through.
const MAX_ESCAPE_BYTES: usize = 16;
/// The prefix of an OSC or DCS string buffered while deciding its fate.
/// Longer strings are passed through.
const MAX_STRING_PREFIX: usize = 256;

/// Output retained for replay, bounded by lines and bytes.
pub(crate) struct TerminalHistory {
    bytes: VecDeque<u8>,
    /// The position of every retained `\n`, counted like `base`.
    newlines: VecDeque<u64>,
    /// The position of `bytes[0]` among every byte ever pushed.
    base: u64,
    max_lines: usize,
    max_bytes: usize,
    /// Output has been evicted since the history was created or cleared.
    evicted: bool,
}

impl Default for TerminalHistory {
    fn default() -> Self {
        Self::with_limits(TERMINAL_RETAINED_LINES, TERMINAL_RETAINED_BYTES)
    }
}

impl TerminalHistory {
    pub(crate) fn with_limits(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            bytes: VecDeque::new(),
            newlines: VecDeque::new(),
            base: 0,
            max_lines: max_lines.max(1),
            max_bytes: max_bytes.max(4),
            evicted: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Retained `\n`-terminated lines.
    #[cfg(test)]
    pub(crate) fn lines(&self) -> usize {
        self.newlines.len()
    }

    fn end(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }

    pub(crate) fn push(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let end = self.end();
        self.newlines.extend(
            data.iter()
                .enumerate()
                .filter(|(_, byte)| **byte == b'\n')
                .map(|(index, _)| end + index as u64),
        );
        self.bytes.extend(data);
        self.evict();
    }

    pub(crate) fn clear(&mut self) {
        self.base = self.end();
        self.bytes.clear();
        self.bytes.shrink_to(64 * 1024);
        self.newlines.clear();
        self.evicted = false;
    }

    fn evict(&mut self) {
        if self.newlines.len() > self.max_lines {
            let oldest_kept = self.newlines.len() - self.max_lines;
            let start = self.newlines[oldest_kept - 1] + 1;
            self.drop_before(start);
        }
        if self.bytes.len() > self.max_bytes {
            let cut = self.end() - self.max_bytes as u64;
            let start = self.boundary_at_or_after(cut);
            self.drop_before(start);
        }
    }

    /// The first position at or after `position` worth starting at: just past
    /// a nearby `\n`, or else the next UTF-8 character boundary.
    fn boundary_at_or_after(&self, position: u64) -> u64 {
        let line = self.newlines.partition_point(|&newline| newline < position);
        if let Some(&newline) = self.newlines.get(line)
            && newline - position < LINE_BOUNDARY_SLACK
        {
            return newline + 1;
        }
        // A code point has at most three continuation bytes; stepping over
        // at most that many keeps malformed input from walking far.
        let mut index = (position - self.base) as usize;
        let mut steps = 0;
        while steps < 3 && index < self.bytes.len() && is_utf8_continuation(self.bytes[index]) {
            index += 1;
            steps += 1;
        }
        self.base + index as u64
    }

    fn drop_before(&mut self, position: u64) {
        let count = (position.saturating_sub(self.base) as usize).min(self.bytes.len());
        if count == 0 {
            return;
        }
        self.bytes.drain(..count);
        self.base += count as u64;
        while self
            .newlines
            .front()
            .is_some_and(|&newline| newline < self.base)
        {
            self.newlines.pop_front();
        }
        self.evicted = true;
    }

    /// At most `max_bytes` of the newest output, starting on a line or UTF-8
    /// boundary, and whether anything older exists that it leaves out.
    pub(crate) fn tail(&self, max_bytes: usize) -> (Vec<u8>, bool) {
        if self.bytes.len() <= max_bytes {
            return (self.bytes.iter().copied().collect(), self.evicted);
        }
        let start = self.boundary_at_or_after(self.end() - max_bytes as u64);
        let start = (start - self.base) as usize;
        (self.bytes.range(start..).copied().collect(), true)
    }
}

fn is_utf8_continuation(byte: u8) -> bool {
    byte & 0b1100_0000 == 0b1000_0000
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StringKind {
    /// Operating system command (`ESC ]`), ended by BEL or ST.
    Osc,
    /// Device control string (`ESC P`), ended by ST.
    Dcs,
    /// APC, PM or SOS (`ESC _`, `ESC ^`, `ESC X`), ended by ST.
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Ground,
    /// After ESC; `pending` holds the sequence so far.
    Escape,
    /// Inside `ESC [`; `pending` holds the sequence so far.
    Csi,
    /// Inside a string whose prefix is still being read into `pending`.
    StringUndecided(StringKind),
    /// Inside a string being copied through.
    StringKeep(StringKind),
    /// Inside a string being dropped.
    StringDrop(StringKind),
}

enum Verdict {
    Keep,
    Drop,
    Undecided,
}

/// Strips terminal queries and replies from a byte stream, across chunk
/// boundaries. Everything else, including text and invalid UTF-8, passes
/// through byte for byte.
pub(crate) struct QueryFilter {
    state: State,
    pending: Vec<u8>,
    /// Inside a string, the previous byte was ESC (a possible ST).
    string_escape: bool,
}

impl Default for QueryFilter {
    fn default() -> Self {
        Self {
            state: State::Ground,
            pending: Vec::new(),
            string_escape: false,
        }
    }
}

impl QueryFilter {
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    /// The bytes of a sequence still being decided. A replay appends them so
    /// that output continuing the sequence live still parses.
    pub(crate) fn pending(&self) -> &[u8] {
        &self.pending
    }

    pub(crate) fn feed(&mut self, input: &[u8], output: &mut Vec<u8>) {
        for &byte in input {
            self.step(byte, output);
        }
    }

    fn step(&mut self, byte: u8, output: &mut Vec<u8>) {
        match self.state {
            State::Ground => {
                if byte == ESC {
                    self.begin_escape();
                } else {
                    output.push(byte);
                }
            }
            State::Escape => self.step_escape(byte, output),
            State::Csi => self.step_csi(byte, output),
            State::StringUndecided(kind) => self.step_undecided_string(kind, byte, output),
            State::StringKeep(kind) => self.step_string(kind, true, byte, output),
            State::StringDrop(kind) => self.step_string(kind, false, byte, output),
        }
    }

    fn begin_escape(&mut self) {
        self.pending.clear();
        self.pending.push(ESC);
        self.state = State::Escape;
    }

    fn flush(&mut self, output: &mut Vec<u8>) {
        output.append(&mut self.pending);
        self.state = State::Ground;
    }

    fn discard(&mut self) {
        self.pending.clear();
        self.state = State::Ground;
    }

    fn step_escape(&mut self, byte: u8, output: &mut Vec<u8>) {
        let introducer = self.pending.len() == 1;
        match byte {
            ESC => {
                // A new sequence aborts this one.
                self.flush(output);
                self.begin_escape();
            }
            CAN | SUB => {
                self.pending.push(byte);
                self.flush(output);
            }
            b'[' if introducer => {
                self.pending.push(byte);
                self.state = State::Csi;
            }
            b']' if introducer => self.begin_string(StringKind::Osc, byte),
            b'P' if introducer => self.begin_string(StringKind::Dcs, byte),
            b'_' | b'^' | b'X' if introducer => {
                self.pending.clear();
                self.string_escape = false;
                self.state = State::StringDrop(StringKind::Other);
            }
            // DECID, an old form of the primary device attributes query.
            b'Z' if introducer => self.discard(),
            0x20..=0x2f if self.pending.len() < MAX_ESCAPE_BYTES => self.pending.push(byte),
            0x00..=0x1f => {
                // C0 controls execute inside escape sequences.
                self.pending.push(byte);
                if self.pending.len() >= MAX_ESCAPE_BYTES {
                    self.flush(output);
                }
            }
            _ => {
                self.pending.push(byte);
                self.flush(output);
            }
        }
    }

    fn step_csi(&mut self, byte: u8, output: &mut Vec<u8>) {
        match byte {
            ESC => {
                self.flush(output);
                self.begin_escape();
            }
            CAN | SUB => {
                self.pending.push(byte);
                self.flush(output);
            }
            0x40..=0x7e => {
                self.pending.push(byte);
                if is_query_csi(&self.pending) {
                    self.discard();
                } else {
                    self.flush(output);
                }
            }
            0x00..=0x3f => {
                self.pending.push(byte);
                if self.pending.len() > MAX_CSI_BYTES {
                    self.flush(output);
                }
            }
            _ => {
                self.pending.push(byte);
                self.flush(output);
            }
        }
    }

    fn begin_string(&mut self, kind: StringKind, introducer: u8) {
        self.pending.push(introducer);
        self.string_escape = false;
        self.state = State::StringUndecided(kind);
    }

    /// Whether `byte` ends the current string. Updates the ST tracking; on
    /// `Some(false)` the string was aborted by an ESC starting a new
    /// sequence, with `byte` its second byte.
    fn string_end(&mut self, kind: StringKind, byte: u8) -> Option<bool> {
        if self.string_escape {
            self.string_escape = false;
            return Some(byte == b'\\');
        }
        match byte {
            ESC => {
                self.string_escape = true;
                None
            }
            BEL if kind == StringKind::Osc => Some(true),
            CAN | SUB => Some(true),
            _ => None,
        }
    }

    fn step_undecided_string(&mut self, kind: StringKind, byte: u8, output: &mut Vec<u8>) {
        let was_escape = self.string_escape;
        match self.string_end(kind, byte) {
            Some(true) => {
                self.pending.push(byte);
                let terminator = if was_escape { 2 } else { 1 };
                let body = &self.pending[2..self.pending.len() - terminator];
                match classify_string(kind, body, true) {
                    Verdict::Drop => self.discard(),
                    Verdict::Keep | Verdict::Undecided => self.flush(output),
                }
            }
            Some(false) => {
                // ESC then something other than `\`: the string is aborted
                // (the emulator drops it too) and a new sequence begins.
                self.pending.pop();
                self.flush(output);
                self.begin_escape();
                self.step_escape(byte, output);
            }
            None => {
                self.pending.push(byte);
                if self.string_escape {
                    return;
                }
                match classify_string(kind, &self.pending[2..], false) {
                    Verdict::Drop => {
                        self.pending.clear();
                        self.state = State::StringDrop(kind);
                    }
                    Verdict::Keep => {
                        output.append(&mut self.pending);
                        self.state = State::StringKeep(kind);
                    }
                    Verdict::Undecided if self.pending.len() > MAX_STRING_PREFIX => {
                        output.append(&mut self.pending);
                        self.state = State::StringKeep(kind);
                    }
                    Verdict::Undecided => {}
                }
            }
        }
    }

    fn step_string(&mut self, kind: StringKind, keep: bool, byte: u8, output: &mut Vec<u8>) {
        match self.string_end(kind, byte) {
            Some(true) => {
                if keep {
                    output.append(&mut self.pending);
                    output.push(byte);
                }
                self.discard();
            }
            Some(false) => {
                if keep {
                    // Keep the aborted string's bytes; the ESC starts anew.
                    self.pending.clear();
                }
                self.begin_escape();
                self.step_escape(byte, output);
            }
            None if byte == ESC => {
                // Hold a possible ST until the next byte decides it.
                self.pending.clear();
                self.pending.push(ESC);
            }
            None => {
                if keep {
                    output.push(byte);
                }
            }
        }
    }
}

/// Whether a complete CSI sequence (`ESC [` … final) is a query or a reply.
fn is_query_csi(sequence: &[u8]) -> bool {
    let Some((&final_byte, body)) = sequence[2..].split_last() else {
        return false;
    };
    let (marker, body) = match body.first() {
        Some(&marker @ b'<'..=b'?') => (Some(marker), &body[1..]),
        _ => (None, body),
    };
    let parameters_end = body
        .iter()
        .position(|byte| (0x20..=0x2f).contains(byte))
        .unwrap_or(body.len());
    let (parameters, intermediates) = body.split_at(parameters_end);
    let first_parameter = parameters
        .split(|byte| *byte == b';')
        .next()
        .unwrap_or_default();
    match final_byte {
        // Device status and cursor position requests.
        b'n' => matches!(marker, None | Some(b'?')),
        // Primary, secondary and tertiary device attributes, and replies.
        b'c' => intermediates.is_empty(),
        // Cursor position reply.
        b'R' => matches!(marker, None | Some(b'?')) && intermediates.is_empty(),
        // DECRQM and its DECRPM reply.
        b'p' | b'y' => intermediates == b"$",
        // XTVERSION.
        b'q' => marker == Some(b'>'),
        // Kitty keyboard protocol flags query and reply.
        b'u' => marker == Some(b'?'),
        // Window reports (XTWINOPS 11, 13–21).
        b't' => {
            marker.is_none()
                && intermediates.is_empty()
                && matches!(
                    first_parameter,
                    b"11" | b"13" | b"14" | b"15" | b"16" | b"18" | b"19" | b"20" | b"21"
                )
        }
        // DECREQTPARM.
        b'x' => {
            marker.is_none() && intermediates.is_empty() && matches!(parameters, b"" | b"0" | b"1")
        }
        // XTSMGRAPHICS and XTQMODKEYS.
        b'S' | b'm' => marker == Some(b'?'),
        _ => false,
    }
}

/// Whether an OSC or DCS string is a query or a reply. `body` is everything
/// after the introducer and before the terminator; `complete` says whether
/// the terminator has been seen.
fn classify_string(kind: StringKind, body: &[u8], complete: bool) -> Verdict {
    match kind {
        StringKind::Other => Verdict::Drop,
        StringKind::Osc => {
            let number_end = body
                .iter()
                .position(|byte| !byte.is_ascii_digit())
                .unwrap_or(body.len());
            if number_end == body.len() && !complete {
                return Verdict::Undecided;
            }
            match &body[..number_end] {
                // Clipboard reads and writes.
                b"52" => Verdict::Drop,
                // Palette, special and dynamic colours: `?` asks for the value.
                b"4" | b"5" | b"10" | b"11" | b"12" | b"13" | b"14" | b"15" | b"16" | b"17"
                | b"18" | b"19" => {
                    if !complete {
                        Verdict::Undecided
                    } else if body.split(|byte| *byte == b';').any(|part| part == b"?") {
                        Verdict::Drop
                    } else {
                        Verdict::Keep
                    }
                }
                _ => Verdict::Keep,
            }
        }
        StringKind::Dcs => {
            let rest_start = body
                .iter()
                .position(|byte| !(byte.is_ascii_digit() || *byte == b';'))
                .unwrap_or(body.len());
            let rest = &body[rest_start..];
            if rest.len() < 2 {
                return if complete {
                    Verdict::Keep
                } else {
                    Verdict::Undecided
                };
            }
            match &rest[..2] {
                // DECRQSS and XTGETTCAP requests, their replies, and the
                // XTVERSION reply.
                b"$q" | b"+q" | b"$r" | b"+r" | b">|" => Verdict::Drop,
                _ => Verdict::Keep,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filtered_whole(input: &[u8]) -> Vec<u8> {
        let mut filter = QueryFilter::default();
        let mut output = Vec::new();
        filter.feed(input, &mut output);
        output
    }

    /// The same input fed one byte at a time, as the worst chunking.
    fn filtered_bytewise(input: &[u8]) -> Vec<u8> {
        let mut filter = QueryFilter::default();
        let mut output = Vec::new();
        for byte in input {
            filter.feed(std::slice::from_ref(byte), &mut output);
        }
        output
    }

    fn assert_filtered(input: &[u8], expected: &[u8]) {
        for (label, output) in [
            ("whole", filtered_whole(input)),
            ("bytewise", filtered_bytewise(input)),
        ] {
            assert_eq!(
                String::from_utf8_lossy(&output),
                String::from_utf8_lossy(expected),
                "{label} filtering of {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    #[test]
    fn strips_queries_and_replies() {
        let cases: &[&[u8]] = &[
            b"\x1b[6n",
            b"\x1b[5n",
            b"\x1b[?6n",
            b"\x1b[c",
            b"\x1b[0c",
            b"\x1b[>c",
            b"\x1b[>0c",
            b"\x1b[=c",
            b"\x1b[?1;2c",
            b"\x1b[>1;10;0c",
            b"\x1b[12;40R",
            b"\x1b[?2004$p",
            b"\x1b[?2004;2$y",
            b"\x1b[>0q",
            b"\x1b[?u",
            b"\x1b[?1u",
            b"\x1b[14t",
            b"\x1b[18t",
            b"\x1bZ",
            b"\x1b]11;?\x07",
            b"\x1b]10;?\x1b\\",
            b"\x1b]4;1;?\x07",
            b"\x1b]52;c;aGVsbG8=\x07",
            b"\x1b]52;c;?\x1b\\",
            b"\x1bP$qm\x1b\\",
            b"\x1bP+q544e\x1b\\",
            b"\x1bP1$r0m\x1b\\",
            b"\x1bP>|xterm(388)\x1b\\",
            b"\x1b_Ga=q,i=1;AAAA\x1b\\",
        ];
        for case in cases {
            let mut input = b"before ".to_vec();
            input.extend_from_slice(case);
            input.extend_from_slice(b" after");
            assert_filtered(&input, b"before  after");
        }
    }

    #[test]
    fn keeps_rendering_sequences_and_text() {
        let cases: &[&[u8]] = &[
            b"\x1b[1;31mred\x1b[0m",
            b"\x1b[2J\x1b[H",
            b"\x1b[?1049h\x1b[?25l",
            b"\x1b[>4;1m",
            b"\x1b[8;24;80t",
            b"\x1b]0;window title\x07",
            b"\x1b]2;title\x1b\\",
            b"\x1b]8;;https://example.test\x1b\\link\x1b]8;;\x1b\\",
            b"\x1b]11;rgb:0000/0000/0000\x07",
            b"\x1b]7;file://host/tmp\x07",
            b"\x1b(B\x1b)0",
            b"\x1b7\x1b8\x1bc",
            b"\x1bPq#0;2;0;0;0#1!10~\x1b\\",
            "héllo — 世界 🎉\r\n".as_bytes(),
            b"\xff\xfe invalid utf8 \x80",
        ];
        for case in cases {
            assert_filtered(case, case);
        }
    }

    #[test]
    fn an_escape_inside_a_sequence_starts_a_new_one() {
        // A CSI aborted by ESC is kept as is; the query after it still goes.
        assert_filtered(b"\x1b[12\x1b[6nok", b"\x1b[12ok");
        // An OSC aborted by a new sequence.
        assert_filtered(b"\x1b]0;ti\x1b[6nok", b"\x1b]0;tiok");
        // A dropped clipboard write aborted by a kept sequence.
        assert_filtered(b"\x1b]52;c;abc\x1b[1mok", b"\x1b[1mok");
    }

    #[test]
    fn pending_holds_an_undecided_sequence() {
        let mut filter = QueryFilter::default();
        let mut output = Vec::new();
        filter.feed(b"text\x1b[1;3", &mut output);
        assert_eq!(output, b"text");
        assert_eq!(filter.pending(), b"\x1b[1;3");
        filter.feed(b"1m", &mut output);
        assert_eq!(output, b"text\x1b[1;31m");
        assert!(filter.pending().is_empty());
    }

    #[test]
    fn long_strings_pass_through_once_undecidable() {
        let mut input = b"\x1b]1337;".to_vec();
        input.extend(std::iter::repeat_n(b'x', 1000));
        input.push(BEL);
        assert_filtered(&input, &input);
    }

    #[test]
    fn history_keeps_the_newest_lines() {
        let mut history = TerminalHistory::with_limits(5, 1024 * 1024);
        for line in 0..20 {
            history.push(format!("line {line}\n").as_bytes());
        }
        history.push(b"partial");
        assert_eq!(history.lines(), 5);
        let (tail, truncated) = history.tail(usize::MAX);
        assert!(truncated);
        assert_eq!(
            String::from_utf8(tail).unwrap(),
            "line 15\nline 16\nline 17\nline 18\nline 19\npartial"
        );
    }

    #[test]
    fn history_eviction_never_splits_a_code_point() {
        // No line breaks, so byte eviction falls back to UTF-8 boundaries.
        let text = "é世🎉".repeat(200);
        // Only the start can be split by eviction: mid-stream, the newest
        // bytes may legitimately end inside a character still arriving.
        let starts_on_a_boundary = |bytes: &[u8]| {
            bytes
                .first()
                .is_none_or(|byte| !is_utf8_continuation(*byte))
        };
        for limit in 4..40 {
            let mut history = TerminalHistory::with_limits(5_000, limit);
            for chunk in text.as_bytes().chunks(7) {
                history.push(chunk);
                assert!(history.len() <= limit);
                let (tail, _) = history.tail(usize::MAX);
                assert!(
                    starts_on_a_boundary(&tail),
                    "eviction split a code point at limit {limit}"
                );
                for max in 1..limit {
                    let (tail, _) = history.tail(max);
                    assert!(starts_on_a_boundary(&tail));
                    assert!(tail.len() <= max);
                }
            }
            // With every character complete, all of it is valid UTF-8.
            let (tail, truncated) = history.tail(usize::MAX);
            assert!(truncated);
            assert!(std::str::from_utf8(&tail).is_ok());
            for max in 1..limit {
                assert!(std::str::from_utf8(&history.tail(max).0).is_ok());
            }
        }
    }

    #[test]
    fn history_eviction_prefers_line_boundaries() {
        let mut history = TerminalHistory::with_limits(5_000, 32);
        history.push(b"first line\nsecond line\nthird line is longer\n");
        let (tail, truncated) = history.tail(usize::MAX);
        assert!(truncated);
        assert_eq!(tail, b"third line is longer\n");
    }

    #[test]
    fn history_respects_the_default_byte_bound() {
        let mut history = TerminalHistory::default();
        let line = format!("{}\n", "x".repeat(4095));
        for _ in 0..(TERMINAL_RETAINED_BYTES / 4096 + 100) {
            history.push(line.as_bytes());
        }
        assert!(history.len() <= TERMINAL_RETAINED_BYTES);
        assert!(history.lines() <= TERMINAL_RETAINED_LINES);
    }

    #[test]
    fn tail_starts_on_a_line_boundary() {
        let mut history = TerminalHistory::default();
        history.push(b"alpha\nbravo\ncharlie\n");
        assert_eq!(history.tail(14), (b"charlie\n".to_vec(), true));
        assert_eq!(
            history.tail(1024),
            (b"alpha\nbravo\ncharlie\n".to_vec(), false)
        );
    }

    #[test]
    fn clear_drops_everything_and_resets_truncation() {
        let mut history = TerminalHistory::with_limits(2, 1024);
        history.push(b"a\nb\nc\n");
        assert!(history.tail(1024).1);
        history.clear();
        assert_eq!(history.tail(1024), (Vec::new(), false));
        history.push(b"d\n");
        assert_eq!(history.tail(1024), (b"d\n".to_vec(), false));
    }
}
