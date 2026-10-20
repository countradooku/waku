//! Thread search and in-thread find over the FTS5 index (#65, R24).
//!
//! - [`search_threads`] finds threads across projects: one match per thread
//!   (its title, else its latest matching message, else its latest matching
//!   plan), threads ordered by that source and then by recency.
//! - [`find_in_thread`] lists every match inside one thread by transcript
//!   position, whether or not the rows are loaded anywhere, plus snippets for
//!   a navigation window around an anchor. [`items_around`] loads the rows at
//!   a match so a client can jump to history it has not paged in yet.
//!
//! Both run on a read-only connection. Words are matched as prefixes
//! (`unicode61`, diacritics folded) and all must occur. Text in scripts
//! written without spaces (CJK) has no word boundaries for the tokenizer, so
//! such a query falls back to a substring scan of the same documents.

use std::sync::LazyLock;

use rusqlite::{Connection, OptionalExtension, params};
use waku_protocol::orchestration::*;

use crate::projection::search_index::{KIND_PLAN, KIND_THREAD_TITLE, item_document, thread_token};
use crate::store::{StoreResult, decode};

/// Default number of threads [`search_threads`] returns.
pub const DEFAULT_SEARCH_LIMIT: usize = 50;
/// The most matches [`find_in_thread`] lists; positions only, so this bounds
/// a pathological query, not a normal one.
pub const MAX_FIND_MATCHES: usize = 5_000;
/// Snippets [`find_in_thread`] cuts around its anchor (T3 navigates ≤17).
pub const FIND_WINDOW: usize = 17;
/// Snippet length in characters, including ellipses.
pub const SNIPPET_CHARS: usize = 240;
const SNIPPET_CONTEXT_BEFORE: usize = 72;

/// Where a match was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SearchSource {
    Title,
    Message,
    Plan,
}

impl SearchSource {
    fn from_kind(kind: &str) -> Self {
        match kind {
            KIND_THREAD_TITLE => Self::Title,
            KIND_PLAN => Self::Plan,
            _ => Self::Message,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchThreadsQuery {
    pub text: String,
    /// Only this project's threads.
    pub project_id: Option<ProjectId>,
    pub include_archived: bool,
    pub limit: usize,
}

impl SearchThreadsQuery {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            project_id: None,
            include_archived: false,
            limit: DEFAULT_SEARCH_LIMIT,
        }
    }
}

/// One thread found by [`search_threads`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadSearchMatch {
    pub thread_id: ThreadId,
    pub project_id: ProjectId,
    pub title: String,
    pub source: SearchSource,
    /// The message, plan or thread id that matched.
    pub entity_id: String,
    /// The matching row's transcript position; `None` for a title match.
    pub position: Option<u64>,
    pub snippet: String,
    /// `last_reply_at`, else `updated_at`: the order within a source.
    pub activity_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadFindQuery {
    pub thread_id: ThreadId,
    pub text: String,
    /// The position the user is at; the window is cut around the first match
    /// at or after it. `None` anchors on the latest match.
    pub anchor: Option<u64>,
    pub window: usize,
}

impl ThreadFindQuery {
    pub fn new(thread_id: ThreadId, text: impl Into<String>) -> Self {
        Self {
            thread_id,
            text: text.into(),
            anchor: None,
            window: FIND_WINDOW,
        }
    }
}

/// One match inside a thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindHit {
    pub source: SearchSource,
    pub entity_id: String,
    pub position: u64,
}

/// A match in the navigation window, with its snippet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindSnippet {
    /// Index into [`ThreadFindResult::matches`].
    pub index: usize,
    pub hit: FindHit,
    pub snippet: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadFindResult {
    /// Every match in transcript order (at most [`MAX_FIND_MATCHES`]).
    pub matches: Vec<FindHit>,
    /// More matches exist than `matches` holds.
    pub truncated: bool,
    /// The match the anchor selects, as an index into `matches`.
    pub current: Option<usize>,
    /// Snippets for up to `window` matches around `current`.
    pub window: Vec<FindSnippet>,
}

/// Whether `text` holds a character of a script written without spaces.
fn needs_substring(text: &str) -> bool {
    text.chars().any(|character| {
        matches!(character as u32,
            0x3040..=0x30FF        // Hiragana, Katakana
            | 0x3400..=0x4DBF      // CJK extension A
            | 0x4E00..=0x9FFF      // CJK unified ideographs
            | 0xAC00..=0xD7AF      // Hangul syllables
            | 0xF900..=0xFAFF      // CJK compatibility ideographs
            | 0x20000..=0x2FFFF) // CJK extensions B+
    })
}

/// The FTS5 expression for a user's text: every word, quoted, as a prefix;
/// all must match in `body`. `None` when the text has no word.
pub fn fts_expression(text: &str) -> Option<String> {
    let terms = text
        .split(|character: char| !character.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(|word| format!("\"{}\"*", word.replace('"', "\"\"")))
        .collect::<Vec<_>>();
    (!terms.is_empty()).then(|| format!("body : ({})", terms.join(" ")))
}

/// Matching documents as `(thread_id, entity_kind, entity_id, position)`,
/// FTS5 form: `?1` is the expression, `?2` an optional project.
const FTS_HITS: &str = "SELECT d.thread_id, d.entity_kind, d.entity_id, d.position
     FROM search_fts JOIN search_docs AS d ON d.doc_id = search_fts.rowid
     WHERE search_fts MATCH ?1 AND (?2 IS NULL OR d.project_id = ?2)";

/// The substring form for scripts without word boundaries: `?1` is the
/// lowercased needle, `?2` an optional project, `?5` an optional thread.
const SUBSTRING_HITS: &str = "SELECT d.thread_id, d.entity_kind, d.entity_id, d.position
     FROM search_docs AS d
     LEFT JOIN p_turn_items AS i
       ON d.position IS NOT NULL AND i.thread_id = d.thread_id AND i.position = d.position
     LEFT JOIN p_threads AS t
       ON d.entity_kind = 'thread_title' AND t.thread_id = d.thread_id
     WHERE (?2 IS NULL OR d.project_id = ?2) AND (?5 IS NULL OR d.thread_id = ?5)
       AND instr(lower(COALESCE(i.payload_json, t.title, '')), ?1) > 0";

/// Picks one hit per thread (a title, else the latest message, else the
/// latest plan; SQLite takes bare columns from the `MIN` row), drops deleted
/// and optionally archived threads (`?3`), and orders by source, then
/// recency, keeping `?4` rows.
fn best_per_thread(hits: &str) -> String {
    format!(
        "WITH hits AS ({hits}),
         best AS (
           SELECT thread_id, entity_kind, entity_id, position,
                  MIN((CASE entity_kind WHEN 'thread_title' THEN 0
                                        WHEN 'message' THEN 1 ELSE 2 END) * 1152921504606846976
                      + (1152921504606846975 - COALESCE(position, 0))) AS best_key
           FROM hits GROUP BY thread_id)
         SELECT b.thread_id, t.project_id, t.title, b.entity_kind, b.entity_id, b.position,
                COALESCE(t.last_reply_at, t.updated_at) AS activity_at
         FROM best AS b JOIN p_threads AS t ON t.thread_id = b.thread_id
         WHERE t.deleted_at IS NULL AND (?3 OR t.archived_at IS NULL)
         ORDER BY b.best_key >> 60, activity_at DESC, b.thread_id
         LIMIT ?4"
    )
}

// Every statement binds the same five parameters; unused ones are tested
// against NULL so the counts agree.
static SEARCH_FTS_SQL: LazyLock<String> =
    LazyLock::new(|| best_per_thread(&format!("{FTS_HITS} AND ?5 IS NULL")));
static SEARCH_SUBSTRING_SQL: LazyLock<String> =
    LazyLock::new(|| best_per_thread(&format!("{SUBSTRING_HITS} AND ?5 IS NULL")));
static FIND_FTS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT entity_kind, entity_id, position FROM ({FTS_HITS})
         WHERE position IS NOT NULL AND ?4 IS NULL AND ?5 IS NULL
         ORDER BY position LIMIT ?3"
    )
});
static FIND_SUBSTRING_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT entity_kind, entity_id, position FROM ({SUBSTRING_HITS})
         WHERE position IS NOT NULL AND ?4 IS NULL
         ORDER BY position LIMIT ?3"
    )
});

/// The SQL and its `?1` for a user's text, or `None` when nothing can match.
fn query_form<'a>(text: &str, fts: &'a str, substring: &'a str) -> Option<(&'a str, String)> {
    if needs_substring(text) {
        let needle = text.trim().to_lowercase();
        (!needle.is_empty()).then_some((substring, needle))
    } else {
        fts_expression(text).map(|expression| (fts, expression))
    }
}

/// Threads matching `query`, at most `query.limit`, best first.
pub fn search_threads(
    connection: &Connection,
    query: &SearchThreadsQuery,
) -> StoreResult<Vec<ThreadSearchMatch>> {
    if query.limit == 0 {
        return Ok(Vec::new());
    }
    let Some((sql, needle)) = query_form(&query.text, &SEARCH_FTS_SQL, &SEARCH_SUBSTRING_SQL)
    else {
        return Ok(Vec::new());
    };
    let project = query.project_id.map(|id| id.to_string());
    let limit = query.limit.min(i64::MAX as usize) as i64;
    let mut statement = connection.prepare_cached(sql)?;
    let rows = statement
        .query_map(
            params![
                needle,
                project,
                query.include_archived,
                limit,
                None::<String>
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let mut matches = Vec::with_capacity(rows.len());
    for (thread_id, project_id, title, kind, entity_id, position, activity_at) in rows {
        let (Ok(thread_id), Ok(project_id)) = (thread_id.parse(), project_id.parse()) else {
            continue;
        };
        let source = SearchSource::from_kind(&kind);
        let body = match (source, position) {
            (SearchSource::Title, _) | (_, None) => title.clone(),
            (_, Some(position)) => item_text(connection, thread_id, position as u64)?,
        };
        matches.push(ThreadSearchMatch {
            thread_id,
            project_id,
            title,
            source,
            entity_id,
            position: position.map(|position| position as u64),
            snippet: snippet(&body, &query.text),
            activity_at,
        });
    }
    Ok(matches)
}

/// Every match of `query.text` inside one thread, with snippets around the
/// anchor.
pub fn find_in_thread(
    connection: &Connection,
    query: &ThreadFindQuery,
) -> StoreResult<ThreadFindResult> {
    let Some((sql, needle)) = query_form(&query.text, &FIND_FTS_SQL, &FIND_SUBSTRING_SQL) else {
        return Ok(ThreadFindResult::default());
    };
    // The FTS form narrows by the thread's token; the substring form by
    // `?5`.
    let (needle, thread) = if sql == FIND_FTS_SQL.as_str() {
        let expression = format!(
            "thread : \"{}\" AND {needle}",
            thread_token(query.thread_id)
        );
        (expression, None)
    } else {
        (needle, Some(query.thread_id.to_string()))
    };
    let limit = (MAX_FIND_MATCHES + 1) as i64;
    let mut statement = connection.prepare_cached(sql)?;
    let mut hits = statement
        .query_map(
            params![needle, None::<String>, limit, None::<i64>, thread],
            |row| {
                Ok(FindHit {
                    source: SearchSource::from_kind(&row.get::<_, String>(0)?),
                    entity_id: row.get(1)?,
                    position: row.get::<_, i64>(2)? as u64,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    let truncated = hits.len() > MAX_FIND_MATCHES;
    hits.truncate(MAX_FIND_MATCHES);
    if hits.is_empty() {
        return Ok(ThreadFindResult::default());
    }
    let current = match query.anchor {
        Some(anchor) => hits
            .iter()
            .position(|hit| hit.position >= anchor)
            .unwrap_or(hits.len() - 1),
        None => hits.len() - 1,
    };
    let window = query.window.max(1).min(hits.len());
    let start = current.saturating_sub(window / 2).min(hits.len() - window);
    let mut snippets = Vec::with_capacity(window);
    for (index, hit) in hits.iter().enumerate().skip(start).take(window) {
        let body = item_text(connection, query.thread_id, hit.position)?;
        snippets.push(FindSnippet {
            index,
            hit: hit.clone(),
            snippet: snippet(&body, &query.text),
        });
    }
    Ok(ThreadFindResult {
        matches: hits,
        truncated,
        current: Some(current),
        window: snippets,
    })
}

/// The turn items around `position` in one thread: up to `before` rows
/// before it, the rows at or after it up to `after + 1`, in order. A find
/// result's position lands here even when the row was never paged in.
pub fn items_around(
    connection: &Connection,
    thread_id: ThreadId,
    position: u64,
    before: usize,
    after: usize,
) -> StoreResult<Vec<TurnItem>> {
    let thread = thread_id.to_string();
    let rows = |sql: &str, limit: usize| -> StoreResult<Vec<TurnItem>> {
        let mut statement = connection.prepare_cached(sql)?;
        let rows = statement
            .query_map(params![thread, position as i64, limit as i64], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.iter()
            .map(|json| decode("p_turn_items", json))
            .collect()
    };
    let mut items = rows(
        "SELECT payload_json FROM p_turn_items WHERE thread_id = ?1 AND position < ?2
         ORDER BY position DESC LIMIT ?3",
        before,
    )?;
    items.reverse();
    items.extend(rows(
        "SELECT payload_json FROM p_turn_items WHERE thread_id = ?1 AND position >= ?2
         ORDER BY position LIMIT ?3",
        after + 1,
    )?);
    Ok(items)
}

/// The indexed text of the item at `position`.
fn item_text(connection: &Connection, thread_id: ThreadId, position: u64) -> StoreResult<String> {
    let json = connection
        .prepare_cached(
            "SELECT payload_json FROM p_turn_items WHERE thread_id = ?1 AND position = ?2",
        )?
        .query_row(params![thread_id.to_string(), position as i64], |row| {
            row.get::<_, String>(0)
        })
        .optional()?;
    let Some(json) = json else {
        return Ok(String::new());
    };
    let item: TurnItem = decode("p_turn_items", &json)?;
    Ok(item_document(&item)
        .map(|(_, _, body)| body)
        .unwrap_or_default())
}

/// A whitespace-normalized excerpt of `text` around the first occurrence of
/// the query's first word, at most [`SNIPPET_CHARS`] characters.
pub fn snippet(text: &str, query: &str) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let char_count = normalized.chars().count();
    if char_count <= SNIPPET_CHARS {
        return normalized;
    }
    let needle = if needs_substring(query) {
        query.trim().to_lowercase()
    } else {
        query
            .split(|character: char| !character.is_alphanumeric())
            .find(|word| !word.is_empty())
            .unwrap_or_default()
            .to_lowercase()
    };
    let lower = normalized.to_lowercase();
    // Lowercasing can change byte lengths outside ASCII; map the match back
    // through character counts.
    let match_char = lower
        .find(&needle)
        .map(|byte| lower[..byte].chars().count())
        .unwrap_or(0);
    let body_chars = SNIPPET_CHARS.saturating_sub(4);
    let start = match_char
        .saturating_sub(SNIPPET_CONTEXT_BEFORE)
        .min(char_count.saturating_sub(body_chars));
    let end = (start + body_chars).min(char_count);
    let body = normalized
        .chars()
        .skip(start)
        .take(end - start)
        .collect::<String>();
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        body,
        if end < char_count { "…" } else { "" }
    )
}
