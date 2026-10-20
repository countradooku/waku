//! Thread search and in-thread find (#65).

use waku_protocol::orchestration::*;

use super::common::*;
use crate::search::{SearchSource, SearchThreadsQuery, ThreadFindQuery, snippet};

fn search(env: &Env, text: &str) -> Vec<(ThreadId, SearchSource)> {
    env.orchestrator
        .search_threads(&SearchThreadsQuery::new(text))
        .unwrap()
        .into_iter()
        .map(|found| (found.thread_id, found.source))
        .collect()
}

fn rename(env: &Env, thread_id: ThreadId, title: &str) {
    env.ok(Command::ThreadRename {
        thread_id,
        title: title.into(),
    });
}

#[test]
fn search_finds_titles_messages_and_replies_one_row_per_thread() {
    let env = Env::new();
    let project = env.project();
    let parser = env.thread(project);
    rename(&env, parser, "Parser spans");
    turn(
        &env,
        parser,
        "Make the tokenizer keep spans",
        "Done: tokens now carry spans.",
    );
    let lexer = env.thread(project);
    turn(&env, lexer, "Unrelated work", "The lexer emits spans too.");
    let other = env.thread(project);
    turn(&env, other, "Nothing to see", "Café crème recipe");

    // Title beats a message; the reply-only thread follows.
    assert_eq!(
        search(&env, "spans"),
        vec![
            (parser, SearchSource::Title),
            (lexer, SearchSource::Message)
        ]
    );
    // Prefixes, several words (all must match), diacritics folded.
    assert_eq!(
        search(&env, "tokeni"),
        vec![(parser, SearchSource::Message)]
    );
    assert_eq!(
        search(&env, "lexer spans"),
        vec![(lexer, SearchSource::Message)]
    );
    assert_eq!(
        search(&env, "cafe CREME"),
        vec![(other, SearchSource::Message)]
    );
    assert!(search(&env, "absent").is_empty());
    assert!(search(&env, "  ** ").is_empty());

    // The match carries its position and a snippet of the source row.
    let found = env
        .orchestrator
        .search_threads(&SearchThreadsQuery::new("lexer"))
        .unwrap();
    assert_eq!(found[0].snippet, "The lexer emits spans too.");
    assert!(found[0].position.is_some());

    // A renamed thread no longer matches its old title.
    rename(&env, parser, "Renamed");
    assert_eq!(search(&env, "spans")[0], (lexer, SearchSource::Message));
    assert!(search(&env, "spans").contains(&(parser, SearchSource::Message)));
    env.assert_verified();
}

#[test]
fn search_scopes_archived_deleted_and_projects() {
    let env = Env::new();
    let first = env.project();
    let second = env.project();
    let kept = env.thread(first);
    rename(&env, kept, "Quantum notes");
    let archived = env.thread(first);
    rename(&env, archived, "Quantum archive");
    env.ok(Command::ThreadArchive {
        thread_id: archived,
    });
    let deleted = env.thread(first);
    rename(&env, deleted, "Quantum deleted");
    env.ok(Command::ThreadDelete { thread_id: deleted });
    let elsewhere = env.thread(second);
    rename(&env, elsewhere, "Quantum elsewhere");

    let mut query = SearchThreadsQuery::new("quantum");
    let ids = |query: &SearchThreadsQuery| {
        let mut ids = env
            .orchestrator
            .search_threads(query)
            .unwrap()
            .into_iter()
            .map(|found| found.thread_id)
            .collect::<Vec<_>>();
        ids.sort();
        ids
    };
    let mut expected = vec![kept, elsewhere];
    expected.sort();
    assert_eq!(ids(&query), expected);
    query.include_archived = true;
    let mut expected = vec![kept, archived, elsewhere];
    expected.sort();
    assert_eq!(ids(&query), expected);
    query.project_id = Some(second);
    assert_eq!(ids(&query), vec![elsewhere]);
    query.project_id = None;
    query.limit = 1;
    assert_eq!(ids(&query).len(), 1);
}

#[test]
fn streaming_text_is_indexed_once_it_settles() {
    let env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    let run_id = env.send(thread_id, "hello", DispatchMode::Auto);
    go_running(&env, thread_id, run_id).unwrap();
    assistant_text(&env, thread_id, run_id, "partial zeppelin").unwrap();
    assert!(search(&env, "zeppelin").is_empty());
    final_assistant(&env, thread_id, run_id, "final zeppelin");
    assert_eq!(
        search(&env, "zeppelin"),
        vec![(thread_id, SearchSource::Message)]
    );
    env.assert_verified();
}

#[test]
fn cjk_text_matches_by_substring() {
    let env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    turn(&env, thread_id, "请修复标题生成的问题", "已经修复了标题");
    // The first prompt also titled the thread.
    assert_eq!(
        search(&env, "标题生成"),
        vec![(thread_id, SearchSource::Title)]
    );
    assert_eq!(
        search(&env, "已经修复"),
        vec![(thread_id, SearchSource::Message)]
    );
    let found = env
        .orchestrator
        .find_in_thread(&ThreadFindQuery::new(thread_id, "标题"))
        .unwrap();
    assert_eq!(found.matches.len(), 2);
}

#[test]
fn find_lists_matches_in_unloaded_history_and_jumps_to_them() {
    let env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    let mut needles = Vec::new();
    for n in 0..40 {
        let user = if n % 4 == 0 {
            format!("turn {n}: look at the flux capacitor")
        } else {
            format!("turn {n}: routine work")
        };
        let run_id = turn(&env, thread_id, &user, &format!("reply {n}"));
        if n % 4 == 0 {
            needles.push(run_id);
        }
    }
    // Completed runs are not hot: the cached projection holds none of the
    // matching rows.
    let projection = env.projection(thread_id);
    assert!(projection.hot_items.is_empty());

    let result = env
        .orchestrator
        .find_in_thread(&ThreadFindQuery::new(thread_id, "flux capacitor"))
        .unwrap();
    assert_eq!(result.matches.len(), needles.len());
    assert!(!result.truncated);
    assert!(
        result
            .matches
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    // No anchor: the latest match is current.
    assert_eq!(result.current, Some(needles.len() - 1));
    assert_eq!(result.window.len(), needles.len().min(17));

    // Anchor in the middle: the first match at or after it.
    let middle = result.matches[4].position;
    let anchored = env
        .orchestrator
        .find_in_thread(&ThreadFindQuery {
            anchor: Some(middle - 1),
            window: 3,
            ..ThreadFindQuery::new(thread_id, "flux")
        })
        .unwrap();
    assert_eq!(anchored.current, Some(4));
    assert_eq!(
        anchored
            .window
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    assert_eq!(
        anchored.window[1].snippet,
        "turn 16: look at the flux capacitor"
    );

    // Jumping to the first match loads its rows from SQLite.
    let first = result.matches[0].position;
    let rows = env
        .orchestrator
        .items_around(thread_id, first, 2, 3)
        .unwrap();
    assert_eq!(rows[0].position, first);
    assert!(matches!(
        &rows[0].kind,
        TurnItemKind::UserMessage { text, .. } if text.contains("flux capacitor")
    ));
    assert_eq!(rows.len(), 4);

    // Another thread's matches never leak in.
    let other = env.thread(project);
    turn(&env, other, "flux elsewhere", "ok");
    let again = env
        .orchestrator
        .find_in_thread(&ThreadFindQuery::new(thread_id, "flux"))
        .unwrap();
    assert_eq!(again.matches.len(), needles.len());
}

#[test]
fn rebuild_restores_the_index() {
    let mut env = Env::new();
    let project = env.project();
    let thread_id = env.thread(project);
    rename(&env, thread_id, "Orbital mechanics");
    turn(
        &env,
        thread_id,
        "compute the hohmann transfer",
        "delta-v is 3.9 km/s",
    );
    let before = search(&env, "hohmann");
    env.orchestrator.rebuild_projections().unwrap();
    assert_eq!(search(&env, "hohmann"), before);
    assert_eq!(
        search(&env, "orbital"),
        vec![(thread_id, SearchSource::Title)]
    );
    env.assert_verified();
    env.reopen();
    assert_eq!(search(&env, "hohmann"), before);
}

#[test]
fn snippets_center_on_the_match() {
    let text = format!("{} needle {}", "lead ".repeat(60), "tail ".repeat(60));
    let cut = snippet(&text, "needle");
    assert!(cut.starts_with('…') && cut.ends_with('…'));
    assert!(cut.contains("needle"));
    assert!(cut.chars().count() <= crate::search::SNIPPET_CHARS);
    assert_eq!(snippet("  short   text ", "x"), "short text");
}
