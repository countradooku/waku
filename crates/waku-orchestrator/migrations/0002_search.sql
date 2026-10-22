-- Thread search (#65, RFC 0001 R24 and "DDL: search").
--
-- One row per searchable entity: a message once it stops streaming, a plan
-- (proposed plan or todo list) on every update, and a thread's title.
-- `search_fts` is contentless: the text lives only in the projection rows,
-- and snippets are cut in Rust from them. `apply_sql` maintains both tables in
-- the sink transaction.

CREATE TABLE search_docs (
  doc_id      INTEGER PRIMARY KEY,          -- = search_fts rowid
  entity_kind TEXT    NOT NULL CHECK (entity_kind IN ('message', 'plan', 'thread_title')),
  entity_id   TEXT    NOT NULL UNIQUE,
  thread_id   TEXT    NOT NULL,
  project_id  TEXT    NOT NULL,
  position    INTEGER,                      -- turn item position, for in-thread find
  body_hash   INTEGER NOT NULL,             -- FNV-1a of the indexed body; skips no-op re-indexing
  updated_at  INTEGER NOT NULL) STRICT;
CREATE INDEX search_docs_thread ON search_docs(thread_id, position);

-- `thread` holds one token per row naming its thread (`t<uuid hex>`), so an
-- in-thread find intersects two doclists instead of filtering every match.
CREATE VIRTUAL TABLE search_fts USING fts5(
  body, thread,
  content = '', contentless_delete = 1,
  tokenize = 'unicode61 remove_diacritics 2', prefix = '2 3');
