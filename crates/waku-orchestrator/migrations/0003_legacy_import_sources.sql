-- Where each legacy import read from (#63). `legacy_imports` records a
-- source fingerprint per session; a lazy transcript import needs the file
-- again, after a restart and whatever the file looks like by then.
CREATE TABLE legacy_import_sources (
  source_fingerprint TEXT    PRIMARY KEY,
  source             TEXT    NOT NULL CHECK (source IN ('fork_app_db', 'upstream_app_db')),
  path               TEXT    NOT NULL,
  imported_at        INTEGER NOT NULL) STRICT;
CREATE INDEX legacy_imports_pending ON legacy_imports(session_id)
  WHERE transcript_imported_at IS NULL;
