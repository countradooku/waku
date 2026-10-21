-- Decision D6 (docs/fork/PLAN.md §2): the orchestrator database starts
-- fresh, with no import of upstream Waku's or the fork's older `app.db`
-- (#55). The importer's bookkeeping from #63 goes with it. 0001 and 0003
-- stay as they were applied, so a `state.sqlite` written during Phase 2
-- keeps opening; this migration removes what they created.
DROP INDEX IF EXISTS legacy_imports_pending;
DROP TABLE IF EXISTS legacy_import_sources;
DROP TABLE IF EXISTS legacy_imports;
