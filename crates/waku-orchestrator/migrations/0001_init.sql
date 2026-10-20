-- RFC 0001 0001_init.sql: the log tables, the outbox and the projection graph.
-- Every table is STRICT; ids are TEXT, timestamps and booleans INTEGER.
-- Projection tables declare no foreign keys (R19): snapshot events of
-- different entity kinds may arrive in any order and rebuild must accept them.

-- Log ----------------------------------------------------------------------

CREATE TABLE events (
  sequence       INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id       TEXT    NOT NULL UNIQUE,
  command_id     TEXT,
  project_id     TEXT,
  thread_id      TEXT,
  run_id         TEXT,
  node_id        TEXT,
  entity_id      TEXT    NOT NULL,
  driver         TEXT,
  event_type     TEXT    NOT NULL,
  occurred_at    INTEGER NOT NULL,
  payload_json   TEXT    NOT NULL,
  CHECK (thread_id IS NOT NULL OR project_id IS NOT NULL)
) STRICT;
CREATE INDEX events_thread_seq  ON events(thread_id, sequence)  WHERE thread_id IS NOT NULL;
CREATE INDEX events_project_seq ON events(project_id, sequence) WHERE project_id IS NOT NULL;
CREATE INDEX events_run_seq     ON events(run_id, sequence)     WHERE run_id IS NOT NULL;
CREATE INDEX events_entity_seq  ON events(entity_id, sequence);
CREATE INDEX events_command     ON events(command_id)           WHERE command_id IS NOT NULL;

CREATE TABLE command_receipts (
  command_id      TEXT    PRIMARY KEY,
  command_type    TEXT    NOT NULL,
  project_id      TEXT,
  thread_id       TEXT,
  status          TEXT    NOT NULL CHECK (status IN ('accepted', 'rejected')),
  result_sequence INTEGER NOT NULL,
  error_json      TEXT,
  accepted_at     INTEGER NOT NULL
) STRICT;
CREATE INDEX command_receipts_thread   ON command_receipts(thread_id, result_sequence) WHERE thread_id IS NOT NULL;
CREATE INDEX command_receipts_accepted ON command_receipts(accepted_at);

CREATE TABLE effect_outbox (
  effect_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  command_id       TEXT,
  lane_key         TEXT    NOT NULL,
  thread_id        TEXT,
  effect_type      TEXT    NOT NULL,
  lane             TEXT    NOT NULL CHECK (lane IN ('thread', 'title')),
  process_bound    INTEGER NOT NULL CHECK (process_bound IN (0, 1)),
  status           TEXT    NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
  attempt_count    INTEGER NOT NULL DEFAULT 0,
  available_at     INTEGER NOT NULL,
  lease_owner      TEXT,
  lease_expires_at INTEGER,
  payload_json     TEXT    NOT NULL,
  last_error       TEXT,
  cancel_reason    TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL,
  settled_at       INTEGER
) STRICT;
CREATE INDEX effect_outbox_open    ON effect_outbox(lane_key, lane, effect_id) WHERE status IN ('pending', 'running');
CREATE INDEX effect_outbox_ready   ON effect_outbox(available_at, effect_id)   WHERE status = 'pending';
CREATE INDEX effect_outbox_leases  ON effect_outbox(lease_expires_at)          WHERE status = 'running';
CREATE INDEX effect_outbox_settled ON effect_outbox(settled_at)                WHERE status IN ('succeeded', 'cancelled');
CREATE INDEX effect_outbox_command ON effect_outbox(command_id)                WHERE command_id IS NOT NULL;

-- Projections ----------------------------------------------------------------

CREATE TABLE p_projects (
  project_id TEXT PRIMARY KEY, name TEXT NOT NULL, path TEXT NOT NULL, order_key TEXT NOT NULL,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, removed_at INTEGER,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_projects_order ON p_projects(order_key) WHERE removed_at IS NULL;

CREATE TABLE p_threads (
  thread_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL,
  provider_instance_id TEXT NOT NULL, active_provider_thread_id TEXT,
  parent_thread_id TEXT, root_thread_id TEXT NOT NULL,
  activity TEXT NOT NULL,
  pending_request_count INTEGER NOT NULL,
  queued_run_count INTEGER NOT NULL,
  created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_reply_at INTEGER,
  archived_at INTEGER, deleted_at INTEGER, settled_at INTEGER, snoozed_until INTEGER,
  pinned_at INTEGER, pin_order_key TEXT, active_order_key TEXT, last_visited_at INTEGER,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_threads_shell    ON p_threads(project_id, last_reply_at DESC) WHERE archived_at IS NULL AND deleted_at IS NULL;
CREATE INDEX p_threads_pinned   ON p_threads(pin_order_key)  WHERE pinned_at IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX p_threads_archived ON p_threads(project_id, archived_at DESC) WHERE archived_at IS NOT NULL AND deleted_at IS NULL;
CREATE INDEX p_threads_parent   ON p_threads(parent_thread_id) WHERE parent_thread_id IS NOT NULL;
CREATE INDEX p_threads_snoozed  ON p_threads(snoozed_until)  WHERE snoozed_until IS NOT NULL;
CREATE INDEX p_threads_active   ON p_threads(activity)       WHERE activity <> 'idle';

CREATE TABLE p_runs (
  run_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, ordinal INTEGER, status TEXT NOT NULL,
  queue_position INTEGER, queue_held INTEGER NOT NULL, provider_thread_id TEXT,
  continuation_of_run_id TEXT, requested_at INTEGER NOT NULL, started_at INTEGER, completed_at INTEGER,
  payload_json TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX p_runs_ordinal ON p_runs(thread_id, ordinal) WHERE ordinal IS NOT NULL;
CREATE INDEX p_runs_thread_status  ON p_runs(thread_id, status);
CREATE INDEX p_runs_queue          ON p_runs(thread_id, queue_position) WHERE queue_position IS NOT NULL;
CREATE INDEX p_runs_open           ON p_runs(status) WHERE status IN ('preparing', 'queued', 'starting', 'running', 'waiting', 'finalizing');
CREATE INDEX p_runs_provider_thread ON p_runs(provider_thread_id) WHERE provider_thread_id IS NOT NULL;

CREATE TABLE p_run_attempts (
  attempt_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT NOT NULL, attempt_ordinal INTEGER NOT NULL,
  root_node_id TEXT NOT NULL, provider_thread_id TEXT NOT NULL, provider_turn_id TEXT, status TEXT NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX p_run_attempts_ordinal ON p_run_attempts(run_id, attempt_ordinal);
CREATE INDEX p_run_attempts_thread ON p_run_attempts(thread_id, run_id);

CREATE TABLE p_nodes (
  node_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT, parent_node_id TEXT, root_node_id TEXT NOT NULL,
  kind TEXT NOT NULL, status TEXT NOT NULL, provider_turn_id TEXT, runtime_request_id TEXT,
  started_at INTEGER, completed_at INTEGER,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_nodes_run      ON p_nodes(thread_id, run_id);
CREATE INDEX p_nodes_parent   ON p_nodes(parent_node_id)   WHERE parent_node_id IS NOT NULL;
CREATE INDEX p_nodes_provider ON p_nodes(provider_turn_id) WHERE provider_turn_id IS NOT NULL;

CREATE TABLE p_subagents (
  subagent_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT, parent_node_id TEXT NOT NULL,
  child_thread_id TEXT, origin TEXT NOT NULL, status TEXT NOT NULL, driver TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_subagents_thread ON p_subagents(thread_id, status);
CREATE INDEX p_subagents_child  ON p_subagents(child_thread_id) WHERE child_thread_id IS NOT NULL;

CREATE TABLE p_provider_sessions (
  provider_session_id TEXT PRIMARY KEY, driver TEXT NOT NULL, provider_instance_id TEXT NOT NULL,
  status TEXT NOT NULL, updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_provider_sessions_live ON p_provider_sessions(provider_instance_id, status) WHERE status NOT IN ('stopped', 'error');

CREATE TABLE p_provider_session_bindings (
  provider_session_id TEXT NOT NULL, thread_id TEXT NOT NULL,
  PRIMARY KEY (provider_session_id, thread_id)) STRICT;
CREATE INDEX p_provider_session_bindings_thread ON p_provider_session_bindings(thread_id);

CREATE TABLE p_provider_threads (
  provider_thread_id TEXT PRIMARY KEY, thread_id TEXT, owner_node_id TEXT, provider_session_id TEXT,
  driver TEXT NOT NULL, provider_instance_id TEXT NOT NULL, native_id TEXT, status TEXT NOT NULL,
  first_run_ordinal INTEGER, last_run_ordinal INTEGER, updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_provider_threads_thread  ON p_provider_threads(thread_id) WHERE thread_id IS NOT NULL;
CREATE INDEX p_provider_threads_session ON p_provider_threads(provider_session_id) WHERE provider_session_id IS NOT NULL;
CREATE INDEX p_provider_threads_owner   ON p_provider_threads(owner_node_id) WHERE owner_node_id IS NOT NULL;
CREATE INDEX p_provider_threads_native  ON p_provider_threads(driver, native_id) WHERE native_id IS NOT NULL;

CREATE TABLE p_provider_turns (
  provider_turn_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, provider_thread_id TEXT NOT NULL,
  node_id TEXT NOT NULL, run_attempt_id TEXT, ordinal INTEGER NOT NULL, status TEXT NOT NULL,
  started_at INTEGER, completed_at INTEGER,
  payload_json TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX p_provider_turns_ordinal ON p_provider_turns(provider_thread_id, ordinal);
CREATE INDEX p_provider_turns_thread  ON p_provider_turns(thread_id);
CREATE INDEX p_provider_turns_attempt ON p_provider_turns(run_attempt_id) WHERE run_attempt_id IS NOT NULL;

CREATE TABLE p_runtime_requests (
  runtime_request_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, node_id TEXT NOT NULL, provider_turn_id TEXT,
  kind TEXT NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, resolved_at INTEGER,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_runtime_requests_thread  ON p_runtime_requests(thread_id, status);
CREATE INDEX p_runtime_requests_pending ON p_runtime_requests(created_at) WHERE status = 'pending';

CREATE TABLE p_messages (
  message_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT, node_id TEXT,
  role TEXT NOT NULL, streaming INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_messages_thread ON p_messages(thread_id, created_at, message_id);
CREATE INDEX p_messages_run    ON p_messages(run_id) WHERE run_id IS NOT NULL;

CREATE TABLE p_plans (
  plan_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT, node_id TEXT NOT NULL,
  kind TEXT NOT NULL, status TEXT NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_plans_thread ON p_plans(thread_id, status);
CREATE INDEX p_plans_run    ON p_plans(run_id) WHERE run_id IS NOT NULL;

CREATE TABLE p_turn_items (
  turn_item_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT, node_id TEXT, provider_turn_id TEXT,
  parent_item_id TEXT, position INTEGER NOT NULL, item_type TEXT NOT NULL, status TEXT NOT NULL,
  payload_bytes INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX p_turn_items_position ON p_turn_items(thread_id, position);
CREATE INDEX p_turn_items_type     ON p_turn_items(thread_id, item_type, position);
CREATE INDEX p_turn_items_run      ON p_turn_items(run_id, position) WHERE run_id IS NOT NULL;
CREATE INDEX p_turn_items_provider ON p_turn_items(provider_turn_id) WHERE provider_turn_id IS NOT NULL;

CREATE TABLE p_checkpoint_scopes (
  scope_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT, node_id TEXT NOT NULL, parent_scope_id TEXT,
  kind TEXT NOT NULL, ordinal_within_parent INTEGER NOT NULL, advances_app_run_count INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_checkpoint_scopes_thread ON p_checkpoint_scopes(thread_id);
CREATE INDEX p_checkpoint_scopes_parent ON p_checkpoint_scopes(parent_scope_id) WHERE parent_scope_id IS NOT NULL;

CREATE TABLE p_checkpoints (
  checkpoint_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, scope_id TEXT NOT NULL, run_id TEXT,
  app_run_ordinal INTEGER, ordinal_within_scope INTEGER NOT NULL, status TEXT NOT NULL,
  git_ref TEXT NOT NULL, captured_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE UNIQUE INDEX p_checkpoints_ordinal ON p_checkpoints(scope_id, ordinal_within_scope);
CREATE INDEX p_checkpoints_thread ON p_checkpoints(thread_id, app_run_ordinal);

CREATE TABLE p_context_transfers (
  context_transfer_id TEXT PRIMARY KEY, source_thread_id TEXT NOT NULL, target_thread_id TEXT NOT NULL,
  target_run_id TEXT, kind TEXT NOT NULL, status TEXT NOT NULL, updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_context_transfers_source ON p_context_transfers(source_thread_id);
CREATE INDEX p_context_transfers_target ON p_context_transfers(target_thread_id, status);

CREATE TABLE p_context_handoffs (
  context_handoff_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, target_run_id TEXT NOT NULL,
  to_provider_thread_id TEXT NOT NULL, strategy TEXT NOT NULL, status TEXT NOT NULL, updated_at INTEGER NOT NULL,
  payload_json TEXT NOT NULL) STRICT;
CREATE INDEX p_context_handoffs_thread ON p_context_handoffs(thread_id);
CREATE INDEX p_context_handoffs_run    ON p_context_handoffs(target_run_id);

CREATE TABLE projection_metadata (
  name TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL,
  last_sequence INTEGER NOT NULL,
  updated_at INTEGER NOT NULL) STRICT;

CREATE TABLE legacy_imports (
  session_id TEXT PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN ('fork_app_db', 'upstream_app_db')),
  source_fingerprint TEXT NOT NULL,
  shell_imported_at INTEGER NOT NULL,
  transcript_imported_at INTEGER,
  error TEXT) STRICT;
