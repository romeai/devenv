CREATE TABLE IF NOT EXISTS task_cache_v2 (
  task_name TEXT PRIMARY KEY,
  definition_hash TEXT NOT NULL,
  input_snapshot JSON NOT NULL,
  output_snapshot JSON NOT NULL,
  output JSON,
  last_run INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);
