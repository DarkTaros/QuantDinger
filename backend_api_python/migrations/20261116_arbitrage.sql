-- Independent arbitrage state; existing backtest records are not modified.
CREATE TABLE IF NOT EXISTS qd_arb_datasets (
  id CHAR(64) PRIMARY KEY, user_id BIGINT NOT NULL, pair TEXT NOT NULL,
  start_at TIMESTAMPTZ NOT NULL, end_at TIMESTAMPTZ NOT NULL,
  manifest JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Identical content can be made available to more than one importing tenant.
CREATE TABLE IF NOT EXISTS qd_arb_dataset_access (
  dataset_id CHAR(64) REFERENCES qd_arb_datasets(id), user_id BIGINT NOT NULL,
  PRIMARY KEY(dataset_id, user_id)
);
CREATE TABLE IF NOT EXISTS qd_arb_runs (
  id TEXT PRIMARY KEY, user_id BIGINT NOT NULL, job_id TEXT UNIQUE,
  mode TEXT NOT NULL CHECK (mode IN ('backtest','paper')),
  status TEXT NOT NULL DEFAULT 'queued', config JSONB NOT NULL,
  dataset_id CHAR(64) REFERENCES qd_arb_datasets(id),
  summary JSONB NOT NULL DEFAULT '{}', error TEXT,
  stop_requested BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS qd_arb_runs_user ON qd_arb_runs(user_id, created_at DESC);
CREATE TABLE IF NOT EXISTS qd_arb_controls (
  job_id TEXT PRIMARY KEY, run_id TEXT REFERENCES qd_arb_runs(id),
  token TEXT, lease_until TIMESTAMPTZ, dispatched_at TIMESTAMPTZ,
  attempts INT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS qd_arb_cycles (
  run_id TEXT NOT NULL REFERENCES qd_arb_runs(id), seq BIGINT NOT NULL,
  body JSONB NOT NULL, PRIMARY KEY(run_id, seq)
);
CREATE TABLE IF NOT EXISTS qd_arb_events (
  run_id TEXT NOT NULL REFERENCES qd_arb_runs(id), seq BIGINT NOT NULL,
  body JSONB NOT NULL, PRIMARY KEY(run_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS qd_arb_human_idempotency
  ON qd_agent_jobs(user_id, kind, idempotency_key)
  WHERE kind IN ('arb_import','arb_backtest','arb_paper') AND idempotency_key IS NOT NULL;
