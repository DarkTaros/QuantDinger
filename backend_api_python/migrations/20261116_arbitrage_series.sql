CREATE TABLE IF NOT EXISTS qd_arb_series (
  run_id TEXT NOT NULL REFERENCES qd_arb_runs(id),
  metric TEXT NOT NULL,
  direction TEXT NOT NULL DEFAULT '',
  seq BIGINT NOT NULL,
  ts_ns BIGINT NOT NULL,
  value TEXT,
  unit TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (run_id, metric, direction, seq)
);
CREATE INDEX IF NOT EXISTS qd_arb_series_lookup
  ON qd_arb_series(run_id, metric, direction, seq);
