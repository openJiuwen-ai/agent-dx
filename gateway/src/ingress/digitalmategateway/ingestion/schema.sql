CREATE SCHEMA IF NOT EXISTS digitalmate_ingestion;
CREATE TABLE IF NOT EXISTS digitalmate_ingestion.version (version integer PRIMARY KEY CHECK(version=2));
INSERT INTO digitalmate_ingestion.version VALUES(2) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS digitalmate_ingestion.workspaces (
  workspace_key text PRIMARY KEY, due_at timestamptz,
  lease_epoch bigint NOT NULL DEFAULT 0, lease_until timestamptz,
  last_attempt timestamptz, active_batch text
);
CREATE TABLE IF NOT EXISTS digitalmate_ingestion.subscriptions (
  subscription_id text PRIMARY KEY, workspace_key text NOT NULL REFERENCES digitalmate_ingestion.workspaces,
  generation bigint NOT NULL CHECK(generation>0), connection_generation bigint NOT NULL CHECK(connection_generation>0),
  trigger_id text NOT NULL UNIQUE, binding_json text NOT NULL, enabled boolean NOT NULL,
  due_at timestamptz,
  screening_json text NOT NULL DEFAULT '{"version":1,"rules":[]}',
  screening_revision bigint NOT NULL DEFAULT 0 CHECK(screening_revision>=0)
);
CREATE TABLE IF NOT EXISTS digitalmate_ingestion.events (
  row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  subscription_id text NOT NULL REFERENCES digitalmate_ingestion.subscriptions,
  generation bigint NOT NULL, event_id text NOT NULL, digest text NOT NULL, raw bytea NOT NULL,
  state text NOT NULL CHECK(state IN ('pending','batched','delivered','revoked','suppressed','quarantined')),
  decision_json text NOT NULL,
  batch_id text, received_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
  UNIQUE(subscription_id,generation,event_id)
);
CREATE INDEX IF NOT EXISTS ingestion_pending ON digitalmate_ingestion.events(subscription_id,generation,row_id) WHERE state='pending';
CREATE INDEX IF NOT EXISTS ingestion_quarantine ON digitalmate_ingestion.events(subscription_id,finished_at,row_id) WHERE state='quarantined';
CREATE INDEX IF NOT EXISTS ingestion_terminal ON digitalmate_ingestion.events(finished_at,row_id) WHERE state IN ('delivered','revoked','suppressed');
CREATE TABLE IF NOT EXISTS digitalmate_ingestion.batches (
  batch_id text PRIMARY KEY, workspace_key text NOT NULL REFERENCES digitalmate_ingestion.workspaces,
  subscription_id text NOT NULL REFERENCES digitalmate_ingestion.subscriptions,
  generation bigint NOT NULL, digest text NOT NULL, payload bytea NOT NULL,
  state text NOT NULL CHECK(state IN ('ready','accepted','revoked')),
  attempts integer NOT NULL DEFAULT 0, last_error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS digitalmate_ingestion.capacity (
  singleton boolean PRIMARY KEY CHECK(singleton), pending_events bigint NOT NULL CHECK(pending_events>=0),
  pending_bytes bigint NOT NULL CHECK(pending_bytes>=0),
  retained_events bigint NOT NULL DEFAULT 0 CHECK(retained_events>=0)
);
INSERT INTO digitalmate_ingestion.capacity(singleton,pending_events,pending_bytes) VALUES(true,0,0) ON CONFLICT DO NOTHING;
