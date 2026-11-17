-- Schema-owner operation only, after intake/subscription writers are quiesced
-- and the existing wire-v2 projection upgrade has settled all old work.
-- Never run on application startup. Terminal input identities are preserved.
BEGIN;
LOCK TABLE digitalmate_ingestion.version,
  digitalmate_ingestion.workspaces, digitalmate_ingestion.subscriptions,
  digitalmate_ingestion.events, digitalmate_ingestion.batches,
  digitalmate_ingestion.capacity IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF (SELECT array_agg(version) FROM digitalmate_ingestion.version) IS DISTINCT FROM ARRAY[1] THEN
    RAISE EXCEPTION 'screening_upgrade_requires_schema_1';
  END IF;
  IF EXISTS(SELECT 1 FROM digitalmate_ingestion.events WHERE state NOT IN ('delivered','revoked'))
    OR EXISTS(SELECT 1 FROM digitalmate_ingestion.batches WHERE state NOT IN ('accepted','revoked'))
    OR EXISTS(SELECT 1 FROM digitalmate_ingestion.workspaces WHERE active_batch IS NOT NULL OR lease_until IS NOT NULL)
    OR EXISTS(SELECT 1 FROM digitalmate_ingestion.capacity WHERE pending_events<>0 OR pending_bytes<>0) THEN
    RAISE EXCEPTION 'screening_upgrade_requires_settled_work';
  END IF;
END $$;
ALTER TABLE digitalmate_ingestion.subscriptions
  ADD COLUMN screening_json text NOT NULL DEFAULT '{"version":1,"rules":[]}',
  ADD COLUMN screening_revision bigint NOT NULL DEFAULT 0 CHECK(screening_revision>=0);
ALTER TABLE digitalmate_ingestion.events ADD COLUMN decision_json text NOT NULL DEFAULT
  '{"action":"accept","rule_id":null,"reason":"legacy_unscreened","policy_digest":"6be6bac38f2d35217ca3cd98e36322f9e8fb6638564f5a87ff660589f6302103","policy_version":1,"policy_revision":0}';
ALTER TABLE digitalmate_ingestion.events ALTER COLUMN decision_json DROP DEFAULT;
ALTER TABLE digitalmate_ingestion.events DROP CONSTRAINT events_state_check;
ALTER TABLE digitalmate_ingestion.events ADD CHECK(state IN ('pending','batched','delivered','revoked','suppressed','quarantined'));
CREATE INDEX IF NOT EXISTS ingestion_quarantine ON digitalmate_ingestion.events(subscription_id,finished_at,row_id) WHERE state='quarantined';
CREATE INDEX IF NOT EXISTS ingestion_terminal ON digitalmate_ingestion.events(finished_at,row_id) WHERE state IN ('delivered','revoked','suppressed');
ALTER TABLE digitalmate_ingestion.capacity ADD COLUMN retained_events bigint NOT NULL DEFAULT 0 CHECK(retained_events>=0);
UPDATE digitalmate_ingestion.capacity SET retained_events=(SELECT count(*) FROM digitalmate_ingestion.events);
ALTER TABLE digitalmate_ingestion.version DROP CONSTRAINT version_version_check;
UPDATE digitalmate_ingestion.version SET version=2;
ALTER TABLE digitalmate_ingestion.version ADD CHECK(version=2);
COMMIT;
