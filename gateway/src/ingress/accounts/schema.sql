-- Apply with a schema-owner role before starting Ingress. Runtime needs DML only.
BEGIN;
CREATE SCHEMA IF NOT EXISTS adx_accounts;
CREATE TABLE IF NOT EXISTS adx_accounts.schema_version (version integer PRIMARY KEY);
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM adx_accounts.schema_version WHERE version <> 1) THEN
        RAISE EXCEPTION 'Unsupported account schema version';
    END IF;
END $$;
INSERT INTO adx_accounts.schema_version VALUES (1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS adx_accounts.users (
    user_id text NOT NULL,
    provider text NOT NULL DEFAULT 'huawei',
    display_name text CHECK (char_length(display_name) <= 128),
    developer_scope text NOT NULL,
    client_id text NOT NULL,
    union_id text COLLATE "C" NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled','deleted')),
    agreement_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id),
    UNIQUE (provider,developer_scope,union_id)
);
CREATE TABLE IF NOT EXISTS adx_accounts.sessions (
    token_digest text PRIMARY KEY,
    user_id text NOT NULL,
    expires_at bigint NOT NULL,
    revoked_at timestamptz,
    FOREIGN KEY (user_id) REFERENCES adx_accounts.users(user_id)
);
CREATE INDEX IF NOT EXISTS sessions_expiry ON adx_accounts.sessions(expires_at);
CREATE TABLE IF NOT EXISTS adx_accounts.model_credentials (
    user_id text NOT NULL,
    credential_version text NOT NULL,
    encrypted_key bytea NOT NULL,
    ready boolean NOT NULL DEFAULT false,
    PRIMARY KEY (user_id),
    FOREIGN KEY (user_id) REFERENCES adx_accounts.users(user_id)
);
CREATE TABLE IF NOT EXISTS adx_accounts.user_secrets (
    tenant_id text NOT NULL CHECK (octet_length(tenant_id) BETWEEN 1 AND 128),
    workspace_id text NOT NULL CHECK (octet_length(workspace_id) BETWEEN 1 AND 128),
    user_id text NOT NULL,
    secret_key text NOT NULL CHECK (octet_length(secret_key) BETWEEN 1 AND 128),
    revision text NOT NULL CHECK (octet_length(revision) = 36),
    key_id text CHECK (octet_length(key_id) BETWEEN 1 AND 128),
    encrypted_value bytea CHECK (octet_length(encrypted_value) BETWEEN 28 AND 131100),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, workspace_id, user_id, secret_key),
    FOREIGN KEY (user_id) REFERENCES adx_accounts.users(user_id),
    CHECK ((key_id IS NULL) = (encrypted_value IS NULL))
);
-- Candidate updates are staged until the workspace records transaction commits.
CREATE TABLE IF NOT EXISTS adx_accounts.privacy_transactions (
    transaction_id uuid PRIMARY KEY,
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    user_id text NOT NULL REFERENCES adx_accounts.users(user_id),
    secret_key text NOT NULL,
    expected_revision text,
    candidate_revision text NOT NULL CHECK (octet_length(candidate_revision) = 36),
    candidate_key_id text,
    candidate_value bytea,
    state text NOT NULL CHECK (state IN ('pending','committed','aborted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    CHECK ((candidate_key_id IS NULL) = (candidate_value IS NULL))
);
ALTER TABLE adx_accounts.privacy_transactions ADD COLUMN IF NOT EXISTS finished_at timestamptz;
UPDATE adx_accounts.privacy_transactions SET finished_at=created_at WHERE state <> 'pending' AND finished_at IS NULL;
CREATE INDEX IF NOT EXISTS privacy_transactions_owner
    ON adx_accounts.privacy_transactions(tenant_id,workspace_id,user_id,finished_at);
CREATE UNIQUE INDEX IF NOT EXISTS privacy_one_pending_update
    ON adx_accounts.privacy_transactions(tenant_id,workspace_id,user_id,secret_key)
    WHERE state = 'pending';
-- Business authorization only: no shared identity directories or platform lifecycle state.
CREATE TABLE IF NOT EXISTS adx_accounts.privacy_grants (
    token_digest text PRIMARY KEY CHECK (octet_length(token_digest) = 64),
    tenant_id text NOT NULL,
    workspace_id text NOT NULL,
    user_id text NOT NULL REFERENCES adx_accounts.users(user_id),
    scope_json text NOT NULL CHECK (octet_length(scope_json) <= 8192),
    binding_generation text NOT NULL,
    expires_at bigint NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS privacy_grants_expiry ON adx_accounts.privacy_grants(expires_at);
COMMIT;
