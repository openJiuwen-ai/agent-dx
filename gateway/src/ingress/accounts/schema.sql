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
COMMIT;
