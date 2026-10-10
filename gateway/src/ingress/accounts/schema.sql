-- Apply with a schema-owner role before starting Ingress. Runtime needs DML only.
BEGIN;
CREATE SCHEMA IF NOT EXISTS adx_accounts;
CREATE TABLE IF NOT EXISTS adx_accounts.schema_version (version integer PRIMARY KEY);
INSERT INTO adx_accounts.schema_version VALUES (1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS adx_accounts.users (
    tenant text NOT NULL,
    user_id text NOT NULL,
    provider text NOT NULL DEFAULT 'huawei',
    developer_scope text NOT NULL,
    client_id text NOT NULL,
    union_id text COLLATE "C" NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled','deleted')),
    agreement_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant,user_id),
    UNIQUE (tenant,provider,developer_scope,union_id)
);
CREATE TABLE IF NOT EXISTS adx_accounts.sessions (
    token_digest text PRIMARY KEY,
    tenant text NOT NULL,
    user_id text NOT NULL,
    expires_at bigint NOT NULL,
    revoked_at timestamptz,
    FOREIGN KEY (tenant,user_id) REFERENCES adx_accounts.users(tenant,user_id)
);
CREATE INDEX IF NOT EXISTS sessions_expiry ON adx_accounts.sessions(expires_at);
CREATE TABLE IF NOT EXISTS adx_accounts.model_credentials (
    tenant text NOT NULL,
    user_id text NOT NULL,
    credential_version text NOT NULL,
    encrypted_key bytea NOT NULL,
    ready boolean NOT NULL DEFAULT false,
    PRIMARY KEY (tenant,user_id),
    FOREIGN KEY (tenant,user_id) REFERENCES adx_accounts.users(tenant,user_id)
);
COMMIT;
