CREATE SCHEMA IF NOT EXISTS digitalmate_composio_budget;
CREATE TABLE IF NOT EXISTS digitalmate_composio_budget.version (
    version integer PRIMARY KEY CHECK (version = 1)
);
INSERT INTO digitalmate_composio_budget.version VALUES (1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS digitalmate_composio_budget.organizations (
    organization text PRIMARY KEY,
    organization_limit bigint NOT NULL CHECK (organization_limit > 0),
    user_limit bigint NOT NULL CHECK (user_limit > 0 AND user_limit <= organization_limit),
    window_id bigint NOT NULL DEFAULT -1,
    used bigint NOT NULL DEFAULT 0 CHECK (used >= 0)
);
CREATE TABLE IF NOT EXISTS digitalmate_composio_budget.users (
    organization text NOT NULL REFERENCES digitalmate_composio_budget.organizations,
    identity text NOT NULL,
    window_id bigint NOT NULL,
    used bigint NOT NULL CHECK (used > 0),
    PRIMARY KEY (organization, identity)
);
CREATE INDEX IF NOT EXISTS composio_budget_users_window
    ON digitalmate_composio_budget.users (organization, window_id);
