-- P4.1: persistent backtest strategy templates
-- Run once against the Vercel/Neon application database before deploying
-- the P4.1 frontend/API changes.

CREATE TABLE IF NOT EXISTS backtest_strategy_templates (
    id              BIGSERIAL PRIMARY KEY,
    user_id         TEXT NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    config          JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bst_user_updated
    ON backtest_strategy_templates (user_id, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_bst_user_name
    ON backtest_strategy_templates (user_id, name);
