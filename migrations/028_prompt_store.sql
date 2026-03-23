CREATE TABLE IF NOT EXISTS prompts (
    prompt_key       TEXT PRIMARY KEY,
    title            TEXT NOT NULL,
    description      TEXT NOT NULL DEFAULT '',
    current_version  INTEGER NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prompt_revisions (
    prompt_key       TEXT NOT NULL REFERENCES prompts(prompt_key) ON DELETE CASCADE,
    version          INTEGER NOT NULL,
    content          TEXT NOT NULL,
    source_path      TEXT,
    change_summary   TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (prompt_key, version)
);

CREATE INDEX IF NOT EXISTS idx_prompt_revisions_prompt_created
    ON prompt_revisions (prompt_key, created_at DESC);
