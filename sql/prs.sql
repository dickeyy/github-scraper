CREATE TABLE IF NOT EXISTS prs (
    id TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    owner TEXT NOT NULL,
    comment_count INTEGER NOT NULL,
    bot_comments INTEGER NOT NULL DEFAULT 0,
    lines_changed INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);