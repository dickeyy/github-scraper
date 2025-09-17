CREATE TABLE IF NOT EXISTS prs (
    id SERIAL PRIMARY KEY,
    repo TEXT NOT NULL,
    owner TEXT NOT NULL,
    comment_count INTEGER NOT NULL,
    lines_changed INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);