package db

import (
	"context"

	"github.com/dickeyy/github-scraper/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

var (
	Pool *pgxpool.Pool
)

func Init(ctx context.Context, connString string) error {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return err
	}
	Pool = pool
	log.Info().Msg("connected to Postgres")
	return ensureSchema(ctx)
}

func ensureSchema(ctx context.Context) error {
	_, err := Pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS prs (
			id INTEGER PRIMARY KEY,
			repo TEXT NOT NULL,
			owner TEXT NOT NULL,
			comment_count INTEGER NOT NULL,
			lines_changed INTEGER NOT NULL,
			created_at TIMESTAMPTZ NOT NULL
		);
	`)
	return err
}

func InsertPRRow(ctx context.Context, row types.PRRow) error {
	_, err := Pool.Exec(ctx, `
		INSERT INTO prs (id, owner, repo, comment_count, lines_changed, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id)
		DO UPDATE SET
			owner = EXCLUDED.owner,
			repo = EXCLUDED.repo,
			comment_count = EXCLUDED.comment_count,
			lines_changed = EXCLUDED.lines_changed,
			created_at = EXCLUDED.created_at;
	`, row.ID, row.Owner, row.Repo, row.CommentCount, row.LinesChanged, row.CreatedAt)
	if err == nil {
		log.Debug().Int("id", row.ID).Str("owner", row.Owner).Str("repo", row.Repo).Msg("inserted PR row")
	}
	return err
}

func Close() {
	if Pool != nil {
		Pool.Close()
	}
}
