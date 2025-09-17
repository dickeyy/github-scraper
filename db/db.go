package db

import (
	"context"
	"fmt"
	"os"

	"github.com/dickeyy/github-scraper/types"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

var (
	Pool *pgxpool.Pool
)

func Init(ctx context.Context) error {
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), os.Getenv("POSTGRES_HOST"), os.Getenv("POSTGRES_PORT"), os.Getenv("POSTGRES_DB"))
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
            bot_comments INTEGER NOT NULL DEFAULT 0,
            lines_changed INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );
        ALTER TABLE prs
            ADD COLUMN IF NOT EXISTS bot_comments INTEGER NOT NULL DEFAULT 0;
    `)
	return err
}

func InsertPRRow(ctx context.Context, row types.PRRow) error {
	id := fmt.Sprintf("%d:%s:%s", row.ID, row.Owner, row.Repo)
	_, err := Pool.Exec(ctx, `
        INSERT INTO prs (id, owner, repo, comment_count, bot_comments, lines_changed, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id)
        DO UPDATE SET
            owner = EXCLUDED.owner,
            repo = EXCLUDED.repo,
            comment_count = EXCLUDED.comment_count,
            bot_comments = EXCLUDED.bot_comments,
            lines_changed = EXCLUDED.lines_changed,
            created_at = EXCLUDED.created_at;
    `, id, row.Owner, row.Repo, row.CommentCount, row.BotComments, row.LinesChanged, row.CreatedAt)
	if err == nil {
		log.Debug().Str("id", id).Str("owner", row.Owner).Str("repo", row.Repo).Msg("inserted PR row")
	}
	return err
}

func Close() {
	if Pool != nil {
		Pool.Close()
	}
}
