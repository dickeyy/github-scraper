package main

import (
	"context"
	"flag"
	"os"

	"github.com/dickeyy/github-scraper/db"
	"github.com/dickeyy/github-scraper/scraper"
	"github.com/dickeyy/github-scraper/services"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load .env file")
	}

	var (
		owner       string
		repo        string
		concurrency int
	)

	flag.StringVar(&owner, "owner", "", "GitHub repository owner/org")
	flag.StringVar(&repo, "repo", "", "GitHub repository name")
	flag.IntVar(&concurrency, "concurrency", 4, "Number of workers for detail fetch + insert")
	flag.Parse()

	if owner == "" || repo == "" {
		log.Fatal().Msg("owner and repo flags are required")
	}

	ctx := context.Background()
	services.InitGitHub(ctx)

	if err := db.Init(ctx); err != nil {
		log.Fatal().Err(err).Msg("failed to connect to Postgres")
	}
	defer db.Close()

	if err := scraper.Run(ctx, owner, repo, concurrency); err != nil {
		log.Fatal().Err(err).Msg("scrape failed")
	}
}
