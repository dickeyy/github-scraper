package scraper

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/dickeyy/github-scraper/db"
	"github.com/dickeyy/github-scraper/services"
	"github.com/dickeyy/github-scraper/types"
	"github.com/google/go-github/v74/github"
	"github.com/rs/zerolog/log"
)

type job struct{ number int }

type result struct {
	number   int
	row      types.PRRow
	inserted bool
	err      error
}

// Run orchestrates fetching PR numbers, concurrently retrieving details, building rows,
// inserting into Postgres, and logging periodic progress.
func Run(ctx context.Context, owner, repo string, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}

	prs, err := services.GetAllPRs(ctx, owner, repo)
	if err != nil {
		return err
	}

	jobNumbers := make([]int, 0, len(prs))
	for _, pr := range prs {
		if pr == nil || pr.Number == nil {
			continue
		}
		jobNumbers = append(jobNumbers, *pr.Number)
	}
	total := len(jobNumbers)
	log.Info().Str("owner", owner).Str("repo", repo).Int("total_prs", total).Msg("ready to process PRs")
	if total == 0 {
		return nil
	}

	jobs := make(chan job)
	results := make(chan result)
	var processed atomic.Int64
	var inserted atomic.Int64
	var errs atomic.Int64

	// Workers
	for w := 0; w < concurrency; w++ {
		go func() {
			for j := range jobs {
				full, err := services.GetPRWithBackoff(ctx, owner, repo, j.number)
				if err != nil {
					results <- result{number: j.number, err: err}
					continue
				}

				// Compute comment breakdown (total and bot-only)
				breakdown, berr := services.GetPRCommentsBreakdown(ctx, owner, repo, j.number)
				if berr != nil {
					results <- result{number: j.number, err: berr}
					continue
				}

				row := buildPRRow(full, owner, repo, j.number, breakdown.TotalComments, breakdown.BotComments)

				ins := false
				if db.Pool != nil {
					if err := db.InsertPRRow(ctx, row); err != nil {
						results <- result{number: j.number, err: err}
						continue
					}
					ins = true
				}

				results <- result{number: j.number, row: row, inserted: ins}
			}
		}()
	}

	// Dispatch jobs
	go func() {
		for _, num := range jobNumbers {
			select {
			case <-ctx.Done():
				close(jobs)
				return
			case jobs <- job{number: num}:
			}
		}
		close(jobs)
	}()

	done := make(chan struct{})
	// Periodic progress logger
	go func(totalJobs int) {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				p := processed.Load()
				i := inserted.Load()
				e := errs.Load()
				remaining := int64(totalJobs) - p - e
				if remaining < 0 {
					remaining = 0
				}
				log.Info().
					Str("owner", owner).
					Str("repo", repo).
					Int("total", totalJobs).
					Int64("processed", p).
					Int64("inserted", i).
					Int64("errors", e).
					Int64("remaining", remaining).
					Msg("PR processing progress")
			}
		}
	}(total)

	// Consume results
	for i := 0; i < total; i++ {
		select {
		case <-ctx.Done():
			close(done)
			return ctx.Err()
		case res := <-results:
			if res.err != nil {
				errs.Add(1)
				log.Error().Int("number", res.number).Err(res.err).Msg("failed to process PR")
				continue
			}
			processed.Add(1)
			if res.inserted {
				inserted.Add(1)
			}
		}
	}

	close(done)

	log.Info().
		Str("owner", owner).
		Str("repo", repo).
		Int("total", total).
		Int64("processed", processed.Load()).
		Int64("inserted", inserted.Load()).
		Int64("errors", errs.Load()).
		Msg("completed PR processing")

	return nil
}

func buildPRRow(full *github.PullRequest, owner, repo string, number int, commentCount int, botComments int) types.PRRow {

	additions := 0
	if full.Additions != nil {
		additions = *full.Additions
	}
	deletions := 0
	if full.Deletions != nil {
		deletions = *full.Deletions
	}
	linesChanged := additions + deletions

	createdAt := time.Now()
	if full.CreatedAt != nil {
		createdAt = full.CreatedAt.Time
	}

	return types.PRRow{
		ID:           number,
		Repo:         repo,
		Owner:        owner,
		CommentCount: commentCount,
		BotComments:  botComments,
		LinesChanged: linesChanged,
		CreatedAt:    createdAt,
	}
}
