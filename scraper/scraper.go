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

	// Fetch PR minimal details via GraphQL in bulk
	lites, err := services.GetAllPRsGraphQL(ctx, owner, repo)
	if err != nil {
		return err
	}

	jobNumbers := make([]int, 0, len(lites))
	liteMap := make(map[int]services.PRLite, len(lites))
	for _, pr := range lites {
		jobNumbers = append(jobNumbers, pr.Number)
		liteMap[pr.Number] = pr
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

	// Preload repo-level comments breakdown to reduce API calls
	prSet := make(map[int]struct{}, len(jobNumbers))
	for _, n := range jobNumbers {
		prSet[n] = struct{}{}
	}
	repoBreakdowns, err := services.GetRepoCommentsBreakdown(ctx, owner, repo, prSet)
	if err != nil {
		log.Warn().Err(err).Msg("failed to preload repo-level comment breakdowns; falling back to per-PR calls")
	}

	// Workers
	for w := 0; w < concurrency; w++ {
		go func() {
			for j := range jobs {
				// We no longer need full PR REST call; using lite + comments

				// Get breakdown from preloaded map if available, else compute per-PR
				breakdown, ok := repoBreakdowns[j.number]
				if !ok {
					var berr error
					breakdown, berr = services.GetPRCommentsBreakdown(ctx, owner, repo, j.number)
					if berr != nil {
						results <- result{number: j.number, err: berr}
						continue
					}
				}

				// Build row using GraphQL lites for lines changed & createdAt
				lite := liteMap[j.number]
				createdAt := lite.CreatedAt
				additions := lite.Additions
				deletions := lite.Deletions
				linesChanged := additions + deletions

				row := types.PRRow{
					ID:           j.number,
					Repo:         repo,
					Owner:        owner,
					CommentCount: breakdown.TotalComments,
					BotComments:  breakdown.BotComments,
					LinesChanged: linesChanged,
					CreatedAt:    createdAt,
				}

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
