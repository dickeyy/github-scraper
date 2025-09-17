package services

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/google/go-github/v74/github"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

var (
	GitHubClient *github.Client
)

func InitGitHub(ctx context.Context) {
	token := os.Getenv("GITHUB_TOKEN")
	if token != "" {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
		tc := oauth2.NewClient(ctx, ts)
		GitHubClient = github.NewClient(tc)
		log.Info().Bool("token_present", true).Msg("GitHub client initialized")
		return
	}

	GitHubClient = github.NewClient(nil)
	log.Info().Bool("token_present", false).Msg("GitHub client initialized")
}

func GetPRs(ctx context.Context, owner, repo string) ([]*github.PullRequest, error) {
	if GitHubClient == nil {
		return nil, errors.New("GitHub client not initialized")
	}

	prs, _, err := GitHubClient.PullRequests.List(ctx, owner, repo, &github.PullRequestListOptions{
		State:     "all",
		Sort:      "created",
		Direction: "desc",
		ListOptions: github.ListOptions{
			PerPage: 100,
		},
	})
	if err != nil {
		return nil, err
	}

	return prs, nil
}

// GetAllPRs fetches all PRs from the repository, paginating through results
// and respecting GitHub API rate limits and abuse detection backoffs.
func GetAllPRs(ctx context.Context, owner, repo string) ([]*github.PullRequest, error) {
	if GitHubClient == nil {
		return nil, errors.New("GitHub client not initialized")
	}

	opts := &github.PullRequestListOptions{
		State:     "all",
		Sort:      "created",
		Direction: "desc",
		ListOptions: github.ListOptions{
			PerPage: 100,
			Page:    1,
		},
	}

	log.Info().Str("owner", owner).Str("repo", repo).Int("per_page", opts.PerPage).Msg("begin fetching PRs")

	var allPRs []*github.PullRequest

	for {
		var (
			pagePRs []*github.PullRequest
			resp    *github.Response
			err     error
		)

		log.Debug().Str("owner", owner).Str("repo", repo).Int("page", opts.Page).Int("per_page", opts.PerPage).Msg("fetching PR page")

		for {
			pagePRs, resp, err = GitHubClient.PullRequests.List(ctx, owner, repo, opts)
			if err == nil {
				break
			}

			if rlErr, ok := err.(*github.RateLimitError); ok {
				resetAt := rlErr.Rate.Reset.Time
				sleepFor := time.Until(resetAt) + time.Second
				if sleepFor < 0 {
					sleepFor = 5 * time.Second
				}
				log.Warn().Time("reset_at", resetAt).Dur("sleep_for", sleepFor).Msg("rate limit reached; sleeping")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleepFor):
				}
				continue
			}

			if abuseErr, ok := err.(*github.AbuseRateLimitError); ok {
				var sleepFor time.Duration
				if abuseErr.RetryAfter != nil {
					sleepFor = *abuseErr.RetryAfter
				} else {
					sleepFor = 10 * time.Second
				}
				log.Warn().Dur("sleep_for", sleepFor).Msg("abuse detection triggered; backing off")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleepFor):
				}
				continue
			}

			if resp != nil && resp.Response != nil && resp.Response.StatusCode >= 500 {
				log.Warn().Int("status", resp.Response.StatusCode).Msg("server error; retrying")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(3 * time.Second):
				}
				continue
			}

			return nil, err
		}

		nextTotal := len(allPRs) + len(pagePRs)
		if resp != nil {
			log.Info().Str("owner", owner).Str("repo", repo).Int("page", opts.Page).Int("page_count", len(pagePRs)).Int("total_so_far", nextTotal).Int("rate_remaining", resp.Rate.Remaining).Time("rate_reset", resp.Rate.Reset.Time).Msg("fetched PR page")
		} else {
			log.Info().Str("owner", owner).Str("repo", repo).Int("page", opts.Page).Int("page_count", len(pagePRs)).Int("total_so_far", nextTotal).Msg("fetched PR page")
		}

		allPRs = append(allPRs, pagePRs...)

		if resp == nil || resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	log.Info().Str("owner", owner).Str("repo", repo).Int("total", len(allPRs)).Msg("completed fetching PRs")

	return allPRs, nil
}

// GetPRWithBackoff fetches a single PR with rate-limit and abuse backoff handling.
func GetPRWithBackoff(ctx context.Context, owner, repo string, number int) (*github.PullRequest, error) {
	if GitHubClient == nil {
		return nil, errors.New("GitHub client not initialized")
	}

	for {
		pr, resp, err := GitHubClient.PullRequests.Get(ctx, owner, repo, number)
		if err == nil {
			return pr, nil
		}

		if rlErr, ok := err.(*github.RateLimitError); ok {
			resetAt := rlErr.Rate.Reset.Time
			sleepFor := time.Until(resetAt) + time.Second
			if sleepFor < 0 {
				sleepFor = 5 * time.Second
			}
			log.Warn().Int("number", number).Time("reset_at", resetAt).Dur("sleep_for", sleepFor).Msg("rate limit reached while fetching PR; sleeping")
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepFor):
			}
			continue
		}

		if abuseErr, ok := err.(*github.AbuseRateLimitError); ok {
			var sleepFor time.Duration
			if abuseErr.RetryAfter != nil {
				sleepFor = *abuseErr.RetryAfter
			} else {
				sleepFor = 10 * time.Second
			}
			log.Warn().Int("number", number).Dur("sleep_for", sleepFor).Msg("abuse detection triggered while fetching PR; backing off")
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepFor):
			}
			continue
		}

		if resp != nil && resp.Response != nil && resp.Response.StatusCode >= 500 {
			log.Warn().Int("number", number).Int("status", resp.Response.StatusCode).Msg("server error while fetching PR; retrying")
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(3 * time.Second):
			}
			continue
		}

		return nil, err
	}
}
