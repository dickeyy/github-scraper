package services

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
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

// CommentsBreakdown holds counts for total comments and bot-only comments across
// issue comments and review comments for a PR. "Comments" includes both types.
type CommentsBreakdown struct {
	TotalComments int
	BotComments   int
}

// GetPRCommentsBreakdown returns total and bot comment counts for a PR by
// fetching issue comments and review comments with pagination and robust
// backoff handling.
func GetPRCommentsBreakdown(ctx context.Context, owner, repo string, number int) (CommentsBreakdown, error) {
	if GitHubClient == nil {
		return CommentsBreakdown{}, errors.New("GitHub client not initialized")
	}

	var breakdown CommentsBreakdown

	// Helper to determine if a comment user is a bot
	isBot := func(u *github.User) bool {
		if u == nil || u.Type == nil {
			return false
		}
		return *u.Type == "Bot"
	}

	// Paginate Issue Comments (a.k.a. PR comments on the conversation tab)
	issueOpts := &github.IssueListCommentsOptions{
		ListOptions: github.ListOptions{PerPage: 100, Page: 1},
	}
	for {
		var (
			comments []*github.IssueComment
			resp     *github.Response
			err      error
		)
		for {
			comments, resp, err = GitHubClient.Issues.ListComments(ctx, owner, repo, number, issueOpts)
			if err == nil {
				break
			}
			if rlErr, ok := err.(*github.RateLimitError); ok {
				resetAt := rlErr.Rate.Reset.Time
				sleepFor := time.Until(resetAt) + time.Second
				if sleepFor < 0 {
					sleepFor = 5 * time.Second
				}
				log.Warn().Int("number", number).Time("reset_at", resetAt).Dur("sleep_for", sleepFor).Msg("rate limit while listing issue comments; sleeping")
				select {
				case <-ctx.Done():
					return CommentsBreakdown{}, ctx.Err()
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
				log.Warn().Int("number", number).Dur("sleep_for", sleepFor).Msg("abuse while listing issue comments; backing off")
				select {
				case <-ctx.Done():
					return CommentsBreakdown{}, ctx.Err()
				case <-time.After(sleepFor):
				}
				continue
			}
			if resp != nil && resp.Response != nil && resp.Response.StatusCode >= 500 {
				log.Warn().Int("number", number).Int("status", resp.Response.StatusCode).Msg("server error listing issue comments; retrying")
				select {
				case <-ctx.Done():
					return CommentsBreakdown{}, ctx.Err()
				case <-time.After(3 * time.Second):
				}
				continue
			}
			return CommentsBreakdown{}, err
		}
		for _, c := range comments {
			breakdown.TotalComments++
			if isBot(c.User) {
				breakdown.BotComments++
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		issueOpts.Page = resp.NextPage
	}

	// Paginate Review Comments (comments on diffs)
	reviewOpts := &github.PullRequestListCommentsOptions{ListOptions: github.ListOptions{PerPage: 100, Page: 1}}
	for {
		var (
			comments []*github.PullRequestComment
			resp     *github.Response
			err      error
		)
		for {
			comments, resp, err = GitHubClient.PullRequests.ListComments(ctx, owner, repo, number, reviewOpts)
			if err == nil {
				break
			}
			if rlErr, ok := err.(*github.RateLimitError); ok {
				resetAt := rlErr.Rate.Reset.Time
				sleepFor := time.Until(resetAt) + time.Second
				if sleepFor < 0 {
					sleepFor = 5 * time.Second
				}
				log.Warn().Int("number", number).Time("reset_at", resetAt).Dur("sleep_for", sleepFor).Msg("rate limit while listing review comments; sleeping")
				select {
				case <-ctx.Done():
					return CommentsBreakdown{}, ctx.Err()
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
				log.Warn().Int("number", number).Dur("sleep_for", sleepFor).Msg("abuse while listing review comments; backing off")
				select {
				case <-ctx.Done():
					return CommentsBreakdown{}, ctx.Err()
				case <-time.After(sleepFor):
				}
				continue
			}
			if resp != nil && resp.Response != nil && resp.Response.StatusCode >= 500 {
				log.Warn().Int("number", number).Int("status", resp.Response.StatusCode).Msg("server error listing review comments; retrying")
				select {
				case <-ctx.Done():
					return CommentsBreakdown{}, ctx.Err()
				case <-time.After(3 * time.Second):
				}
				continue
			}
			return CommentsBreakdown{}, err
		}
		for _, c := range comments {
			breakdown.TotalComments++
			if isBot(c.User) {
				breakdown.BotComments++
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		reviewOpts.Page = resp.NextPage
	}

	return breakdown, nil
}

// GetRepoCommentsBreakdown aggregates comment counts for all PRs in the given
// set by scanning repository-level endpoints, drastically reducing request
// volume compared to per-PR calls. If prNumberSet is nil or empty, all
// comments will be scanned but none will be recorded.
func GetRepoCommentsBreakdown(ctx context.Context, owner, repo string, prNumberSet map[int]struct{}) (map[int]CommentsBreakdown, error) {
	if GitHubClient == nil {
		return nil, errors.New("GitHub client not initialized")
	}

	breakdowns := make(map[int]CommentsBreakdown)

	// Helper to determine if a comment user is a bot
	isBot := func(u *github.User) bool {
		if u == nil || u.Type == nil {
			return false
		}
		return *u.Type == "Bot"
	}

	// Helper to record counts for a PR
	record := func(prNumber int, bot bool) {
		if _, ok := prNumberSet[prNumber]; !ok {
			return
		}
		bd := breakdowns[prNumber]
		bd.TotalComments++
		if bot {
			bd.BotComments++
		}
		breakdowns[prNumber] = bd
	}

	// Extract trailing integer from a URL/path string
	extractTrailingInt := func(s string) (int, bool) {
		if s == "" {
			return 0, false
		}
		// Trim query/fragment
		if idx := strings.IndexByte(s, '?'); idx >= 0 {
			s = s[:idx]
		}
		if idx := strings.IndexByte(s, '#'); idx >= 0 {
			s = s[:idx]
		}
		parts := strings.Split(strings.TrimRight(s, "/"), "/")
		if len(parts) == 0 {
			return 0, false
		}
		last := parts[len(parts)-1]
		n, err := strconv.Atoi(last)
		if err != nil {
			return 0, false
		}
		return n, true
	}

	// 1) Repository-level Issue Comments
	issPage := 1
	for {
		endpoint := strings.Builder{}
		endpoint.WriteString("repos/")
		endpoint.WriteString(owner)
		endpoint.WriteString("/")
		endpoint.WriteString(repo)
		endpoint.WriteString("/issues/comments?per_page=100&page=")
		endpoint.WriteString(strconv.Itoa(issPage))

		req, err := GitHubClient.NewRequest("GET", endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		var comments []*github.IssueComment
		resp, err := GitHubClient.Do(ctx, req, &comments)
		if err != nil {
			if rlErr, ok := err.(*github.RateLimitError); ok {
				resetAt := rlErr.Rate.Reset.Time
				sleepFor := time.Until(resetAt) + time.Second
				if sleepFor < 0 {
					sleepFor = 5 * time.Second
				}
				log.Warn().Dur("sleep_for", sleepFor).Msg("rate limit while listing repo issue comments; sleeping")
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
				log.Warn().Dur("sleep_for", sleepFor).Msg("abuse while listing repo issue comments; backing off")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleepFor):
				}
				continue
			}
			// Non-2xx or other errors; small backoff and retry
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(3 * time.Second):
			}
			continue
		}
		for _, c := range comments {
			if c == nil || c.User == nil {
				continue
			}
			// Comment belongs to an issue number
			if c.IssueURL != nil {
				if n, ok := extractTrailingInt(*c.IssueURL); ok {
					record(n, isBot(c.User))
				}
			}
		}
		if resp == nil || resp.NextPage == 0 {
			break
		}
		issPage = resp.NextPage
	}

	// 2) Repository-level Review Comments (code comments)
	// Use a manual request as the go-github method for repo-level review comments may not be exposed.
	revPage := 1
	for {
		endpoint := strings.Builder{}
		endpoint.WriteString("repos/")
		endpoint.WriteString(owner)
		endpoint.WriteString("/")
		endpoint.WriteString(repo)
		endpoint.WriteString("/pulls/comments?per_page=100&page=")
		endpoint.WriteString(strconv.Itoa(revPage))

		req, err := GitHubClient.NewRequest("GET", endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		var comments []*github.PullRequestComment
		resp, err := GitHubClient.Do(ctx, req, &comments)
		if err != nil {
			if rlErr, ok := err.(*github.RateLimitError); ok {
				resetAt := rlErr.Rate.Reset.Time
				sleepFor := time.Until(resetAt) + time.Second
				if sleepFor < 0 {
					sleepFor = 5 * time.Second
				}
				log.Warn().Dur("sleep_for", sleepFor).Msg("rate limit while listing repo review comments; sleeping")
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
				log.Warn().Dur("sleep_for", sleepFor).Msg("abuse while listing repo review comments; backing off")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleepFor):
				}
				continue
			}
			// Non-2xx handled above; 5xx may not be parsed to Response; retry basic backoff
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(3 * time.Second):
			}
			continue
		}

		for _, c := range comments {
			if c == nil || c.User == nil {
				continue
			}
			// Prefer PullRequestURL to extract PR number; fallback to HTMLURL
			var prNumber int
			var ok bool
			if c.PullRequestURL != nil {
				prNumber, ok = extractTrailingInt(*c.PullRequestURL)
			}
			if !ok && c.HTMLURL != nil {
				prNumber, ok = extractTrailingInt(*c.HTMLURL)
			}
			if ok {
				record(prNumber, isBot(c.User))
			}
		}

		if resp == nil || resp.NextPage == 0 {
			break
		}
		revPage = resp.NextPage
	}

	return breakdowns, nil
}
