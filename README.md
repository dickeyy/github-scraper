# GitHub PR Scraper

A small Go tool that fetches all pull requests for a given GitHub repository and stores summary metrics in Postgres.

### Features

- Fetches all PRs (state: all) via GitHub API (with token support)
- Handles rate limits and abuse detection backoffs
- Concurrent detail fetch and insert into Postgres
- Structured logging with zerolog

## Prerequisites

- Go (to run locally) – see `go.mod` for version
- Docker and Docker Compose (to run Postgres easily)
- A GitHub token (optional but recommended to increase API limits)

## Environment Variables

This project reads configuration from environment variables. You can set them in your shell or create a `.env.local` file in the project root. The file `.env.local` is already gitignored.

Required by the app:

- `POSTGRES_HOST` (e.g., `localhost` when using Docker Compose)
- `POSTGRES_PORT` (e.g., `5432`)
- `POSTGRES_DB` (e.g., `scraper`)
- `POSTGRES_USER` (e.g., `postgres`)
- `POSTGRES_PASSWORD` (secure password)
- `GITHUB_TOKEN` (optional; recommended)

When using Docker Compose, `docker-compose.yml` will read these env vars for the Postgres container as well. You may put them in a `.env` file (Compose automatically loads `.env`), or export them in your shell before running Compose.

Example `.env` (used by the Go app):

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=scraper
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
GITHUB_TOKEN=ghp_your_token_here
```

Example `.env` (used by Docker Compose):

```env
POSTGRES_DB=scraper
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_PORT=5432
```

## Database (Docker Compose)

Bring up Postgres with Docker Compose:

```bash
docker compose up -d
```

- The service exposes port `5432` on your host.
- On first start, the schema from `sql/prs.sql` is applied automatically.
- Healthcheck waits for Postgres to be ready.

Rebuild the Postgres image (if you changed the Compose config or need a fresh container):

```bash
docker compose up -d --build
```

Stop services without removing volumes:

```bash
docker compose stop
```

Stop and remove containers, networks (keeps volumes):

```bash
docker compose down
```

Destroy everything including the persistent volume (DANGER: deletes data):

```bash
docker compose down -v
```

View logs:

```bash
docker compose logs -f postgres
```

## Running the Scraper (Locally)

Ensure Postgres is running (via Compose) and your `.env.local` is configured. Then run:

```bash
go run ./... -owner <org-or-user> -repo <repository> -concurrency 4
```

Flags:

- `-owner` (required): GitHub repository owner/org
- `-repo` (required): GitHub repository name
- `-concurrency` (optional, default 4): number of workers fetching PR details

## Data Model

PR rows are stored in the `prs` table with the following fields:

- `id` (int, primary key)
- `owner` (text)
- `repo` (text)
- `comment_count` (int)
- `bot_comments` (int)
- `lines_changed` (int)
- `created_at` (timestamptz)

The table is created automatically on startup if it doesn’t exist.

## Notes

- The GitHub client uses an access token if `GITHUB_TOKEN` is present. Without a token, it uses the unauthenticated client (with lower rate limits).
- The application logs progress every few seconds and prints a final summary.
- `.env.local` is loaded automatically by the app on startup.

## License

See [LICENSE](LICENSE).
