package types

import "time"

type PRRow struct {
	ID           int       `json:"id"`
	Repo         string    `json:"repo"`
	Owner        string    `json:"owner"`
	CommentCount int       `json:"comment_count"`
	LinesChanged int       `json:"lines_changed"`
	CreatedAt    time.Time `json:"created_at"`
}
