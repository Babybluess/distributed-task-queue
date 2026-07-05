package task

import (
	"encoding/json"
	"time"
)

type ResultStatus string

const (
	StatusSuccess ResultStatus = "success"
	StatusFailure ResultStatus = "failure"
)

// Result is the outcome of a completed task, stored by the worker so
// producers can look it up later — the AsyncResult pattern from Celery.
// A missing result means the task hasn't finished (or is still pending
// retries) rather than that it failed.
type Result struct {
	TaskID      string          `json:"task_id"`
	Status      ResultStatus    `json:"status"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	CompletedAt time.Time       `json:"completed_at"`
}
