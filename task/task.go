package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

type Task struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    Priority        `json:"priority"`
	Retries     int             `json:"retries"`
	MaxRetry    int             `json:"max_retry"`
	CreatedAt   time.Time       `json:"created_at"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
}

// Option customizes a Task at construction time, e.g. WithPriority.
type Option func(*Task)

func WithPriority(p Priority) Option {
	return func(t *Task) { t.Priority = p }
}

// WithScheduledAt delays a task's first execution until the given time.
func WithScheduledAt(at time.Time) Option {
	return func(t *Task) { t.ScheduledAt = &at }
}

func New(taskType string, payload any, maxRetry int, opts ...Option) (*Task, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	t := &Task{
		ID:        fmt.Sprintf("task_%d", time.Now().UnixNano()),
		Type:      taskType,
		Payload:   b,
		Priority:  PriorityNormal,
		MaxRetry:  maxRetry,
		CreatedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

// HandlerFunc processes a task. The returned value, if non-nil, is
// JSON-marshaled and stored as the task's Result.Output on success.
type HandlerFunc func(ctx context.Context, t *Task) (any, error)

type Registry struct {
	handlers map[string]HandlerFunc
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

func (r *Registry) Register(taskType string, fn HandlerFunc) {
	r.handlers[taskType] = fn
}

func (r *Registry) Get(taskType string) (HandlerFunc, bool) {
	fn, ok := r.handlers[taskType]
	return fn, ok
}
