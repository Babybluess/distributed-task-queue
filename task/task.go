package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Retries   int             `json:"retries"`
	MaxRetry  int             `json:"max_retry"`
	CreatedAt time.Time       `json:"created_at"`
}

func New(taskType string, payload any, maxRetry int) (*Task, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return &Task{
		ID:        fmt.Sprintf("task_%d", time.Now().UnixNano()),
		Type:      taskType,
		Payload:   b,
		MaxRetry:  maxRetry,
		CreatedAt: time.Now(),
	}, nil
}

type HandlerFunc func(ctx context.Context, t *Task) error

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
