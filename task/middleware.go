package task

import (
	"context"
	"fmt"
	"log"
	"time"
)

type Middleware func(HandlerFunc) HandlerFunc

// Chain composes middlewares into a single Middleware.
func Chain(mw ...Middleware) Middleware {
	return func(final HandlerFunc) HandlerFunc {
		for i := len(mw) - 1; i >= 0; i-- {
			final = mw[i](final)
		}
		return final
	}
}

// Logging returns a Middleware that logs each task's outcome and duration.
func Logging(logger *log.Logger) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, t *Task) (any, error) {
			start := time.Now()
			out, err := next(ctx, t)
			if err != nil {
				logger.Printf("task %s type=%s failed in %s: %v", t.ID, t.Type, time.Since(start), err)
			} else {
				logger.Printf("task %s type=%s succeeded in %s", t.ID, t.Type, time.Since(start))
			}
			return out, err
		}
	}
}

// Recover returns a Middleware that turns a panicking handler into a
// returned error, so one bad task can't take down the worker goroutine
// running it.
func Recover() Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, t *Task) (out any, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic in handler for task %s type=%s: %v", t.ID, t.Type, r)
				}
			}()
			return next(ctx, t)
		}
	}
}

// Metrics returns a Middleware that reports each task's execution duration
// and outcome (nil error on success) to record.
func Metrics(record func(taskType string, dur time.Duration, err error)) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, t *Task) (any, error) {
			start := time.Now()
			out, err := next(ctx, t)
			record(t.Type, time.Since(start), err)
			return out, err
		}
	}
}
