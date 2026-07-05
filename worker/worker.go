package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"gotasks/broker"
	"gotasks/task"
)

type Worker struct {
	broker      *broker.Broker
	registry    *task.Registry
	concurrency int
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

func New(b *broker.Broker, r *task.Registry, concurrency int) *Worker {
	return &Worker{
		broker:      b,
		registry:    r,
		concurrency: concurrency,
		stopCh:      make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := w.broker.FlushRetry(ctx); err != nil {
					log.Printf("flush retry: %v", err)
				}
				if err := w.broker.FlushScheduled(ctx); err != nil {
					log.Printf("flush scheduled: %v", err)
				}
			case <-w.stopCh:
				return
			}
		}
	}()

	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func(id int) {
			defer w.wg.Done()
			w.loop(ctx, id)
		}(i)
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
	log.Println("all workers stopped")
}

func (w *Worker) loop(ctx context.Context, id int) {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		t, err := w.broker.Dequeue(ctx, 2*time.Second)
		if err != nil {
			log.Printf("worker %d dequeue error: %v", id, err)
			continue
		}
		if t == nil {
			continue
		}

		log.Printf("worker %d picked up task %s (type=%s attempt=%d)", id, t.ID, t.Type, t.Retries+1)
		w.execute(ctx, t)
	}
}

func (w *Worker) execute(ctx context.Context, t *task.Task) {
	handler, ok := w.registry.Get(t.Type)
	if !ok {
		w.fail(ctx, t, fmt.Errorf("no handler registered for type %q", t.Type))
		return
	}

	taskCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	output, err := handler(taskCtx, t)
	if err != nil {
		w.fail(ctx, t, err)
		return
	}

	log.Printf("task %s DONE", t.ID)
	w.broker.Ack(ctx, t)

	raw, err := marshalOutput(output)
	if err != nil {
		log.Printf("task %s: marshal output: %v", t.ID, err)
	}
	w.storeResult(ctx, t, task.StatusSuccess, raw, "")
}

// fail records a failed attempt and, once the task has exhausted its
// retries (Nack has dead-lettered it), stores the final failure result.
func (w *Worker) fail(ctx context.Context, t *task.Task, err error) {
	log.Printf("task %s FAILED (attempt %d/%d): %v", t.ID, t.Retries+1, t.MaxRetry, err)
	w.broker.Nack(ctx, t, err)
	if t.Retries >= t.MaxRetry {
		w.storeResult(ctx, t, task.StatusFailure, nil, err.Error())
	}
}

func (w *Worker) storeResult(ctx context.Context, t *task.Task, status task.ResultStatus, output json.RawMessage, errMsg string) {
	r := &task.Result{
		TaskID:      t.ID,
		Status:      status,
		Output:      output,
		Error:       errMsg,
		CompletedAt: time.Now(),
	}
	if err := w.broker.SetResult(ctx, r); err != nil {
		log.Printf("task %s: store result: %v", t.ID, err)
	}
}

func marshalOutput(output any) (json.RawMessage, error) {
	if output == nil {
		return nil, nil
	}
	return json.Marshal(output)
}
