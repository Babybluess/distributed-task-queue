package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gotasks/metrics"
	"gotasks/task"
)

const (
	HighQueue       = "tasks:high"
	NormalQueue     = "tasks:normal"
	LowQueue        = "tasks:low"
	ProcessingSet   = "tasks:processing"
	RetryQueue      = "tasks:retry"
	ScheduledQueue  = "tasks:scheduled"
	DeadLetterQueue = "tasks:dead"
	processingTTL   = 5 * time.Minute

	// resultTTL bounds how long a completed task's result stays in Redis
	// before expiring, so the result hashes don't grow unbounded.
	resultTTL = 24 * time.Hour

	// highPriorityPollTimeout bounds how long Dequeue blocks on a higher
	// priority list before falling through to check the next one.
	highPriorityPollTimeout = 100 * time.Millisecond
)

// priorityQueues lists the pending lists in priority order, highest first.
var priorityQueues = []string{HighQueue, NormalQueue, LowQueue}

// ErrResultNotFound is returned by GetResult when a task hasn't completed
// yet (or its result has expired).
var ErrResultNotFound = errors.New("broker: result not found")

func resultKey(taskID string) string {
	return "task:result:" + taskID
}

func resultChannel(taskID string) string {
	return "task:result:" + taskID + ":events"
}

func queueFor(p task.Priority) string {
	switch p {
	case task.PriorityHigh:
		return HighQueue
	case task.PriorityLow:
		return LowQueue
	default:
		return NormalQueue
	}
}

type Broker struct {
	rdb *redis.Client
}

func New(addr string) *Broker {
	return &Broker{
		rdb: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (b *Broker) Enqueue(ctx context.Context, t *task.Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if t.ScheduledAt != nil && t.ScheduledAt.After(time.Now()) {
		score := float64(t.ScheduledAt.Unix())
		if err := b.rdb.ZAdd(ctx, ScheduledQueue, redis.Z{Score: score, Member: data}).Err(); err != nil {
			return err
		}
		metrics.TasksEnqueued.WithLabelValues(t.Type, string(t.Priority)).Inc()
		return nil
	}

	if err := b.rdb.LPush(ctx, queueFor(t.Priority), data).Err(); err != nil {
		return err
	}
	metrics.TasksEnqueued.WithLabelValues(t.Type, string(t.Priority)).Inc()
	return nil
}

func (b *Broker) Dequeue(ctx context.Context, timeout time.Duration) (*task.Task, error) {
	pollTimeout := min(highPriorityPollTimeout, timeout)

	for i, queue := range priorityQueues {
		qTimeout := pollTimeout
		if i == len(priorityQueues)-1 {
			qTimeout = timeout
		}

		t, err := b.dequeueFrom(ctx, queue, qTimeout)
		if t != nil || err != nil {
			return t, err
		}
	}
	return nil, nil
}

func (b *Broker) dequeueFrom(ctx context.Context, queue string, timeout time.Duration) (*task.Task, error) {
	result, err := b.rdb.BRPop(ctx, timeout, queue).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var t task.Task
	if err := json.Unmarshal([]byte(result[1]), &t); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	deadline := float64(time.Now().Add(processingTTL).Unix())
	b.rdb.ZAdd(ctx, ProcessingSet, redis.Z{Score: deadline, Member: result[1]})

	return &t, nil
}

func (b *Broker) Ack(ctx context.Context, t *task.Task) error {
	data, _ := json.Marshal(t)
	return b.rdb.ZRem(ctx, ProcessingSet, string(data)).Err()
}

func (b *Broker) Nack(ctx context.Context, t *task.Task, execErr error) error {
	data, _ := json.Marshal(t)
	b.rdb.ZRem(ctx, ProcessingSet, string(data))

	t.Retries++
	if t.Retries >= t.MaxRetry {
		fmt.Printf("[DLQ] task %s type=%s after %d retries: %v\n", t.ID, t.Type, t.Retries, execErr)
		dead, _ := json.Marshal(t)
		if err := b.rdb.LPush(ctx, DeadLetterQueue, dead).Err(); err != nil {
			return err
		}
		metrics.TasksDeadLettered.WithLabelValues(t.Type).Inc()
		return nil
	}

	delay := time.Duration(t.Retries*t.Retries) * 5 * time.Second
	retryAt := float64(time.Now().Add(delay).Unix())
	updated, _ := json.Marshal(t)
	return b.rdb.ZAdd(ctx, RetryQueue, redis.Z{Score: retryAt, Member: string(updated)}).Err()
}

// FlushRetry moves tasks:retry entries whose retry delay has elapsed back
// onto their priority queue.
func (b *Broker) FlushRetry(ctx context.Context) error {
	return b.flushDueSet(ctx, RetryQueue)
}

// FlushScheduled moves tasks:scheduled entries whose run time has arrived
// onto their priority queue.
func (b *Broker) FlushScheduled(ctx context.Context) error {
	return b.flushDueSet(ctx, ScheduledQueue)
}

// flushDueSet moves every member of the given sorted set scored at or
// before now onto its task's priority queue.
func (b *Broker) flushDueSet(ctx context.Context, set string) error {
	now := fmt.Sprintf("%f", float64(time.Now().Unix()))
	items, err := b.rdb.ZRangeByScore(ctx, set, &redis.ZRangeBy{
		Min: "-inf",
		Max: now,
	}).Result()
	if err != nil {
		return err
	}

	for _, item := range items {
		queue := NormalQueue
		var t task.Task
		if err := json.Unmarshal([]byte(item), &t); err == nil {
			queue = queueFor(t.Priority)
		}

		pipe := b.rdb.Pipeline()
		pipe.ZRem(ctx, set, item)
		pipe.LPush(ctx, queue, item)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SetResult stores a task's outcome in a Redis hash keyed by task ID and
// publishes it on that task's result channel, so producers can either poll
// GetResult or subscribe via WatchResult.
func (b *Broker) SetResult(ctx context.Context, r *task.Result) error {
	fields := map[string]any{
		"task_id":      r.TaskID,
		"status":       string(r.Status),
		"error":        r.Error,
		"completed_at": r.CompletedAt.Format(time.RFC3339Nano),
	}
	if len(r.Output) > 0 {
		fields["output"] = string(r.Output)
	}

	key := resultKey(r.TaskID)
	pipe := b.rdb.Pipeline()
	pipe.HSet(ctx, key, fields)
	pipe.Expire(ctx, key, resultTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store result: %w", err)
	}

	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	return b.rdb.Publish(ctx, resultChannel(r.TaskID), data).Err()
}

// GetResult polls for a task's stored result. It returns ErrResultNotFound
// if the task hasn't completed (or its result already expired).
func (b *Broker) GetResult(ctx context.Context, taskID string) (*task.Result, error) {
	fields, err := b.rdb.HGetAll(ctx, resultKey(taskID)).Result()
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, ErrResultNotFound
	}

	r := &task.Result{
		TaskID: fields["task_id"],
		Status: task.ResultStatus(fields["status"]),
		Error:  fields["error"],
	}
	if output := fields["output"]; output != "" {
		r.Output = json.RawMessage(output)
	}
	if completedAt, err := time.Parse(time.RFC3339Nano, fields["completed_at"]); err == nil {
		r.CompletedAt = completedAt
	}
	return r, nil
}

// WatchResult subscribes to taskID's completion event, delivering the
// Result on the returned channel as soon as SetResult publishes it.
func (b *Broker) WatchResult(ctx context.Context, taskID string) (<-chan *task.Result, func(), error) {
	sub := b.rdb.Subscribe(ctx, resultChannel(taskID))
	if _, err := sub.Receive(ctx); err != nil {
		sub.Close()
		return nil, nil, fmt.Errorf("subscribe: %w", err)
	}

	out := make(chan *task.Result, 1)
	go func() {
		defer close(out)
		for msg := range sub.Channel() {
			var r task.Result
			if err := json.Unmarshal([]byte(msg.Payload), &r); err != nil {
				continue
			}
			out <- &r
		}
	}()

	return out, func() { sub.Close() }, nil
}
