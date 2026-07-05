package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gotasks/task"
)

const (
	HighQueue       = "tasks:high"
	NormalQueue     = "tasks:normal"
	LowQueue        = "tasks:low"
	ProcessingSet   = "tasks:processing"
	RetryQueue      = "tasks:retry"
	DeadLetterQueue = "tasks:dead"
	processingTTL   = 5 * time.Minute

	// highPriorityPollTimeout bounds how long Dequeue blocks on a higher
	// priority list before falling through to check the next one.
	highPriorityPollTimeout = 100 * time.Millisecond
)

// priorityQueues lists the pending lists in priority order, highest first.
var priorityQueues = []string{HighQueue, NormalQueue, LowQueue}

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
	return b.rdb.LPush(ctx, queueFor(t.Priority), data).Err()
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
		return b.rdb.LPush(ctx, DeadLetterQueue, dead).Err()
	}

	delay := time.Duration(t.Retries*t.Retries) * 5 * time.Second
	retryAt := float64(time.Now().Add(delay).Unix())
	updated, _ := json.Marshal(t)
	return b.rdb.ZAdd(ctx, RetryQueue, redis.Z{Score: retryAt, Member: string(updated)}).Err()
}

func (b *Broker) FlushRetry(ctx context.Context) error {
	now := fmt.Sprintf("%f", float64(time.Now().Unix()))
	items, err := b.rdb.ZRangeByScore(ctx, RetryQueue, &redis.ZRangeBy{
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
		pipe.ZRem(ctx, RetryQueue, item)
		pipe.LPush(ctx, queue, item)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}
