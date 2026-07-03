package reaper

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"gotasks/broker"
	"gotasks/task"
)

type Reaper struct {
	rdb    *redis.Client
	broker *broker.Broker
}

func New(rdb *redis.Client, b *broker.Broker) *Reaper {
	return &Reaper{rdb: rdb, broker: b}
}

func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.sweep(ctx); err != nil {
				log.Printf("reaper sweep: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reaper) sweep(ctx context.Context) error {
	now := fmt.Sprintf("%f", float64(time.Now().Unix()))
	items, err := r.rdb.ZRangeByScore(ctx, broker.ProcessingSet, &redis.ZRangeBy{
		Min: "-inf",
		Max: now,
	}).Result()
	if err != nil {
		return err
	}

	for _, item := range items {
		var t task.Task
		if err := json.Unmarshal([]byte(item), &t); err != nil {
			continue
		}

		log.Printf("reaper: rescheduling stuck task %s (type=%s)", t.ID, t.Type)

		pipe := r.rdb.Pipeline()
		pipe.ZRem(ctx, broker.ProcessingSet, item)
		if _, err := pipe.Exec(ctx); err != nil {
			continue
		}
		r.broker.Nack(ctx, &t, fmt.Errorf("worker heartbeat timeout"))
	}

	if len(items) > 0 {
		log.Printf("reaper: recovered %d stuck tasks", len(items))
	}
	return nil
}
