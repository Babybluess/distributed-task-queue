# gotasks

A distributed task queue in Go backed by Redis — inspired by Celery and Sidekiq.

## Setup

```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:7

# Install dependencies
go mod tidy

# Run
go run main.go
```

## Inspect queues

```bash
redis-cli LLEN tasks:high
redis-cli LLEN tasks:normal
redis-cli LLEN tasks:low
redis-cli ZCARD tasks:processing
redis-cli ZCARD tasks:retry
redis-cli LLEN tasks:dead
redis-cli LRANGE tasks:dead 0 -1
```

## Layout

```
gotasks/
├── main.go                  entrypoint, wires everything together
├── task/task.go             Task struct, HandlerFunc, Registry
├── broker/broker.go         Redis enqueue/dequeue/ack/nack/retry
├── worker/worker.go         Goroutine pool, retry flusher
├── reaper/reaper.go         Reschedules stuck tasks
└── examples/handlers.go     send_email, resize_image handlers
```

## Queue keys in Redis

| Key                | Type        | Purpose                               |
|--------------------|-------------|----------------------------------------|
| tasks:high         | List        | High priority, waiting to be picked up |
| tasks:normal       | List        | Normal priority (default)             |
| tasks:low          | List        | Low priority                          |
| tasks:processing   | Sorted set  | In-flight (score = deadline)          |
| tasks:retry        | Sorted set  | Delayed retry (score = retry_at)      |
| tasks:dead         | List        | Exhausted all retries                 |

Workers dequeue by checking `tasks:high` first with a short `BRPOP` timeout
(100ms), falling through to `tasks:normal` and then blocking on `tasks:low`
for the remainder of the poll interval. This mirrors Sidekiq Pro's strict
priority strategy: a busy high queue never starves lower ones, and an idle
high queue never blocks lower-priority work. Set priority via
`task.New(..., task.WithPriority(task.PriorityHigh))`; it defaults to
`task.PriorityNormal`.

## Next steps

- Scheduled tasks with tasks:scheduled sorted set
- Result storage in Redis hash keyed by task ID
- Prometheus metrics (enqueued/succeeded/failed counters + duration histogram)
- Middleware chain on handlers (logging, panic recovery)
- Multi-queue routing per task type
