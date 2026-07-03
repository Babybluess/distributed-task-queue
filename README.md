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
redis-cli LLEN tasks:pending
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

| Key                | Type        | Purpose                          |
|--------------------|-------------|----------------------------------|
| tasks:pending      | List        | Waiting to be picked up          |
| tasks:processing   | Sorted set  | In-flight (score = deadline)     |
| tasks:retry        | Sorted set  | Delayed retry (score = retry_at) |
| tasks:dead         | List        | Exhausted all retries            |

## Next steps

- Priority queues (tasks:high, tasks:normal, tasks:low)
- Scheduled tasks with tasks:scheduled sorted set
- Result storage in Redis hash keyed by task ID
- Prometheus metrics (enqueued/succeeded/failed counters + duration histogram)
- Middleware chain on handlers (logging, panic recovery)
- Multi-queue routing per task type
