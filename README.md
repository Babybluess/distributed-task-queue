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
redis-cli LLEN tasks:default:high    # substitute the queue name, e.g. tasks:webhooks:high
redis-cli LLEN tasks:default:normal
redis-cli LLEN tasks:default:low
redis-cli ZCARD tasks:processing
redis-cli ZCARD tasks:retry
redis-cli ZCARD tasks:scheduled
redis-cli LLEN tasks:dead
redis-cli LRANGE tasks:dead 0 -1
redis-cli HGETALL task:result:<task_id>
```

## Layout

```
gotasks/
├── main.go                  entrypoint, wires everything together
├── task/task.go             Task struct, HandlerFunc, Registry
├── task/router.go           Router: task type -> named queue
├── task/result.go           Result struct (status/output/error per task)
├── task/middleware.go       Middleware type, Chain, Logging/Recover/Metrics
├── broker/broker.go         Redis enqueue/dequeue/ack/nack/retry/results
├── worker/worker.go         Goroutine pool bound to one named queue
├── reaper/reaper.go         Reschedules stuck tasks
├── metrics/metrics.go       Prometheus counters/histogram, served on :9090/metrics
└── examples/handlers.go     send_email, resize_image, send_webhook, video_transcode handlers
```

## Queue keys in Redis

| Key                          | Type        | Purpose                                |
|-------------------------------|-------------|----------------------------------------|
| tasks:`<queue>`:high          | List        | High priority, waiting to be picked up |
| tasks:`<queue>`:normal        | List        | Normal priority (default)              |
| tasks:`<queue>`:low           | List        | Low priority                           |
| tasks:processing              | Sorted set  | In-flight (score = deadline)           |
| tasks:retry                   | Sorted set  | Delayed retry (score = retry_at)       |
| tasks:scheduled               | Sorted set  | Delayed first run (score = run_at)     |
| tasks:dead                    | List        | Exhausted all retries                  |
| task:result:`<id>`            | Hash        | Status/output/error for a completed task (TTL 24h) |
| task:result:`<id>`:events     | Pub/Sub     | Published on completion, for `WatchResult` |

Within a named queue, workers dequeue by checking its `high` list first with
a short `BRPOP` timeout (100ms), falling through to `normal` and then
blocking on `low` for the remainder of the poll interval. This mirrors
Sidekiq Pro's strict priority strategy: a busy high list never starves lower
ones, and an idle high list never blocks lower-priority work. Set priority
via `task.New(..., task.WithPriority(task.PriorityHigh))`; it defaults to
`task.PriorityNormal`.

## Multi-queue routing

Each task *type* is routed to a named queue via a `task.Router`, so
producers never have to hardcode queue names — they just create a task by
type, and routing decides where it lands. Route high-volume, fast task
types onto a shared queue, and give slow, CPU-heavy ones their own so they
can't block (or get blocked by) the others:

```go
router := task.NewRouter()
router.Route("send_webhook", "webhooks")
router.Route("send_email", "webhooks")
router.Route("video_transcode", "video")   // isolated: slow, CPU-heavy

b := broker.New(redisAddr, broker.WithRouter(router))
```

Task types with no route fall back to `task.DefaultQueue` ("default").

A `worker.Worker` pool is bound to exactly one named queue, so give each
queue whatever concurrency matches its workload:

```go
webhooks := worker.New(b, registry, "webhooks", 20) // high volume, cheap
video    := worker.New(b, registry, "video", 2)      // slow, isolated pool
webhooks.Start(ctx)
video.Start(ctx)
```

Retries and scheduled tasks aren't queue-specific to flush — `Broker.RunFlusher`
runs once regardless of how many pools exist, and routes each due task back
onto whichever queue it was originally routed to (stored on `Task.Queue`).

## Scheduled tasks

Delay a task's first execution with `task.WithScheduledAt`:

```go
t, _ := task.New("send_email", payload, 3, task.WithScheduledAt(time.Now().Add(1*time.Hour)))
broker.Enqueue(ctx, t)
```

Scheduled tasks land in the `tasks:scheduled` sorted set (scored by run time)
instead of a priority queue. Every 5 seconds the worker pool's flusher moves
any task whose time has arrived onto its normal priority queue, the same
mechanism used for delayed retries.

## Result storage

Every completed task's outcome is stored in Redis so producers can look it
up later — the AsyncResult pattern from Celery:

```go
// Poll for it once you expect it's done.
res, err := b.GetResult(ctx, t.ID) // broker.ErrResultNotFound if still pending

// Or subscribe and block until it arrives.
ch, cancel, err := b.WatchResult(ctx, t.ID)
defer cancel()
res := <-ch
```

`res.Status` is `task.StatusSuccess` or `task.StatusFailure`, `res.Output`
is the handler's JSON-marshaled return value, and `res.Error` is populated
on failure. Results expire after 24h.

## Middleware

Wrap handlers with cross-cutting behavior the same way HTTP middleware
works, without touching handler code:

```go
type Middleware func(task.HandlerFunc) task.HandlerFunc

registry.Use(
    task.Recover(),           // convert handler panics into errors
    task.Logging(log.Default()),
    task.Metrics(func(taskType string, dur time.Duration, err error) {
        // report to your metrics system
    }),
)
```

`Registry.Use` appends to a chain applied to every handler on `Get`; the
first middleware passed is outermost. Write your own by matching the
`Middleware` signature — it composes via `task.Chain`.

## Metrics

Prometheus metrics are served on `:9090/metrics`, so a Grafana dashboard
comes for free — point a scrape config at that endpoint:

```yaml
scrape_configs:
  - job_name: gotasks
    static_configs:
      - targets: ["localhost:9090"]
```

| Metric                              | Type      | Labels           | Recorded when                                  |
|--------------------------------------|-----------|------------------|-------------------------------------------------|
| `gotasks_tasks_enqueued_total`       | Counter   | `type`, `priority` | `Broker.Enqueue` succeeds                     |
| `gotasks_tasks_succeeded_total`      | Counter   | `type`           | A handler attempt returns no error              |
| `gotasks_tasks_failed_total`         | Counter   | `type`           | A handler attempt returns an error (per retry)  |
| `gotasks_tasks_dead_lettered_total`  | Counter   | `type`           | `Broker.Nack` exhausts retries and dead-letters |
| `gotasks_task_duration_seconds`      | Histogram | `type`, `outcome`  | Every handler attempt, success or failure     |

Wired in via the existing middleware hook — no handler code changes needed:

```go
registry.Use(
    task.Recover(),
    task.Logging(log.Default()),
    task.Metrics(metrics.RecordExecution),
)

go metrics.Serve(ctx, ":9090")
```

## Next steps

- Recurring (cron-style) schedules, not just one-shot delayed execution
