package metrics

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// TasksEnqueued counts tasks accepted by the broker, by type and priority.
	TasksEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gotasks_tasks_enqueued_total",
		Help: "Total number of tasks enqueued, by type and priority.",
	}, []string{"type", "priority"})

	// TasksSucceeded counts handler executions that returned no error.
	TasksSucceeded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gotasks_tasks_succeeded_total",
		Help: "Total number of tasks that completed successfully, by type.",
	}, []string{"type"})

	// TasksFailed counts failed handler attempts, one per attempt — a task
	// retried twice before succeeding contributes two increments here.
	TasksFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gotasks_tasks_failed_total",
		Help: "Total number of failed task attempts, by type.",
	}, []string{"type"})

	// TasksDeadLettered counts tasks that exhausted all retries and were
	// moved to the dead-letter queue.
	TasksDeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gotasks_tasks_dead_lettered_total",
		Help: "Total number of tasks moved to the dead-letter queue, by type.",
	}, []string{"type"})

	// TaskDuration observes handler execution time, by type and outcome.
	TaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gotasks_task_duration_seconds",
		Help:    "Task handler execution duration in seconds, by type and outcome.",
		Buckets: prometheus.DefBuckets,
	}, []string{"type", "outcome"})
)

// RecordExecution reports a single handler attempt's duration and outcome.
// It matches the task.Metrics middleware's record signature, so it can be
// wired straight into registry.Use(task.Metrics(metrics.RecordExecution)).
func RecordExecution(taskType string, dur time.Duration, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
		TasksFailed.WithLabelValues(taskType).Inc()
	} else {
		TasksSucceeded.WithLabelValues(taskType).Inc()
	}
	TaskDuration.WithLabelValues(taskType, outcome).Observe(dur.Seconds())
}

// Serve starts an HTTP server exposing Prometheus metrics at addr+"/metrics"
// and blocks until ctx is cancelled, then shuts the server down gracefully.
func Serve(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("metrics: serving %s/metrics", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics server: %v", err)
	}
}
