package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gotasks/broker"
	"gotasks/examples"
	"gotasks/metrics"
	"gotasks/reaper"
	"gotasks/task"
	"gotasks/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	router := task.NewRouter()
	router.Route("send_email", "webhooks")
	router.Route("send_webhook", "webhooks")
	router.Route("video_transcode", "video")

	b := broker.New(redisAddr, broker.WithRouter(router))
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	registry := task.NewRegistry()
	registry.Use(
		task.Recover(),
		task.Logging(log.Default()),
		task.Metrics(metrics.RecordExecution),
	)
	registry.Register("send_email", examples.SendEmailHandler)
	registry.Register("resize_image", examples.ResizeImageHandler)
	registry.Register("send_webhook", examples.SendWebhookHandler)
	registry.Register("video_transcode", examples.TranscodeVideoHandler)

	ctx, cancel := context.WithCancel(context.Background())

	go metrics.Serve(ctx, ":9090")
	go b.RunFlusher(ctx, 5*time.Second)

	r := reaper.New(rdb, b)
	go r.Run(ctx)

	pools := []*worker.Worker{
		worker.New(b, registry, "default", 5),
		worker.New(b, registry, "webhooks", 20),
		worker.New(b, registry, "video", 2),
	}
	for _, pool := range pools {
		pool.Start(ctx)
	}

	go func() {
		time.Sleep(500 * time.Millisecond)

		emailTask, _ := task.New("send_email", examples.EmailPayload{
			To:      "alice@example.com",
			Subject: "Welcome!",
			Body:    "Thanks for signing up.",
		}, 3, task.WithPriority(task.PriorityLow))
		if err := b.Enqueue(ctx, emailTask); err != nil {
			log.Println("enqueue:", err)
		}

		resizeTask, _ := task.New("resize_image", examples.ResizePayload{
			ImageURL: "https://example.com/photo.jpg",
			Width:    800,
			Height:   600,
		}, 5, task.WithPriority(task.PriorityHigh))
		if err := b.Enqueue(ctx, resizeTask); err != nil {
			log.Println("enqueue:", err)
		}

		webhookTask, _ := task.New("send_webhook", examples.WebhookPayload{
			URL:  "https://example.com/hooks/incoming",
			Body: `{"event":"user.created"}`,
		}, 3)
		if err := b.Enqueue(ctx, webhookTask); err != nil {
			log.Println("enqueue:", err)
		}

		transcodeTask, _ := task.New("video_transcode", examples.VideoTranscodePayload{
			VideoURL: "https://example.com/videos/clip.mov",
			Format:   "mp4",
		}, 2)
		if err := b.Enqueue(ctx, transcodeTask); err != nil {
			log.Println("enqueue:", err)
		}

		log.Println("tasks enqueued")
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Println("shutting down...")
	cancel()
	for _, pool := range pools {
		pool.Stop()
	}
}
