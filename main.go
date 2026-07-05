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

	b := broker.New(redisAddr)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	registry := task.NewRegistry()
	registry.Register("send_email", examples.SendEmailHandler)
	registry.Register("resize_image", examples.ResizeImageHandler)

	ctx, cancel := context.WithCancel(context.Background())

	r := reaper.New(rdb, b)
	go r.Run(ctx)

	pool := worker.New(b, registry, 5)
	pool.Start(ctx)

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

		log.Println("tasks enqueued")
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Println("shutting down...")
	cancel()
	pool.Stop()
}
