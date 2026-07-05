package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gotasks/task"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func SendEmailHandler(ctx context.Context, t *task.Task) (any, error) {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[email] to=%s subject=%q\n", p.To, p.Subject)
	time.Sleep(100 * time.Millisecond)
	return nil, nil
}

type ResizePayload struct {
	ImageURL string `json:"image_url"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
}

type ResizeResult struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

func ResizeImageHandler(ctx context.Context, t *task.Task) (any, error) {
	var p ResizePayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[resize] url=%s size=%dx%d\n", p.ImageURL, p.Width, p.Height)
	time.Sleep(500 * time.Millisecond)
	return ResizeResult{Width: p.Width, Height: p.Height}, nil
}

type WebhookPayload struct {
	URL  string `json:"url"`
	Body string `json:"body"`
}

// SendWebhookHandler is a fast, high-volume task type meant to share a
// queue with others like it.
func SendWebhookHandler(ctx context.Context, t *task.Task) (any, error) {
	var p WebhookPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[webhook] POST %s\n", p.URL)
	time.Sleep(20 * time.Millisecond)
	return nil, nil
}

type VideoTranscodePayload struct {
	VideoURL string `json:"video_url"`
	Format   string `json:"format"`
}

// TranscodeVideoHandler is slow and CPU-intensive, so it's routed to its
// own isolated queue rather than sharing one with fast task types.
func TranscodeVideoHandler(ctx context.Context, t *task.Task) (any, error) {
	var p VideoTranscodePayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[transcode] url=%s format=%s\n", p.VideoURL, p.Format)
	time.Sleep(3 * time.Second)
	return nil, nil
}
