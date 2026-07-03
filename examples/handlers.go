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

func SendEmailHandler(ctx context.Context, t *task.Task) error {
	var p EmailPayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[email] to=%s subject=%q\n", p.To, p.Subject)
	time.Sleep(100 * time.Millisecond)
	return nil
}

type ResizePayload struct {
	ImageURL string `json:"image_url"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
}

func ResizeImageHandler(ctx context.Context, t *task.Task) error {
	var p ResizePayload
	if err := json.Unmarshal(t.Payload, &p); err != nil {
		return fmt.Errorf("bad payload: %w", err)
	}
	fmt.Printf("[resize] url=%s size=%dx%d\n", p.ImageURL, p.Width, p.Height)
	time.Sleep(500 * time.Millisecond)
	return nil
}
