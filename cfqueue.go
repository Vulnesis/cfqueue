// Package cfqueue is a small client for one Cloudflare queue over the
// Cloudflare Queues REST API (push, pull, ack), for Go services that run
// outside a Worker. Open takes the queue's API URL:
//
//	https://api.cloudflare.com/client/v4/accounts/<acct>/queues/<queue-id>
//
// The Queues token (Queues Edit on the account) comes from CF_QUEUES_API_TOKEN.
//
// Semantics:
//   - Send sends the body as text; Receive returns it byte-identical.
//   - Message.ReceiptHandle is the Queues lease id; Delete acks it.
//   - A message that is received but not deleted returns after the
//     visibility timeout (the Queues pull visibility timeout).
//   - ReceiveOptions.Wait long-polls: pull is non-blocking, so Receive pulls
//     again every PollInterval until a message arrives or the wait elapses.
//   - Message.Attempts is the Queues delivery attempt count (1 on the first
//     delivery).
package cfqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Message is one message received from a queue.
type Message struct {
	ID string
	// Body is the message body as sent.
	Body string
	// ReceiptHandle identifies this delivery; pass it to Delete.
	ReceiptHandle string
	// Attempts is how many times the queue has delivered the message,
	// counting this delivery.
	Attempts int
}

// ReceiveOptions controls one Receive call. Zero values take the defaults.
type ReceiveOptions struct {
	// MaxMessages is the batch size, 1 to 100 (default 1).
	MaxMessages int
	// Wait is how long to long-poll for a message (default: one pull).
	Wait time.Duration
	// Visibility is how long a received message stays hidden before it is
	// delivered again (default Client.VisibilityTimeout).
	Visibility time.Duration
}

// Queue is the interface the Vulnesis services use, so tests can fake it.
type Queue interface {
	Send(ctx context.Context, body string) error
	Receive(ctx context.Context, opts ReceiveOptions) ([]Message, error)
	Delete(ctx context.Context, receiptHandle string) error
}

var _ Queue = (*Client)(nil)

const apiPrefix = "https://api.cloudflare.com/client/v4/accounts/"

// IsQueueURL reports whether a URL names a Cloudflare queue.
func IsQueueURL(u string) bool {
	rest, ok := strings.CutPrefix(u, apiPrefix)
	if !ok {
		return false
	}
	parts := strings.Split(strings.TrimSuffix(rest, "/"), "/")
	return len(parts) == 3 && parts[0] != "" && parts[1] == "queues" && parts[2] != ""
}

// Open returns a client for the Cloudflare queue at queueURL, with the token
// from CF_QUEUES_API_TOKEN.
func Open(queueURL string) (*Client, error) {
	if !IsQueueURL(queueURL) {
		return nil, fmt.Errorf("cfqueue: %q is not a Cloudflare queue URL", queueURL)
	}
	tok := os.Getenv("CF_QUEUES_API_TOKEN")
	if tok == "" {
		return nil, errors.New("cfqueue: CF_QUEUES_API_TOKEN is required")
	}
	return New(queueURL, tok), nil
}

// Client talks to one Cloudflare queue. Use New or Open.
type Client struct {
	// URL is the queue's API URL, without a trailing slash.
	URL   string
	Token string
	HTTP  *http.Client
	// VisibilityTimeout is used when ReceiveOptions.Visibility is 0.
	VisibilityTimeout time.Duration
	// PollInterval is the pause between pulls while long-polling.
	PollInterval time.Duration
}

// New returns a client for queueURL. It does not check the URL; Open does.
func New(queueURL, token string) *Client {
	return &Client{
		URL:               strings.TrimSuffix(queueURL, "/"),
		Token:             token,
		HTTP:              &http.Client{Timeout: 30 * time.Second},
		VisibilityTimeout: 30 * time.Second,
		PollInterval:      time.Second,
	}
}

type envelope struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result json.RawMessage `json:"result"`
}

func (c *Client) call(ctx context.Context, path string, req any, result any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Authorization", "Bearer "+c.Token)
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(r)
	if err != nil {
		return fmt.Errorf("cfqueue: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("cfqueue: HTTP %d: undecodable response", resp.StatusCode)
	}
	if resp.StatusCode/100 != 2 || !env.Success {
		msgs := make([]string, 0, len(env.Errors))
		for _, e := range env.Errors {
			msgs = append(msgs, fmt.Sprintf("%d %s", e.Code, e.Message))
		}
		return fmt.Errorf("cfqueue: HTTP %d: %s", resp.StatusCode, strings.Join(msgs, "; "))
	}
	if result != nil && len(env.Result) > 0 {
		return json.Unmarshal(env.Result, result)
	}
	return nil
}

// Send pushes one text message.
func (c *Client) Send(ctx context.Context, body string) error {
	return c.call(ctx, "/messages", map[string]any{"body": body, "content_type": "text"}, nil)
}

type pulled struct {
	Messages []struct {
		ID       string          `json:"id"`
		Body     json.RawMessage `json:"body"`
		Attempts float64         `json:"attempts"`
		LeaseID  string          `json:"lease_id"`
	} `json:"messages"`
}

// bodyText returns a text message's body as sent. Bodies sent as JSON by other
// producers come back as their JSON text.
func bodyText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

// Receive pulls up to opts.MaxMessages messages, long-polling for opts.Wait.
// It returns no messages and no error when the wait elapses on an empty queue.
func (c *Client) Receive(ctx context.Context, opts ReceiveOptions) ([]Message, error) {
	batch := min(max(opts.MaxMessages, 1), 100)
	vis := c.VisibilityTimeout
	if opts.Visibility > 0 {
		vis = opts.Visibility
	}
	deadline := time.Now().Add(opts.Wait)
	for {
		var res pulled
		if err := c.call(ctx, "/messages/pull", map[string]any{"batch_size": batch, "visibility_timeout_ms": vis.Milliseconds()}, &res); err != nil {
			return nil, err
		}
		if len(res.Messages) > 0 || !time.Now().Before(deadline) {
			out := make([]Message, 0, len(res.Messages))
			for _, m := range res.Messages {
				out = append(out, Message{ID: m.ID, Body: bodyText(m.Body), ReceiptHandle: m.LeaseID, Attempts: int(m.Attempts)})
			}
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.PollInterval):
		}
	}
}

// Delete acks a received message by its receipt handle.
func (c *Client) Delete(ctx context.Context, receiptHandle string) error {
	if receiptHandle == "" {
		return errors.New("cfqueue: empty receipt handle")
	}
	return c.call(ctx, "/messages/ack", map[string]any{"acks": []map[string]string{{"lease_id": receiptHandle}}}, nil)
}
