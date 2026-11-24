// Package cfqueue lets a Go service that uses Amazon SQS run on Cloudflare
// Queues unchanged. Client has the SendMessage, ReceiveMessage and
// DeleteMessage methods of *sqs.Client, with the same input and output types,
// backed by the Cloudflare Queues REST API (push, pull, ack). Open returns the
// SQS client or this one from the queue URL alone, so a service switches by
// configuration:
//
//	SQS_UPLOAD_QUEUE_URL=https://sqs.ap-southeast-2.amazonaws.com/0614/cyclonedx-uploads   -> SQS
//	SQS_UPLOAD_QUEUE_URL=https://api.cloudflare.com/client/v4/accounts/<acct>/queues/<id> -> Queues
//
// The Queues token (Queues Edit on the account) comes from CF_QUEUES_API_TOKEN.
//
// SQS semantics kept:
//   - MessageBody is sent as text and received back byte-identical.
//   - ReceiptHandle is the Queues lease id; DeleteMessage acks it.
//   - A message that is received but not deleted returns after
//     VisibilityTimeout (the Queues pull visibility timeout).
//   - WaitTimeSeconds long-polls: pull is non-blocking, so Receive pulls again
//     every PollInterval until a message arrives or the wait elapses.
//   - Attributes["ApproximateReceiveCount"] is the Queues attempt count.
//
// Not supported (returns an error): MessageAttributes, FIFO groups and
// deduplication ids. No Vulnesis service uses them.
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
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Queue is the part of *sqs.Client the Vulnesis services call.
type Queue interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

var _ Queue = (*sqs.Client)(nil)
var _ Queue = (*Client)(nil)

const apiPrefix = "https://api.cloudflare.com/client/v4/accounts/"

// IsQueueURL reports whether a queue URL names a Cloudflare queue.
func IsQueueURL(u string) bool {
	rest, ok := strings.CutPrefix(u, apiPrefix)
	if !ok {
		return false
	}
	parts := strings.Split(strings.TrimSuffix(rest, "/"), "/")
	return len(parts) == 3 && parts[0] != "" && parts[1] == "queues" && parts[2] != ""
}

// Open returns a Cloudflare Queues client when queueURL names a Cloudflare
// queue (token from CF_QUEUES_API_TOKEN), and an SQS client from cfg otherwise.
func Open(cfg aws.Config, queueURL string) (Queue, error) {
	if !IsQueueURL(queueURL) {
		return sqs.NewFromConfig(cfg), nil
	}
	tok := os.Getenv("CF_QUEUES_API_TOKEN")
	if tok == "" {
		return nil, errors.New("cfqueue: CF_QUEUES_API_TOKEN is required for a Cloudflare queue URL")
	}
	return New(tok), nil
}

// Client talks to Cloudflare Queues. The zero value is not usable; use New.
type Client struct {
	Token string
	HTTP  *http.Client
	// VisibilityTimeout is used when ReceiveMessageInput.VisibilityTimeout is 0.
	VisibilityTimeout time.Duration
	// PollInterval is the pause between pulls while long-polling.
	PollInterval time.Duration
}

func New(token string) *Client {
	return &Client{Token: token, HTTP: &http.Client{Timeout: 30 * time.Second}, VisibilityTimeout: 30 * time.Second, PollInterval: time.Second}
}

type envelope struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result json.RawMessage `json:"result"`
}

func (c *Client) call(ctx context.Context, url string, req any, result any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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

func queueURL(p *string) (string, error) {
	u := aws.ToString(p)
	if !IsQueueURL(u) {
		return "", fmt.Errorf("cfqueue: %q is not a Cloudflare queue URL", u)
	}
	return strings.TrimSuffix(u, "/"), nil
}

func (c *Client) SendMessage(ctx context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	u, err := queueURL(in.QueueUrl)
	if err != nil {
		return nil, err
	}
	if len(in.MessageAttributes) > 0 || len(in.MessageSystemAttributes) > 0 || in.MessageGroupId != nil || in.MessageDeduplicationId != nil {
		return nil, errors.New("cfqueue: message attributes and FIFO fields are not supported")
	}
	req := map[string]any{"body": aws.ToString(in.MessageBody), "content_type": "text"}
	if in.DelaySeconds > 0 {
		req["delay_seconds"] = in.DelaySeconds
	}
	if err := c.call(ctx, u+"/messages", req, nil); err != nil {
		return nil, err
	}
	return &sqs.SendMessageOutput{}, nil
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
// producers come back as JSON, which is the body text SQS would have held.
func bodyText(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func (c *Client) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	u, err := queueURL(in.QueueUrl)
	if err != nil {
		return nil, err
	}
	batch := int(in.MaxNumberOfMessages)
	if batch < 1 {
		batch = 1
	}
	if batch > 100 {
		batch = 100
	}
	vis := c.VisibilityTimeout
	if in.VisibilityTimeout > 0 {
		vis = time.Duration(in.VisibilityTimeout) * time.Second
	}
	deadline := time.Now().Add(time.Duration(in.WaitTimeSeconds) * time.Second)
	for {
		var res pulled
		if err := c.call(ctx, u+"/messages/pull", map[string]any{"batch_size": batch, "visibility_timeout_ms": vis.Milliseconds()}, &res); err != nil {
			return nil, err
		}
		if len(res.Messages) > 0 || !time.Now().Before(deadline) {
			out := &sqs.ReceiveMessageOutput{}
			for _, m := range res.Messages {
				out.Messages = append(out.Messages, types.Message{
					MessageId:     aws.String(m.ID),
					ReceiptHandle: aws.String(m.LeaseID),
					Body:          aws.String(bodyText(m.Body)),
					Attributes:    map[string]string{"ApproximateReceiveCount": strconv.Itoa(int(m.Attempts))},
				})
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

func (c *Client) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	u, err := queueURL(in.QueueUrl)
	if err != nil {
		return nil, err
	}
	lease := aws.ToString(in.ReceiptHandle)
	if lease == "" {
		return nil, errors.New("cfqueue: empty receipt handle")
	}
	if err := c.call(ctx, u+"/messages/ack", map[string]any{"acks": []map[string]string{{"lease_id": lease}}}, nil); err != nil {
		return nil, err
	}
	return &sqs.DeleteMessageOutput{}, nil
}
