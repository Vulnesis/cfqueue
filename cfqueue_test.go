package cfqueue

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const qpath = "/client/v4/accounts/acct/queues/q1"

// fakeQueues is an in-memory Cloudflare Queues REST API: push, pull (with
// leases), ack. Leased messages are not returned again until acked or retried.
type fakeQueues struct {
	mu       sync.Mutex
	msgs     []map[string]any
	leased   map[string]map[string]any
	n        int
	pulls    int
	lastPull map[string]any
	auth     string
}

func (f *fakeQueues) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth = r.Header.Get("Authorization")
	var req map[string]any
	_ = json.NewDecoder(r.Body).Decode(&req)
	ok := func(result any) {
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "errors": []any{}, "result": result})
	}
	switch r.URL.Path {
	case qpath + "/messages":
		f.n++
		f.msgs = append(f.msgs, map[string]any{"id": "m" + string(rune('0'+f.n)), "body": req["body"], "attempts": 1.0, "ct": req["content_type"]})
		ok(nil)
	case qpath + "/messages/pull":
		f.pulls++
		f.lastPull = req
		batch := int(req["batch_size"].(float64))
		var out []map[string]any
		for len(f.msgs) > 0 && len(out) < batch {
			m := f.msgs[0]
			f.msgs = f.msgs[1:]
			m["lease_id"] = "lease-" + m["id"].(string)
			f.leased[m["lease_id"].(string)] = m
			out = append(out, m)
		}
		ok(map[string]any{"messages": out})
	case qpath + "/messages/ack":
		for _, a := range req["acks"].([]any) {
			delete(f.leased, a.(map[string]any)["lease_id"].(string))
		}
		ok(nil)
	default:
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "errors": []any{map[string]any{"code": 10013, "message": "not found"}}})
	}
}

func setup(t *testing.T) (*fakeQueues, *Client, string) {
	f := &fakeQueues{leased: map[string]map[string]any{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := New("tok")
	c.PollInterval = 10 * time.Millisecond
	// Route the real API prefix to the fake server.
	c.HTTP = &http.Client{Transport: rewrite{srv.URL}}
	return f, c, "https://api.cloudflare.com" + qpath
}

type rewrite struct{ base string }

func (rw rewrite) RoundTrip(r *http.Request) (*http.Response, error) {
	u := rw.base + r.URL.Path
	r2, _ := http.NewRequestWithContext(r.Context(), r.Method, u, r.Body)
	r2.Header = r.Header
	return http.DefaultTransport.RoundTrip(r2)
}

func TestIsQueueURL(t *testing.T) {
	for u, want := range map[string]bool{
		"https://api.cloudflare.com/client/v4/accounts/a/queues/q":  true,
		"https://api.cloudflare.com/client/v4/accounts/a/queues/q/": true,
		"https://api.cloudflare.com/client/v4/accounts/a/queues/":   false,
		"https://api.cloudflare.com/client/v4/accounts/a/workers/q": false,
		"https://sqs.ap-southeast-2.amazonaws.com/0614/cyclonedx":   false,
		"": false,
	} {
		if IsQueueURL(u) != want {
			t.Errorf("IsQueueURL(%q) = %v", u, !want)
		}
	}
}

func TestOpenPicksBackendByURL(t *testing.T) {
	q, err := Open(aws.Config{Region: "ap-southeast-2"}, "https://sqs.ap-southeast-2.amazonaws.com/0614/x")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.(*sqs.Client); !ok {
		t.Fatalf("SQS URL gave %T", q)
	}
	t.Setenv("CF_QUEUES_API_TOKEN", "")
	if _, err := Open(aws.Config{}, "https://api.cloudflare.com/client/v4/accounts/a/queues/q"); err == nil {
		t.Fatal("want error without CF_QUEUES_API_TOKEN")
	}
	t.Setenv("CF_QUEUES_API_TOKEN", "t")
	q, err = Open(aws.Config{}, "https://api.cloudflare.com/client/v4/accounts/a/queues/q")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.(*Client); !ok {
		t.Fatalf("Cloudflare URL gave %T", q)
	}
}

func TestSendReceiveDeleteRoundTrip(t *testing.T) {
	f, c, url := setup(t)
	ctx := context.Background()
	body := `{"artifactUuid":"a1","orgId":"o1"}`
	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String(body)}); err != nil {
		t.Fatal(err)
	}
	if f.auth != "Bearer tok" || f.msgs[0]["ct"] != "text" {
		t.Fatalf("auth %q content_type %v", f.auth, f.msgs[0]["ct"])
	}
	out, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: 10, VisibilityTimeout: 900})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != body {
		t.Fatalf("got %+v", out.Messages)
	}
	if f.lastPull["visibility_timeout_ms"].(float64) != 900000 || f.lastPull["batch_size"].(float64) != 10 {
		t.Fatalf("pull %v", f.lastPull)
	}
	m := out.Messages[0]
	if m.Attributes["ApproximateReceiveCount"] != "1" {
		t.Fatalf("attributes %v", m.Attributes)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	if len(f.leased) != 0 {
		t.Fatalf("lease not acked: %v", f.leased)
	}
}

func TestReceiveLongPollsUntilWait(t *testing.T) {
	f, c, url := setup(t)
	start := time.Now()
	out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), WaitTimeSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Messages) != 0 || time.Since(start) < 900*time.Millisecond || f.pulls < 3 {
		t.Fatalf("messages %d after %v with %d pulls", len(out.Messages), time.Since(start), f.pulls)
	}
}

func TestReceiveReturnsAsSoonAsAMessageArrives(t *testing.T) {
	_, c, url := setup(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = c.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String("late")})
	}()
	out, err := c.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), WaitTimeSeconds: 20})
	if err != nil || len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != "late" {
		t.Fatalf("got %+v, %v", out, err)
	}
}

func TestUnsupportedAndErrors(t *testing.T) {
	_, c, url := setup(t)
	ctx := context.Background()
	_, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String("x"),
		MessageAttributes: map[string]types.MessageAttributeValue{"k": {DataType: aws.String("String"), StringValue: aws.String("v")}}})
	if err == nil {
		t.Fatal("want error for message attributes")
	}
	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String("https://sqs.x/q"), MessageBody: aws.String("x")}); err == nil {
		t.Fatal("want error for an SQS URL")
	}
	bad := strings.Replace(url, "/q1", "/nope", 1)
	if _, err := c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(bad), MessageBody: aws.String("x")}); err == nil || !strings.Contains(err.Error(), "10013") {
		t.Fatalf("want API error, got %v", err)
	}
	if _, err := c.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url)}); err == nil {
		t.Fatal("want error for an empty receipt handle")
	}
}

func TestBodyText(t *testing.T) {
	if bodyText(json.RawMessage(`"a \"b\""`)) != `a "b"` {
		t.Fatal("text body")
	}
	if bodyText(json.RawMessage(`{"k":1}`)) != `{"k":1}` {
		t.Fatal("json body")
	}
}
