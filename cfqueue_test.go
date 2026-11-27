package cfqueue

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const qpath = "/client/v4/accounts/acct/queues/q1"

// fakeQueues is an in-memory Cloudflare Queues REST API: push, pull (with
// leases), ack. Leased messages are not returned again until acked.
type fakeQueues struct {
	mu       sync.Mutex
	msgs     []map[string]any
	leased   map[string]map[string]any
	n        int
	pulls    int
	lastPull map[string]any
	auth     string
	ctype    string
	method   string
}

func (f *fakeQueues) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth = r.Header.Get("Authorization")
	f.ctype = r.Header.Get("Content-Type")
	f.method = r.Method
	var req map[string]any
	_ = json.NewDecoder(r.Body).Decode(&req)
	ok := func(result any) {
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "errors": []any{}, "result": result})
	}
	switch r.URL.Path {
	case qpath + "/messages":
		f.n++
		f.msgs = append(f.msgs, map[string]any{"id": "m" + strconv.Itoa(f.n), "body": req["body"], "attempts": 1.0, "ct": req["content_type"]})
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

// retry returns every leased message to the queue with one more attempt, as
// Queues does when a lease's visibility timeout expires.
func (f *fakeQueues) retry() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, m := range f.leased {
		m["attempts"] = m["attempts"].(float64) + 1
		f.msgs = append(f.msgs, m)
		delete(f.leased, id)
	}
}

func setup(t *testing.T) (*fakeQueues, *Client, *httptest.Server) {
	f := &fakeQueues{leased: map[string]map[string]any{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := New(srv.URL+qpath+"/", "tok")
	c.PollInterval = 10 * time.Millisecond
	return f, c, srv
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

func TestOpen(t *testing.T) {
	const u = "https://api.cloudflare.com/client/v4/accounts/a/queues/q/"
	t.Setenv("CF_QUEUES_API_TOKEN", "t")
	if _, err := Open("https://sqs.ap-southeast-2.amazonaws.com/0614/x"); err == nil {
		t.Fatal("want error for a non-Cloudflare URL")
	}
	c, err := Open(u)
	if err != nil {
		t.Fatal(err)
	}
	if c.URL != strings.TrimSuffix(u, "/") || c.Token != "t" {
		t.Fatalf("client %q %q", c.URL, c.Token)
	}
	t.Setenv("CF_QUEUES_API_TOKEN", "")
	if _, err := Open(u); err == nil {
		t.Fatal("want error without CF_QUEUES_API_TOKEN")
	}
}

func TestSendReceiveDeleteRoundTrip(t *testing.T) {
	f, c, _ := setup(t)
	ctx := context.Background()
	body := `{"artifactUuid":"a1","orgId":"o1"}`
	if err := c.Send(ctx, body); err != nil {
		t.Fatal(err)
	}
	if f.auth != "Bearer tok" || f.ctype != "application/json" || f.method != http.MethodPost || f.msgs[0]["ct"] != "text" {
		t.Fatalf("auth %q type %q method %s content_type %v", f.auth, f.ctype, f.method, f.msgs[0]["ct"])
	}
	msgs, err := c.Receive(ctx, ReceiveOptions{MaxMessages: 10, Visibility: 900 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %+v", msgs)
	}
	m := msgs[0]
	if m.Body != body || m.ID != "m1" || m.ReceiptHandle != "lease-m1" || m.Attempts != 1 {
		t.Fatalf("message %+v", m)
	}
	if f.lastPull["visibility_timeout_ms"].(float64) != 900000 || f.lastPull["batch_size"].(float64) != 10 {
		t.Fatalf("pull %v", f.lastPull)
	}
	if err := c.Delete(ctx, m.ReceiptHandle); err != nil {
		t.Fatal(err)
	}
	if len(f.leased) != 0 {
		t.Fatalf("lease not acked: %v", f.leased)
	}
}

func TestReceiveDefaultsAndBatchClamp(t *testing.T) {
	f, c, _ := setup(t)
	ctx := context.Background()
	if _, err := c.Receive(ctx, ReceiveOptions{}); err != nil {
		t.Fatal(err)
	}
	if f.lastPull["batch_size"].(float64) != 1 || f.lastPull["visibility_timeout_ms"].(float64) != 30000 || f.pulls != 1 {
		t.Fatalf("pull %v after %d pulls", f.lastPull, f.pulls)
	}
	if _, err := c.Receive(ctx, ReceiveOptions{MaxMessages: 500}); err != nil {
		t.Fatal(err)
	}
	if f.lastPull["batch_size"].(float64) != 100 {
		t.Fatalf("pull %v", f.lastPull)
	}
}

func TestUndeletedMessageIsRedeliveredWithMoreAttempts(t *testing.T) {
	f, c, _ := setup(t)
	ctx := context.Background()
	if err := c.Send(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if msgs, err := c.Receive(ctx, ReceiveOptions{}); err != nil || len(msgs) != 1 {
		t.Fatalf("%+v %v", msgs, err)
	}
	f.retry()
	msgs, err := c.Receive(ctx, ReceiveOptions{})
	if err != nil || len(msgs) != 1 || msgs[0].Attempts != 2 || msgs[0].Body != "x" {
		t.Fatalf("%+v %v", msgs, err)
	}
}

func TestReceiveLongPollsUntilWait(t *testing.T) {
	f, c, _ := setup(t)
	start := time.Now()
	msgs, err := c.Receive(context.Background(), ReceiveOptions{Wait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 || time.Since(start) < 900*time.Millisecond || f.pulls < 3 {
		t.Fatalf("messages %d after %v with %d pulls", len(msgs), time.Since(start), f.pulls)
	}
}

func TestReceiveReturnsAsSoonAsAMessageArrives(t *testing.T) {
	_, c, _ := setup(t)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = c.Send(context.Background(), "late")
	}()
	msgs, err := c.Receive(context.Background(), ReceiveOptions{Wait: 20 * time.Second})
	if err != nil || len(msgs) != 1 || msgs[0].Body != "late" {
		t.Fatalf("got %+v, %v", msgs, err)
	}
}

func TestReceiveStopsOnContextCancel(t *testing.T) {
	_, c, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.Receive(ctx, ReceiveOptions{Wait: 20 * time.Second}); err == nil {
		t.Fatal("want context error")
	}
}

func TestErrors(t *testing.T) {
	_, c, srv := setup(t)
	ctx := context.Background()
	bad := New(srv.URL+"/client/v4/accounts/acct/queues/nope", "tok")
	if err := bad.Send(ctx, "x"); err == nil || !strings.Contains(err.Error(), "10013") || !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("want API error, got %v", err)
	}
	if err := c.Delete(ctx, ""); err == nil {
		t.Fatal("want error for an empty receipt handle")
	}
	// success=false on HTTP 200 is still an error.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":11000,"message":"queue paused"}]}`))
	}))
	defer srv2.Close()
	if err := New(srv2.URL+qpath, "tok").Send(ctx, "x"); err == nil || !strings.Contains(err.Error(), "queue paused") {
		t.Fatalf("want envelope error, got %v", err)
	}
	// A non-JSON response.
	srv3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`<html>bad gateway</html>`))
	}))
	defer srv3.Close()
	if _, err := New(srv3.URL+qpath, "tok").Receive(ctx, ReceiveOptions{}); err == nil || !strings.Contains(err.Error(), "HTTP 502") {
		t.Fatalf("want undecodable error, got %v", err)
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
