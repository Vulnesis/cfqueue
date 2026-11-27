# cfqueue

`cfqueue` is a small Go client for one Cloudflare queue over the Cloudflare Queues REST API (push, pull, ack), for services that run outside a Worker. It has no dependencies beyond the standard library.

```go
import "github.com/vulnesis/cfqueue"

q, err := cfqueue.Open("https://api.cloudflare.com/client/v4/accounts/<acct>/queues/<queue-id>")
err = q.Send(ctx, body)
msgs, err := q.Receive(ctx, cfqueue.ReceiveOptions{MaxMessages: 1, Wait: 20 * time.Second, Visibility: 5 * time.Minute})
err = q.Delete(ctx, msgs[0].ReceiptHandle)
```

The Queues token (Queues Edit on the account) comes from `CF_QUEUES_API_TOKEN`. See the package documentation in `cfqueue.go` for the semantics.

v0.2.0 dropped the Amazon SQS fallback and the `aws-sdk-go-v2` types: `Open` takes only a Cloudflare queue URL, and `Send`, `Receive` and `Delete` use cfqueue's own `Message` and `ReceiveOptions`.

Moved from `github.com/vulnesis/ops/edge/cfqueue` on 2026-11-24 so that public and private builds can fetch it without a token; v0.1.0 is the same code as that module's `edge/cfqueue/v0.1.0`.
