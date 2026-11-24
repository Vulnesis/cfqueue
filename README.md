# cfqueue

`cfqueue` lets a Go service that uses Amazon SQS run on Cloudflare Queues unchanged. `Client` has the `SendMessage`, `ReceiveMessage` and `DeleteMessage` methods of `*sqs.Client`, with the same input and output types, backed by the Cloudflare Queues REST API (push, pull, ack). `Open` returns the SQS client or this one from the queue URL alone, so a service switches by configuration. See the package documentation in `cfqueue.go` for the semantics kept.

```go
import "github.com/vulnesis/cfqueue"
```

The Queues token (Queues Edit on the account) comes from `CF_QUEUES_API_TOKEN`.

Moved from `github.com/vulnesis/ops/edge/cfqueue` on 2026-11-24 so that public and private builds can fetch it without a token; v0.1.0 is the same code as that module's `edge/cfqueue/v0.1.0`.
