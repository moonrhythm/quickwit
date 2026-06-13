package quickwit

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	IngestBufferSize       = 10000
	IngestBatchSize        = 1000
	IngestMaxDelay         = time.Second
	IngestConcurrent       = 2
	IngestTimeout          = 15 * time.Second
	ReduceBatchSizeToRatio = 0.9 // reduce 10% of the batch size
	ReduceBatchSizeMin     = 0.1 // do not reduce below 10% of the default batch size
	ResetBatchSizeAfter    = 10 * time.Minute

	// ingestResponseMaxBytes caps how much of the ingest response body is read
	// before parsing, so a pathological body cannot exhaust memory. The default
	// response is a few counts; only detailed_response with many parse failures
	// approaches this.
	ingestResponseMaxBytes = 1 << 20 // 1 MiB
)

type OnDiscardFunc func(any)

// OnRejectFunc is called with the number of documents the server reported as
// parse-rejected in a single ingest response (a 200 with num_rejected_docs > 0).
// It is the only signal for a PARTIAL rejection, where the client cannot tell
// which documents in a coalesced batch failed and Acks the batch anyway. When a
// whole batch is rejected, tracked items also settle with ReasonRejected.
type OnRejectFunc func(numRejected int)

// ingestResponse is the subset of the Quickwit ingest response the client acts
// on. Pointer fields distinguish "absent" (older servers) from zero. See
// https://quickwit.io/docs — a 200 only means the docs were queued.
type ingestResponse struct {
	NumIngestedDocs *int64               `json:"num_ingested_docs"`
	NumRejectedDocs *int64               `json:"num_rejected_docs"`
	ParseFailures   []ingestParseFailure `json:"parse_failures"` // only with detailed_response
}

type ingestParseFailure struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

// DiscardReason explains why a tracked ingest item (one submitted via IngestSync
// or IngestBatch) was dropped before the server durably accepted it. It is
// carried by *IngestError so a caller — e.g. a pub/sub handler — can decide
// between Ack-and-dead-letter and Nack-and-redeliver.
type DiscardReason int

const (
	// ReasonEncode: the value could not be JSON-encoded. Retrying never helps;
	// the item is poison. Dead-letter and Ack.
	ReasonEncode DiscardReason = iota
	// ReasonBufferFull: discard mode is on (SetDiscard) and the buffer was full.
	// Transient; Nack to redeliver.
	ReasonBufferFull
	// ReasonClosed: the client was closed before the item could be accepted.
	// Transient; Nack so another instance handles it.
	ReasonClosed
	// ReasonServer: the server permanently rejected the batch with a 4xx that
	// retrying cannot fix (e.g. 400/422 bad document, 401/403 auth, 404 wrong
	// index, 409 conflict). Inspect the cause: a bad document should be
	// dead-lettered and Acked; a misconfiguration must be fixed (Nacking would
	// redeliver forever). 5xx and 413 are not this — they stay retryable.
	ReasonServer
	// ReasonRejected: the server returned 200 but its response reported that the
	// document was parse-rejected (bad JSON or schema) and not indexed. Retrying
	// never helps; dead-letter and Ack. Only reported when the whole flushed
	// batch was rejected, so attribution is exact (see OnReject for the partial
	// case).
	ReasonRejected
)

func (r DiscardReason) String() string {
	switch r {
	case ReasonEncode:
		return "encode"
	case ReasonBufferFull:
		return "buffer_full"
	case ReasonClosed:
		return "closed"
	case ReasonServer:
		return "server"
	case ReasonRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// permanentIngestStatus reports whether an HTTP status from the ingest endpoint
// is a permanent rejection that retrying the same batch cannot fix. 5xx, 408,
// 425, 429 and 413 are deliberately excluded — they stay retryable (413 has its
// own auto-reduce path).
func permanentIngestStatus(code int) bool {
	switch code {
	case http.StatusBadRequest, // 400
		http.StatusUnauthorized,        // 401
		http.StatusForbidden,           // 403
		http.StatusNotFound,            // 404
		http.StatusConflict,            // 409
		http.StatusUnprocessableEntity: // 422
		return true
	default:
		return false
	}
}

// IngestError is returned by IngestSync and IngestReceipt.Wait when an item was
// dropped by the client before the server durably accepted it. A wrapped
// context error (not an *IngestError) instead means "not confirmed": the item
// may still be ingested. Use errors.As to inspect Reason.
type IngestError struct {
	Reason DiscardReason
	Err    error // underlying cause, if any (e.g. the json encode error)
}

func (e *IngestError) Error() string {
	if e.Err != nil {
		return "quickwit: ingest discarded (" + e.Reason.String() + "): " + e.Err.Error()
	}
	return "quickwit: ingest discarded (" + e.Reason.String() + ")"
}

func (e *IngestError) Unwrap() error { return e.Err }

// ingestItem is one record flowing through the buffer. data is the original
// value (used by OnDiscard and for fire-and-forget worker-side encoding). raw,
// when non-nil, is the pre-encoded NDJSON line — set by the tracked IngestSync/
// IngestBatch path so the worker never re-encodes it and encode errors surface
// synchronously to the caller. ack is nil for fire-and-forget Ingest, so every
// settle site is a no-op and the hot path stays allocation-free.
type ingestItem struct {
	data any
	raw  []byte
	ack  *ackBatch
}

// ackBatch is the shared completion handle for the N items of one IngestSync or
// IngestBatch call. The worker settles each item exactly once — nil on HTTP 200,
// an *IngestError on a terminal drop. done is closed on the 0-transition and the
// first non-nil error wins. settle is concurrency-safe because, with
// SetConcurrent > 1, sibling items of one call may be flushed by different
// workers simultaneously.
type ackBatch struct {
	mu        sync.Mutex
	remaining int
	err       error
	done      chan struct{}
}

func newAckBatch(n int) *ackBatch {
	return &ackBatch{remaining: n, done: make(chan struct{})}
}

func (a *ackBatch) settle(err error) {
	if a == nil { // fire-and-forget item: no completion handle
		return
	}
	a.mu.Lock()
	if a.remaining == 0 {
		a.mu.Unlock()
		return
	}
	if err != nil && a.err == nil {
		a.err = err
	}
	a.remaining--
	last := a.remaining == 0
	a.mu.Unlock()
	if last {
		close(a.done)
	}
}

func (a *ackBatch) wait(ctx context.Context) error {
	select {
	case <-a.done:
		a.mu.Lock()
		err := a.err
		a.mu.Unlock()
		return err
	case <-ctx.Done():
		// Ambiguous: the items are still in the worker buffer and may yet be
		// ingested. The caller must treat this as "not confirmed" and Nack.
		return fmt.Errorf("quickwit: ingest pending: %w", ctx.Err())
	}
}

// IngestReceipt is the completion handle returned by IngestBatch.
type IngestReceipt struct {
	ack *ackBatch
}

// Wait blocks until every item in the batch is durably accepted (returns nil) or
// terminally dropped (returns an *IngestError), or until ctx fires (returns a
// wrapped context error meaning "not confirmed"). See IngestSync for the full
// error contract.
func (r *IngestReceipt) Wait(ctx context.Context) error { return r.ack.wait(ctx) }

// Done is closed once every item in the batch has reached a terminal state, for
// callers that want to select on it alongside other events.
func (r *IngestReceipt) Done() <-chan struct{} { return r.ack.done }

type Client struct {
	client              *http.Client
	auth                func(req *http.Request)
	endpoint            string // http://{host}/api/v1/{index_name}
	batchSize           int
	maxDelay            time.Duration
	ingestBufferSize    int
	ingestTimeout       time.Duration
	discard             bool
	concurrent          int
	ingestBuffer        chan ingestItem
	defaultClient       *http.Client
	onceDefaultClient   sync.Once
	onceSetup           sync.Once
	onceClose           sync.Once
	stopWg              sync.WaitGroup
	closeSignal         chan struct{}
	onDiscard           OnDiscardFunc
	onReject            OnRejectFunc
	autoReduceBatchSize bool
	gzipEnabled         bool
	detailedResponse    bool
	sendMu              sync.RWMutex // guards buffer sends against close(ingestBuffer) in Close
	closed              bool         // set under sendMu write lock in Close
}

func NewClient(endpoint string) *Client {
	return &Client{
		endpoint:    endpoint,
		closeSignal: make(chan struct{}),
	}
}

func (c *Client) SetAuth(auth func(req *http.Request)) {
	c.auth = auth
}

func (c *Client) SetHTTPClient(client *http.Client) {
	c.client = client
}

func (c *Client) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

func (c *Client) SetMaxDelay(maxDelay time.Duration) {
	c.maxDelay = maxDelay
}

func (c *Client) SetIngestBufferSize(size int) {
	c.ingestBufferSize = size
}

func (c *Client) SetIngestTimeout(timeout time.Duration) {
	c.ingestTimeout = timeout
}

func (c *Client) SetDiscard(discard bool) {
	c.discard = discard
}

func (c *Client) SetConcurrent(concurrent int) {
	c.concurrent = concurrent
}

func (c *Client) SetAutoReduceBatchSize(autoReduceBatchSize bool) {
	c.autoReduceBatchSize = autoReduceBatchSize
}

// SetGzip enables gzip compression of the ingest request body. When enabled,
// each batch is compressed and sent with a Content-Encoding: gzip header.
// The Quickwit endpoint must accept gzip-encoded ingest requests.
func (c *Client) SetGzip(enabled bool) {
	c.gzipEnabled = enabled
}

func (c *Client) OnDiscard(f OnDiscardFunc) {
	c.onDiscard = f
}

// OnReject registers a callback invoked with the number of documents the server
// reported as parse-rejected on an otherwise-successful (200) ingest. It is the
// hook for partial rejections that the client cannot attribute to a specific
// document. Set before the first Ingest.
func (c *Client) OnReject(f OnRejectFunc) {
	c.onReject = f
}

// SetIngestDetailedResponse requests Quickwit's detailed ingest response
// (?detailed_response=true) so per-document parse-failure reasons are logged
// when documents are rejected. It adds response size/CPU on the server, so it is
// off by default. Set before the first Ingest.
func (c *Client) SetIngestDetailedResponse(enabled bool) {
	c.detailedResponse = enabled
}

func (c *Client) httpClient() *http.Client {
	if c.client != nil {
		return c.client
	}
	// When no client is supplied, fall back to a tuned default instead of
	// http.DefaultClient. Its transport's MaxIdleConnsPerHost defaults to 2,
	// which would close (and force a fresh handshake on) every connection
	// beyond the first two when SetConcurrent raises the worker count. Sizing
	// the idle pool to the worker count lets each worker keep its connection
	// hot for reuse. Built once, lazily, so the pool is shared across workers.
	c.onceDefaultClient.Do(func() {
		c.defaultClient = c.newDefaultClient()
	})
	return c.defaultClient
}

func (c *Client) newDefaultClient() *http.Client {
	concurrent := c.getConcurrent()

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = concurrent
	if t.MaxIdleConns < concurrent {
		t.MaxIdleConns = concurrent
	}

	// Bound the time spent waiting for response headers to a fraction of the
	// per-flush deadline. A server that completes TCP/TLS and reads the body but
	// then stalls before responding (typical of an L4 load balancer fronting an
	// overloaded indexer) frees the worker in ~1/3 of getIngestTimeout instead of
	// tying it up for the full deadline. Firing returns a transport error, which
	// is already retryable.
	t.ResponseHeaderTimeout = c.getIngestTimeout() / 3

	// No client-level timeout: the per-request context deadline from
	// getIngestTimeout already bounds each flush, matching prior behavior.
	return &http.Client{Transport: t}
}

// RetryAfterMax caps how long a Retry-After header can delay the next flush, so
// a buggy or hostile value cannot stall ingestion indefinitely.
const RetryAfterMax = 60 * time.Second

// jitterBackoff returns a duration in [d/2, d] (equal jitter) so retries across
// workers spread out instead of arriving in lockstep waves. The floor of d/2
// keeps the fast path from collapsing to a near-zero sleep.
func jitterBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// parseRetryAfter parses an HTTP Retry-After header, which is either a number of
// seconds or an HTTP-date. now is passed in so the worker can use a single clock
// read. It returns ok=false when the header is absent or unparseable.
func parseRetryAfter(h string, now time.Time) (time.Duration, bool) {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(h); err == nil {
		if secs <= 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(h); err == nil {
		if d := t.Sub(now); d > 0 {
			return d, true
		}
		return 0, false
	}
	return 0, false
}

func (c *Client) getMaxDelay() time.Duration {
	if c.maxDelay <= 0 {
		return IngestMaxDelay
	}
	return c.maxDelay
}

func (c *Client) getBatchSize() int {
	if c.batchSize <= 0 {
		return IngestBatchSize
	}
	return c.batchSize
}

func (c *Client) getConcurrent() int {
	if c.concurrent <= 0 {
		return IngestConcurrent
	}
	return c.concurrent
}

func (c *Client) getIngestTimeout() time.Duration {
	if c.ingestTimeout <= 0 {
		return IngestTimeout
	}
	return c.ingestTimeout
}

func (c *Client) doAuth(req *http.Request) {
	if c.auth != nil {
		c.auth(req)
	}
}

func (c *Client) invokeOnDiscard(data any) {
	if c.onDiscard != nil {
		c.onDiscard(data)
	}
}

func (c *Client) invokeOnReject(numRejected int) {
	if c.onReject != nil {
		c.onReject(numRejected)
	}
}

// inspectIngestResponse parses a 200 ingest response and reports whether the
// server rejected any documents and whether it rejected ALL of them. It is
// lenient: an unparseable body or absent fields (older servers) is treated as
// "no rejection info" so the accept-on-200 behavior is preserved. allRejected is
// reported only when the server ingested nothing, which makes the per-item
// ReasonRejected verdict exact (no accepted doc is dead-lettered).
func (c *Client) inspectIngestResponse(body []byte, endpoint string) (rejected, allRejected bool) {
	// An empty body (some proxies, older servers) carries no rejection info;
	// accept silently rather than warn on every flush.
	if len(bytes.TrimSpace(body)) == 0 {
		return false, false
	}
	var r ingestResponse
	if err := json.Unmarshal(body, &r); err != nil {
		slog.Warn("quickwit: could not parse ingest response", "endpoint", endpoint, "error", err)
		return false, false
	}
	if r.NumRejectedDocs == nil || *r.NumRejectedDocs <= 0 {
		return false, false
	}

	ingested := int64(-1) // -1: server did not report it
	if r.NumIngestedDocs != nil {
		ingested = *r.NumIngestedDocs
	}
	slog.Warn("quickwit: server rejected documents",
		"endpoint", endpoint,
		"rejected", *r.NumRejectedDocs,
		"ingested", ingested,
	)
	for _, pf := range r.ParseFailures {
		slog.Warn("quickwit: document parse failure", "reason", pf.Reason, "message", pf.Message)
	}
	c.invokeOnReject(int(*r.NumRejectedDocs))

	// Unambiguous only when the server ingested nothing: every doc was rejected.
	return true, r.NumIngestedDocs != nil && *r.NumIngestedDocs == 0
}

// Ingest enqueues data for asynchronous, fire-and-forget delivery. Each value is
// JSON-encoded by a background worker and sent in batches. Ingest does not report
// delivery: on a crash before the batch is flushed the data is lost, and after
// Close it is dropped (via OnDiscard). When you need to know the data was durably
// accepted — for example to Ack a pub/sub message — use IngestSync instead.
//
// Without SetDiscard, Ingest blocks while the buffer is full; with SetDiscard it
// drops the item and invokes OnDiscard.
func (c *Client) Ingest(data ...any) {
	c.onceSetup.Do(c.setup)

	c.sendMu.RLock()
	defer c.sendMu.RUnlock()
	if c.closed {
		for _, x := range data {
			c.invokeOnDiscard(x)
		}
		return
	}
	for _, x := range data {
		it := ingestItem{data: x}
		if c.discard {
			select {
			case c.ingestBuffer <- it:
			default:
				c.invokeOnDiscard(x)
			}
		} else {
			c.ingestBuffer <- it
		}
	}
}

// IngestSync synchronously ingests data and reports whether the server durably
// accepted it, so a caller can decide when to Ack an upstream message (e.g. from
// a pub/sub subscription). It JSON-encodes every value up front — a non-encodable
// value returns *IngestError{Reason: ReasonEncode} immediately, before anything
// is buffered — enqueues the items, and blocks until:
//
//   - all items were durably accepted (HTTP 200 with no rejection) → returns nil
//     (Ack);
//   - an item was terminally dropped → returns an *IngestError whose Reason is
//     ReasonEncode (bad document) or ReasonRejected (server parse-rejected the
//     whole batch) — poison, dead-letter and Ack; ReasonServer (permanent 4xx —
//     inspect, usually a misconfig to fix); or ReasonBufferFull / ReasonClosed
//     (transient — Nack to redeliver);
//   - ctx fired first → returns a wrapped context error. This is AMBIGUOUS: the
//     item is still buffered and may yet be ingested, so treat it as "not
//     confirmed" and Nack.
//
// Delivery is at-least-once: there is an unavoidable window between the server's
// 200 and the caller's Ack, so a crash can redeliver. Attach a deterministic
// document id and dedup on it. Pass a ctx whose deadline sits comfortably inside
// the subscription's ack-deadline; IngestSync never creates its own timeout.
//
// Ack latency is bounded by the round-trip, not by maxDelay: because the items
// are completion-tracked, the worker flushes them as soon as it goes idle rather
// than waiting out the flush interval, while still coalescing bursts into batches.
//
// With multiple values the result is a single first-error-wins verdict for the
// whole call; ingest one document per call for a clean message-to-verdict map.
func (c *Client) IngestSync(ctx context.Context, data ...any) error {
	if len(data) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("quickwit: ingest pending: %w", err)
	}

	raws, err := encodeAll(data)
	if err != nil {
		return err
	}

	c.onceSetup.Do(c.setup)
	ack := newAckBatch(len(data))
	c.enqueueAll(ctx, data, raws, ack)
	return ack.wait(ctx)
}

// IngestBatch enqueues data like IngestSync but returns immediately with a
// receipt the caller can Wait on later (or select on via Done). It uses the same
// backpressure as Ingest: without SetDiscard it blocks while the buffer is full.
// A non-encodable value settles the whole batch with ReasonEncode without
// buffering anything. See IngestSync for the delivery guarantee and caveats.
func (c *Client) IngestBatch(data ...any) *IngestReceipt {
	ack := newAckBatch(len(data))
	if len(data) == 0 {
		close(ack.done)
		return &IngestReceipt{ack: ack}
	}

	raws, err := encodeAll(data)
	if err != nil {
		// Nothing buffered; settle every item with the encode error.
		for range data {
			ack.settle(err)
		}
		return &IngestReceipt{ack: ack}
	}

	c.onceSetup.Do(c.setup)
	// context.Background never cancels, so enqueue blocks on a full buffer
	// (matching Ingest) and only bails on Close.
	c.enqueueAll(context.Background(), data, raws, ack)
	return &IngestReceipt{ack: ack}
}

// encodeAll JSON-encodes every value into an NDJSON line (data + '\n'), failing
// fast on the first non-encodable value so the caller learns of a poison item
// before anything is buffered.
func encodeAll(data []any) ([][]byte, error) {
	raws := make([][]byte, len(data))
	for i, x := range data {
		raw, err := json.Marshal(x)
		if err != nil {
			return nil, &IngestError{Reason: ReasonEncode, Err: err}
		}
		raws[i] = append(raw, '\n')
	}
	return raws, nil
}

// enqueueAll sends every item of one tracked call, sharing ack. If a send fails
// terminally (closed, buffer-full in discard mode, or ctx fired), it settles the
// failing item and the not-yet-enqueued remainder with that error so wait can
// complete; items enqueued before the failure settle later via the worker.
func (c *Client) enqueueAll(ctx context.Context, data []any, raws [][]byte, ack *ackBatch) {
	for i := range raws {
		if err := c.enqueueTracked(ctx, ingestItem{data: data[i], raw: raws[i], ack: ack}); err != nil {
			for j := i; j < len(raws); j++ {
				ack.settle(err)
			}
			return
		}
	}
}

// enqueueTracked sends one tracked item, honoring discard mode, the caller ctx,
// and shutdown. It never blocks past ctx, and the RLock makes the send mutually
// exclusive with Close's channel close, so it cannot panic on a closed channel.
func (c *Client) enqueueTracked(ctx context.Context, it ingestItem) error {
	c.sendMu.RLock()
	defer c.sendMu.RUnlock()
	if c.closed {
		return &IngestError{Reason: ReasonClosed}
	}
	if c.discard {
		select {
		case c.ingestBuffer <- it:
			return nil
		default:
			return &IngestError{Reason: ReasonBufferFull}
		}
	}
	select {
	case c.ingestBuffer <- it:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("quickwit: ingest pending: %w", ctx.Err())
	case <-c.closeSignal:
		return &IngestError{Reason: ReasonClosed}
	}
}

// discardItem reports a dropped item: fire-and-forget items go to OnDiscard;
// tracked items settle their ack with err (no OnDiscard — the ack is the signal).
func (c *Client) discardItem(it ingestItem, err error) {
	if it.ack != nil {
		it.ack.settle(err)
		return
	}
	c.invokeOnDiscard(it.data)
}

func (c *Client) Close() {
	c.onceSetup.Do(c.setup)
	c.onceClose.Do(func() {
		// Signal first so blocked tracked sends and the worker's retry loop can
		// bail, then take the write lock — which waits for all in-flight sends to
		// release their RLock — before closing the buffer, so no send can race the
		// close and panic.
		close(c.closeSignal)
		c.sendMu.Lock()
		c.closed = true
		close(c.ingestBuffer)
		c.sendMu.Unlock()
	})
	c.stopWg.Wait()
}

func (c *Client) setup() {
	if c.ingestBufferSize <= 0 {
		c.ingestBufferSize = IngestBufferSize
	}
	c.ingestBuffer = make(chan ingestItem, c.ingestBufferSize)

	concurrent := c.getConcurrent()
	c.stopWg.Add(concurrent)
	for range concurrent {
		go c.loop()
	}
}

func (c *Client) loop() {
	defer c.stopWg.Done()

	var buf bytes.Buffer
	jsonEnc := json.NewEncoder(&buf)

	// gzip state is per-worker and reused across flushes via Reset to avoid
	// reallocating the compressor on every batch.
	useGzip := c.gzipEnabled
	var gzBuf bytes.Buffer
	var gzw *gzip.Writer
	if useGzip {
		gzw = gzip.NewWriter(&gzBuf)
	}

	batchSize := c.getBatchSize()
	buffer := make([]ingestItem, 0, batchSize)
	var resetBatchSizeAfter time.Time

	// retryAfter carries a server-requested backpressure delay (from a 429/503
	// Retry-After header) out of flush() and into retryFlush()'s sleep. It is
	// reset at the top of every flush so it only reflects the most recent attempt.
	var retryAfter time.Duration

	endpoint := c.endpoint
	endpoint = strings.TrimSuffix(endpoint, "/")
	endpoint = endpoint + "/ingest"
	if c.detailedResponse {
		endpoint = endpoint + "?detailed_response=true"
	}

	flush := func(batch []ingestItem) bool {
		if len(batch) == 0 {
			return true
		}

		retryAfter = 0
		buf.Reset()

		// encodeFailures can only ever hold fire-and-forget items (ack == nil):
		// tracked items carry a pre-encoded raw line, so they cannot fail here.
		// On HTTP 200 we settle the whole batch directly — settle(nil) is a no-op
		// for the ack==nil encode failures — so no separate "encoded" slice is
		// allocated per flush.
		var encodeFailures []ingestItem
		for _, it := range batch {
			if it.raw != nil {
				buf.Write(it.raw)
				continue
			}
			if err := jsonEnc.Encode(it.data); err != nil {
				slog.Error("quickwit: failed to encode record, discarding", "error", err)
				encodeFailures = append(encodeFailures, it)
				continue
			}
		}

		// All items were unencodable — discard them and report success so the
		// caller clears the buffer and does not retry with the same items.
		if buf.Len() == 0 {
			for _, it := range encodeFailures {
				c.invokeOnDiscard(it.data)
			}
			return true
		}

		// body holds the NDJSON payload, gzip-compressed when enabled. Writing
		// to a bytes.Buffer cannot fail, so gzw errors are not expected here.
		body := buf.Bytes()
		if useGzip {
			gzBuf.Reset()
			gzw.Reset(&gzBuf)
			if _, err := gzw.Write(buf.Bytes()); err != nil {
				slog.Error("quickwit: failed to gzip ingest body", "error", err)
				return false
			}
			if err := gzw.Close(); err != nil {
				slog.Error("quickwit: failed to finalize gzip ingest body", "error", err)
				return false
			}
			body = gzBuf.Bytes()
		}

		ctx := context.Background()
		cancel := context.CancelFunc(func() {})
		if t := c.getIngestTimeout(); t != 0 {
			ctx, cancel = context.WithTimeout(ctx, t)
		}
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return false
		}
		if useGzip {
			req.Header.Set("Content-Encoding", "gzip")
		}
		c.doAuth(req)

		resp, err := c.httpClient().Do(req)
		if err != nil {
			return false
		}

		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			slog.Error("quickwit: ingest status not ok", "status", resp.Status)

			if resp.StatusCode == http.StatusRequestEntityTooLarge {
				if c.autoReduceBatchSize {
					beforeSize := batchSize
					batchSize = int(float64(batchSize) * ReduceBatchSizeToRatio)
					defaultSize := c.getBatchSize()
					minimumSize := int(float64(defaultSize) * ReduceBatchSizeMin)
					if batchSize < minimumSize {
						batchSize = minimumSize
					}
					resetBatchSizeAfter = time.Now().Add(ResetBatchSizeAfter)
					slog.Info("quickwit: auto reduce batch size",
						"new", batchSize,
						"old", beforeSize,
						"default", defaultSize,
						"minimum", minimumSize,
						"resetAfter", resetBatchSizeAfter.Format(time.RFC3339),
					)
				}
				return false
			}

			// Permanent rejection: retrying the same batch cannot succeed, so
			// settle/discard it and report it handled. Otherwise the worker would
			// loop forever in retryFlush and, with the default 2 workers, a couple
			// of poison batches would freeze all ingest.
			if permanentIngestStatus(resp.StatusCode) {
				err := &IngestError{
					Reason: ReasonServer,
					Err:    fmt.Errorf("quickwit: ingest rejected with status %s", resp.Status),
				}
				for _, it := range batch {
					c.discardItem(it, err)
				}
				return true
			}

			// Backpressure: on 429/503, honor Retry-After (clamped) so we pace to
			// the server instead of hammering it on the fixed backoff.
			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
				if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
					retryAfter = min(d, RetryAfterMax)
				}
			}

			// Retryable (5xx, 408, 425, 429, transport errors handled above).
			return false
		}

		// HTTP 200 only means the documents were queued. Read the response (bounded)
		// and inspect it: the server reports how many documents it parse-rejected.
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, ingestResponseMaxBytes))
		io.Copy(io.Discard, resp.Body) // drain any remainder so the connection can be reused
		resp.Body.Close()
		if readErr != nil {
			// A truncated/incomplete 200 is ambiguous — retry rather than settle.
			slog.Error("quickwit: failed to read ingest response", "error", readErr)
			return false
		}

		// When the whole batch was parse-rejected the verdict is exact, so settle
		// tracked items with ReasonRejected (and discard fire-and-forget) instead
		// of falsely reporting them durable. A partial rejection cannot be
		// attributed to specific documents in a coalesced batch — it is surfaced
		// via OnReject inside inspectIngestResponse and the batch is still Acked.
		if rejected, allRejected := c.inspectIngestResponse(respBody, endpoint); rejected && allRejected {
			err := &IngestError{
				Reason: ReasonRejected,
				Err:    fmt.Errorf("quickwit: server rejected all %d document(s) in the batch", len(batch)),
			}
			for _, it := range batch {
				c.discardItem(it, err)
			}
			return true
		}

		// Durably accepted: settle every item in the batch (settle is a no-op for
		// fire-and-forget and encode-failure items, which have ack == nil), then
		// discard items that failed encoding.
		for _, it := range batch {
			it.ack.settle(nil)
		}
		for _, it := range encodeFailures {
			c.invokeOnDiscard(it.data)
		}

		return true
	}

	// flushOversize when buffer is oversize (from auto batch resize)
	// split buffer into smaller parts then flush each part separately
	flushOversize := func() bool {
		if len(buffer) == 0 {
			return true
		}

		if len(buffer) <= batchSize {
			if !flush(buffer) {
				return false
			}
			buffer = buffer[:0]
			return true
		}

		var processed int

		for chunk := range slices.Chunk(buffer, batchSize) {
			if !flush(chunk) {
				break
			}
			processed += len(chunk)
		}

		// drop the flushed prefix, keeping unprocessed records in their
		// original order at the front of the buffer, and clear the now
		// unused tail so flushed records can be garbage collected
		oldLen := len(buffer)
		remaining := copy(buffer, buffer[processed:])
		clear(buffer[remaining:oldLen])
		buffer = buffer[:remaining]
		return len(buffer) == 0
	}

	// retryFlush attempts to flush the buffer with retries
	// isClosing indicates if this is a final flush during shutdown
	retryFlush := func(isClosing bool) bool {
		// If not closing, retry indefinitely until success
		if !isClosing {
			attempt := 0
			backoff := 100 * time.Millisecond

			for {
				// close signal during retry
				select {
				default:
				case <-c.closeSignal:
					goto closing
				}

				if flushOversize() {
					return true
				}

				attempt++
				slog.Info("quickwit: flush failed, retrying indefinitely", "attempt", attempt)

				// Equal-jittered backoff, raised to a server-requested Retry-After
				// when present. The sleep is interruptible so Close (or a long
				// Retry-After) cannot delay shutdown past the next close signal.
				sleep := jitterBackoff(backoff)
				if retryAfter > sleep {
					sleep = retryAfter
				}
				timer := time.NewTimer(sleep)
				select {
				case <-timer.C:
				case <-c.closeSignal:
					timer.Stop()
					goto closing
				}

				// Exponential backoff with a cap
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > time.Second {
					backoff = time.Second
				}
			}
		}

	closing:

		// For closing case, use limited retries
		maxRetries := 5
		backoff := 100 * time.Millisecond

		for i := 0; i < maxRetries; i++ {
			if flushOversize() {
				return true
			}

			// Don't sleep on the last attempt
			if i < maxRetries-1 {
				slog.Info("quickwit: flush failed while closing, retrying", "attempt", i+1, "maxRetries", maxRetries)
				// Jittered backoff; Retry-After is intentionally not honored here
				// so a server-requested delay cannot stretch shutdown.
				time.Sleep(jitterBackoff(backoff))
				// Exponential backoff with a cap
				backoff = time.Duration(float64(backoff) * 1.5)
				if backoff > time.Second {
					backoff = time.Second
				}
			}
		}

		return false
	}

	// finalFlush drains the buffer on shutdown, settling/discarding anything the
	// server never accepted.
	finalFlush := func() {
		if !retryFlush(true) {
			slog.Error("quickwit: flush failed while closing")
			for _, it := range buffer {
				c.discardItem(it, &IngestError{Reason: ReasonClosed})
			}
		}
	}

	// maybeResetBatchSize restores the batch size once the post-413 reduction
	// window has elapsed. It is driven by the ticker rather than the flush
	// success path so it fires even when traffic goes quiet — an idle buffer
	// never calls flush, so a success-only reset could leave the batch
	// permanently shrunk after a transient 413 spike.
	maybeResetBatchSize := func() {
		if !resetBatchSizeAfter.IsZero() && time.Now().After(resetBatchSizeAfter) {
			beforeSize := batchSize
			batchSize = c.getBatchSize()
			resetBatchSizeAfter = time.Time{}
			slog.Info("quickwit: reset batch size", "batchSize", batchSize, "old", beforeSize)
		}
	}

	ticker := time.NewTicker(c.getMaxDelay())
	defer ticker.Stop()

	// hasTracked is true while the buffer may hold a completion-tracked item
	// (one submitted via IngestSync / IngestBatch). Such items are latency
	// sensitive — the caller is blocked waiting to Ack — so once one is buffered
	// the worker flushes as soon as the channel goes idle instead of waiting for
	// the ticker, bounding Ack latency by the round-trip rather than by maxDelay.
	// Pure fire-and-forget traffic never sets this, so its batching is unchanged.
	hasTracked := false

	for {
		select {
		case <-ticker.C:
			maybeResetBatchSize()
			flushOversize()
			if len(buffer) == 0 {
				hasTracked = false
			}
		case x, ok := <-c.ingestBuffer:
			if !ok { // channel closed
				finalFlush()
				return
			}
			buffer = append(buffer, x)
			if x.ack != nil {
				hasTracked = true
			}

			if !hasTracked {
				if len(buffer) >= batchSize {
					retryFlush(false)
				}
				continue
			}

			// A tracked item is waiting: greedily pull whatever else is already
			// queued (up to a full batch) so a burst still coalesces, then flush
			// without waiting for the ticker.
			closed := false
		drain:
			for len(buffer) < batchSize {
				select {
				case x, ok := <-c.ingestBuffer:
					if !ok {
						closed = true
						break drain
					}
					buffer = append(buffer, x)
					if x.ack != nil {
						hasTracked = true
					}
				default:
					break drain
				}
			}
			if closed {
				finalFlush()
				return
			}
			retryFlush(false)
			if len(buffer) == 0 {
				hasTracked = false
			}
		}
	}
}

type SearchOpt struct {
	StartTimestamp int64
	EndTimestamp   int64
	StartOffset    int64
	MaxHits        int64
	SearchField    []string
	SnippetFields  []string
	SortBy         string
	Format         string
}

type SearchResult struct {
	Hits              json.RawMessage
	NumHits           int64
	ElapsedTimeMicros int64
}

type searchRequestQueryString struct {
	Query          string   `json:"query"`
	StartTimestamp *int64   `json:"start_timestamp,omitempty"`
	EndTimestamp   *int64   `json:"end_timestamp,omitempty"`
	StartOffset    *int64   `json:"start_offset,omitempty"`
	MaxHits        *int64   `json:"max_hits,omitempty"`
	SearchField    []string `json:"search_field,omitempty"`
	SnippetFields  []string `json:"snippet_fields,omitempty"`
	SortBy         string   `json:"sort_by,omitempty"`
	Format         *string  `json:"format,omitempty"`
}

type searchResponseRest struct {
	Hits              json.RawMessage `json:"hits"`
	NumHits           int64           `json:"num_hits"`
	ElapsedTimeMicros int64           `json:"elapsed_time_micros"`
}

func (c *Client) Search(ctx context.Context, query string, opt *SearchOpt) (*SearchResult, error) {
	params := searchRequestQueryString{
		Query:  query,
		Format: Ptr("json"),
	}
	if opt != nil {
		if opt.StartTimestamp != 0 {
			params.StartTimestamp = &opt.StartTimestamp
		}
		if opt.EndTimestamp != 0 {
			params.EndTimestamp = &opt.EndTimestamp
		}
		if opt.StartOffset != 0 {
			params.StartOffset = &opt.StartOffset
		}
		if opt.MaxHits != 0 {
			params.MaxHits = &opt.MaxHits
		}
		params.SearchField = opt.SearchField
		params.SnippetFields = opt.SnippetFields
		params.SortBy = opt.SortBy
		if opt.Format != "" {
			params.Format = &opt.Format
		}
	}

	reqBody, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.endpoint, "/")+"/search", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.doAuth(req)

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("quickwit: search status not ok, code: %d", resp.StatusCode)
	}

	var res searchResponseRest
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &SearchResult{
		Hits:              res.Hits,
		NumHits:           res.NumHits,
		ElapsedTimeMicros: res.ElapsedTimeMicros,
	}, nil
}

func Ptr[T any](t T) *T {
	return &t
}
