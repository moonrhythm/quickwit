package quickwit_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

// Core: a 200 whose body reports the whole batch was parse-rejected
// (num_ingested_docs == 0) yields a terminal ReasonRejected, not a false nil.
func TestIngestSync_AllRejectedReturnsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"num_docs_for_processing":1,"num_ingested_docs":0,"num_rejected_docs":1}`)
	}))
	defer server.Close()

	var rejectedCount atomic.Int64
	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.OnReject(func(n int) { rejectedCount.Add(int64(n)) })
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.IngestSync(ctx, map[string]any{"index": 0})
	var ie *quickwit.IngestError
	if !errors.As(err, &ie) || ie.Reason != quickwit.ReasonRejected {
		t.Fatalf("err = %v, want *IngestError{ReasonRejected}", err)
	}
	if n := rejectedCount.Load(); n != 1 {
		t.Errorf("OnReject total = %d, want 1", n)
	}
}

// Core: a 200 that reports no rejection settles as accepted (nil).
func TestIngestSync_NoRejectionAccepts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"num_docs_for_processing":1,"num_ingested_docs":1,"num_rejected_docs":0}`)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.OnReject(func(int) { t.Error("OnReject must not fire when nothing was rejected") })
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.IngestSync(ctx, map[string]any{"index": 0}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}
}

// Core: a partial rejection (some ingested, some rejected) cannot be attributed
// per-document, so the batch is still accepted (nil) but OnReject fires with the
// count — the operator's signal for the otherwise-silent loss.
func TestIngestSync_PartialRejectionAcksAndReports(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"num_docs_for_processing":3,"num_ingested_docs":2,"num_rejected_docs":1}`)
	}))
	defer server.Close()

	var reported atomic.Int64
	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.OnReject(func(n int) { reported.Store(int64(n)) })
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Three docs in one call -> one coalesced batch.
	err := c.IngestSync(ctx,
		map[string]any{"index": 0},
		map[string]any{"index": 1},
		map[string]any{"index": 2},
	)
	if err != nil {
		t.Fatalf("partial rejection: IngestSync returned %v, want nil (cannot attribute)", err)
	}
	if n := reported.Load(); n != 1 {
		t.Errorf("OnReject reported %d, want 1", n)
	}
}

// Core: an older/different server whose body lacks the rejection fields is
// treated as accepted (lenient), preserving the accept-on-200 contract.
func TestIngestSync_UnknownResponseBodyAccepts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"num_docs_for_processing":1}`) // no num_rejected_docs
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.IngestSync(ctx, map[string]any{"index": 0}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil for a response without rejection fields", err)
	}
}

// Core: a truncated 200 body (transport cut mid-response) is ambiguous and must
// be retried, not settled.
func TestIngestSync_TruncatedResponseRetried(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		n := attempts.Add(1)
		if n == 1 {
			// Promise more than we send, then hijack-close to truncate the body.
			w.Header().Set("Content-Length", "1024")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, `{"num_ingested`)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				conn.Close() // abrupt close -> client read error
			}
			return
		}
		io.WriteString(w, `{"num_docs_for_processing":1,"num_ingested_docs":1,"num_rejected_docs":0}`)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.IngestSync(ctx, map[string]any{"index": 0}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil after retrying the truncated response", err)
	}
	if n := attempts.Load(); n < 2 {
		t.Errorf("attempts = %d, want >= 2 (truncated 200 retried)", n)
	}
}

// Core: SetIngestDetailedResponse adds ?detailed_response=true to the ingest URL.
func TestIngest_DetailedResponseQueryParam(t *testing.T) {
	var mu sync.Mutex
	var sawDetailed bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.URL.Query().Get("detailed_response") == "true" {
			sawDetailed = true
		}
		mu.Unlock()
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"num_ingested_docs":1,"num_rejected_docs":0}`)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	c.SetIngestDetailedResponse(true)

	c.Ingest(map[string]any{"index": 0})
	c.Close()

	mu.Lock()
	defer mu.Unlock()
	if !sawDetailed {
		t.Error("ingest request did not carry ?detailed_response=true")
	}
}

// Core: with the whole batch rejected, fire-and-forget items are surfaced via
// OnDiscard (and the batch is dropped, not retried forever).
func TestIngest_AllRejectedDiscardsFireAndForget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		n := int64(strings.Count(strings.TrimSpace(string(body)), "\n") + 1)
		io.WriteString(w, `{"num_ingested_docs":0,"num_rejected_docs":`+itoa(n)+`}`)
	}))
	defer server.Close()

	var discarded atomic.Int64
	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(2)
	c.OnDiscard(func(any) { discarded.Add(1) })

	c.Ingest(map[string]any{"index": 0}, map[string]any{"index": 1})

	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return — worker wedged on rejection")
	}
	if n := discarded.Load(); n != 2 {
		t.Errorf("OnDiscard fired %d times, want 2", n)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
