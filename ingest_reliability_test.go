package quickwit_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

// Core: a permanent 4xx is reported as ReasonServer promptly, not retried until
// the context deadline.
func TestIngestSync_PermanentStatusReturnsServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusBadRequest) // 400: permanent
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	err := c.IngestSync(ctx, map[string]any{"index": 0})
	elapsed := time.Since(start)

	var ie *quickwit.IngestError
	if !errors.As(err, &ie) || ie.Reason != quickwit.ReasonServer {
		t.Fatalf("err = %v, want *IngestError{ReasonServer}", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("took %v, want a prompt permanent verdict (not a ctx timeout)", elapsed)
	}
}

// Core: a permanent rejection must not wedge the worker — a second call still
// gets a verdict rather than blocking behind an infinite retry of the first.
func TestIngestSync_PermanentStatusDoesNotWedgeWorker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNotFound) // 404: permanent (e.g. wrong index)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1) // a single worker: if the first call wedges it, the second hangs
	defer c.Close()

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := c.IngestSync(ctx, map[string]any{"index": i})
		cancel()
		var ie *quickwit.IngestError
		if !errors.As(err, &ie) || ie.Reason != quickwit.ReasonServer {
			t.Fatalf("call %d: err = %v, want *IngestError{ReasonServer}", i, err)
		}
	}
}

// Core: a 5xx stays retryable — it must NOT be treated as a permanent rejection.
func TestIngestSync_ServerErrorIsRetriedNotPermanent(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError) // transient
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.IngestSync(ctx, map[string]any{"index": 0}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil after retries", err)
	}
	if n := attempts.Load(); n < 3 {
		t.Errorf("server saw %d attempts, want >= 3 (two 500s then success)", n)
	}
}

// Core: fire-and-forget items on a permanent rejection are surfaced via
// OnDiscard and do not wedge the worker; Close returns promptly.
func TestIngest_PermanentStatusDiscardsFireAndForget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusUnprocessableEntity) // 422: permanent
	}))
	defer server.Close()

	const numItems = 5
	var discarded atomic.Int64

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	c.OnDiscard(func(any) { discarded.Add(1) })

	for i := 0; i < numItems; i++ {
		c.Ingest(map[string]any{"index": i})
	}

	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return — worker wedged on permanent rejection")
	}

	if n := discarded.Load(); n != numItems {
		t.Errorf("OnDiscard fired %d times, want %d", n, numItems)
	}
}

// Core: removing the per-flush "encoded" slice must not change worker-side
// encode-failure handling — a poison fire-and-forget value is still discarded
// while the good value alongside it is delivered.
func TestIngest_WorkerEncodeFailureStillDiscarded(t *testing.T) {
	var mu sync.Mutex
	var received []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, parseIndices(body)...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	var discarded atomic.Int64

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000)            // both items ride in one batch
	c.SetMaxDelay(10 * time.Second) // flushed by Close
	c.OnDiscard(func(any) { discarded.Add(1) })

	c.Ingest(
		map[string]any{"index": 0},
		map[string]any{"bad": make(chan int)}, // unencodable
		map[string]any{"index": 2},
	)
	c.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || received[0] != 0 || received[1] != 2 {
		t.Errorf("received = %v, want [0 2]", received)
	}
	if n := discarded.Load(); n != 1 {
		t.Errorf("OnDiscard fired %d times, want 1 (the poison value)", n)
	}
}
