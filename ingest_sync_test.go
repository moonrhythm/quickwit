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

// Core: IngestSync returns nil only after the server accepts the data with 200,
// and the record actually reaches the server.
func TestIngestSync_ReturnsNilAfterServerAccepts(t *testing.T) {
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

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.IngestSync(ctx, map[string]any{"index": 7}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != 7 {
		t.Errorf("received = %v, want [7]", received)
	}
}

// Core: a non-encodable value is reported as ReasonEncode immediately, and
// nothing is sent to the server.
func TestIngestSync_EncodeFailureIsPoison(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// A channel value cannot be JSON-encoded.
	err := c.IngestSync(ctx, map[string]any{"bad": make(chan int)})
	var ie *quickwit.IngestError
	if !errors.As(err, &ie) || ie.Reason != quickwit.ReasonEncode {
		t.Fatalf("err = %v, want *IngestError{ReasonEncode}", err)
	}
	if n := requests.Load(); n != 0 {
		t.Errorf("server saw %d requests, want 0 (nothing should be sent)", n)
	}
}

// Core: when the server keeps failing, IngestSync does not return success; it
// surfaces the caller's context deadline as a (non-IngestError) "pending" error
// so the caller Nacks.
func TestIngestSync_ContextDeadlineWhileServerFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	err := c.IngestSync(ctx, map[string]any{"index": 1})
	if err == nil {
		t.Fatal("IngestSync returned nil, want a pending/deadline error")
	}
	// It must NOT be a terminal IngestError — the item is still in flight.
	var ie *quickwit.IngestError
	if errors.As(err, &ie) {
		t.Fatalf("err = %v (IngestError), want a wrapped context error", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

// Core: IngestSync after Close reports ReasonClosed rather than panicking or
// hanging.
func TestIngestSync_AfterCloseReturnsClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.Ingest(map[string]any{"index": 0}) // trigger setup
	c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.IngestSync(ctx, map[string]any{"index": 1})
	var ie *quickwit.IngestError
	if !errors.As(err, &ie) || ie.Reason != quickwit.ReasonClosed {
		t.Fatalf("err = %v, want *IngestError{ReasonClosed}", err)
	}
}

// Core: a 413 that splits the batch into smaller chunks still settles every
// tracked item — IngestSync of all records returns nil and all arrive.
func TestIngestSync_SettlesAcross413Split(t *testing.T) {
	var mu sync.Mutex
	var requestCount int
	var received []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requestCount++
		first := requestCount == 1
		if !first {
			received = append(received, parseIndices(body)...)
		}
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetAutoReduceBatchSize(true)
	c.SetBatchSize(10)
	c.SetMaxDelay(10 * time.Second)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// One call with 10 docs: first POST 413s, the retry splits into smaller
	// chunks. All items must settle (nil) once those chunks succeed.
	data := make([]any, 10)
	for i := range data {
		data[i] = map[string]any{"index": i}
	}
	if err := c.IngestSync(ctx, data...); err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 10 {
		t.Fatalf("received %d records, want 10", len(received))
	}
}

// Core: many concurrent IngestSync handlers are coalesced by the worker into
// far fewer requests than calls — batching survives the blocking-per-call model.
func TestIngestSync_ConcurrentCallsAreBatched(t *testing.T) {
	const numCalls = 200

	var requests atomic.Int64
	var mu sync.Mutex
	counts := make(map[int]int)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		for _, idx := range parseIndices(body) {
			counts[idx]++
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(50)
	c.SetMaxDelay(25 * time.Millisecond)
	defer c.Close()

	var wg sync.WaitGroup
	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.IngestSync(ctx, map[string]any{"index": i}); err != nil {
				t.Errorf("call %d: IngestSync returned %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(counts) != numCalls {
		t.Fatalf("delivered %d distinct items, want %d", len(counts), numCalls)
	}
	for i := 0; i < numCalls; i++ {
		if counts[i] != 1 {
			t.Errorf("index %d delivered %d times, want 1", i, counts[i])
		}
	}
	// 200 calls must coalesce into substantially fewer requests; assert a loose
	// bound to avoid flakiness while still proving batching happened.
	if n := requests.Load(); n >= numCalls {
		t.Errorf("server saw %d requests for %d calls, want batching (fewer)", n, numCalls)
	}
}

// Core: IngestBatch returns immediately with a receipt that resolves to nil once
// the server accepts the data.
func TestIngestBatch_ReceiptResolvesOnSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(10)
	c.SetMaxDelay(25 * time.Millisecond)
	defer c.Close()

	r := c.IngestBatch(
		map[string]any{"index": 0},
		map[string]any{"index": 1},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Wait(ctx); err != nil {
		t.Fatalf("receipt Wait returned %v, want nil", err)
	}
}

// Core: when the server never accepts and the client is closed, buffered tracked
// items are settled with ReasonClosed (not left hanging) once the close-time
// retries are exhausted.
func TestIngestBatch_ClosedWhileServerFailsSettlesClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000)                      // never reached
	c.SetMaxDelay(10 * time.Second)           // never fires before Close
	c.SetCloseTimeout(200 * time.Millisecond) // give up quickly for the test
	r := c.IngestBatch(map[string]any{"index": 0})

	// Close drains the buffer, retries the final flush until the close timeout,
	// then gives up and settles the remaining items.
	c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := r.Wait(ctx)
	var ie *quickwit.IngestError
	if !errors.As(err, &ie) || ie.Reason != quickwit.ReasonClosed {
		t.Fatalf("receipt err = %v, want *IngestError{ReasonClosed}", err)
	}
}

// Core: a tracked item flushes as soon as the worker is idle, so IngestSync
// returns well before maxDelay even when the batch size is never reached.
func TestIngestSync_FlushesOnIdleBeforeMaxDelay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000)            // size trigger never fires
	c.SetMaxDelay(30 * time.Second) // ticker must not be what flushes it
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := c.IngestSync(ctx, map[string]any{"index": 0})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("IngestSync took %v, want well under maxDelay (idle flush)", elapsed)
	}
}

// Core: fire-and-forget Ingest is unaffected by idle flush — a partial batch
// still waits for the timer rather than flushing immediately.
func TestIngest_FireAndForgetDoesNotIdleFlush(t *testing.T) {
	const maxDelay = 400 * time.Millisecond

	arrived := make(chan time.Duration, 1)
	start := time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case arrived <- time.Since(start):
		default:
		}
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000) // never reached
	c.SetMaxDelay(maxDelay)
	defer c.Close()

	start = time.Now()
	c.Ingest(
		map[string]any{"index": 0},
		map[string]any{"index": 1},
	)

	select {
	case d := <-arrived:
		// Must have waited for the timer, not flushed on idle. Use a margin
		// below maxDelay to stay robust against scheduling jitter.
		if d < maxDelay/2 {
			t.Errorf("fire-and-forget flush arrived after %v, want it to wait ~%v for the timer", d, maxDelay)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no request arrived before timeout")
	}
}

// Core: even with idle flush active, a tracked burst still coalesces and every
// record is delivered exactly once, in order.
func TestIngestSync_IdleFlushPreservesDeliveryUnderBurst(t *testing.T) {
	const numItems = 300

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

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1) // single worker => global order is observable
	c.SetBatchSize(50)
	c.SetMaxDelay(30 * time.Second)
	defer c.Close()

	// One call enqueues the whole burst, so the drain coalesces it into batches.
	data := make([]any, numItems)
	for i := range data {
		data[i] = map[string]any{"index": i}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.IngestSync(ctx, data...); err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != numItems {
		t.Fatalf("received %d items, want %d", len(received), numItems)
	}
	for i, idx := range received {
		if idx != i {
			t.Errorf("position %d: got index %d, want %d", i, idx, i)
		}
	}
}

// Core: IngestSync with no data is a no-op that returns nil even before setup.
func TestIngestSync_EmptyIsNoop(t *testing.T) {
	c := quickwit.NewClient("http://example/api/v1/test")
	if err := c.IngestSync(context.Background()); err != nil {
		t.Errorf("IngestSync() with no data returned %v, want nil", err)
	}
}

// Core: an already-cancelled context returns a pending error without ingesting.
func TestIngestSync_PreCancelledContext(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.IngestSync(ctx, map[string]any{"index": 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
}
