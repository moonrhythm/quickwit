package quickwit_test

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

// parseIndices extracts the "index" field of every NDJSON line in an ingest body.
func parseIndices(body []byte) []int {
	var out []int
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if v, ok := m["index"].(float64); ok {
			out = append(out, int(v))
		}
	}
	return out
}

// Core: every ingested record reaches the server exactly once, in order
// (single worker), across multiple batches.
func TestIngest_DeliversAllItemsInOrder(t *testing.T) {
	const numItems = 250

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
	c.SetBatchSize(50)

	for i := 0; i < numItems; i++ {
		c.Ingest(map[string]any{"index": i})
	}
	c.Close()

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

// Core: reaching the configured batch size flushes immediately, without waiting
// for the timer. maxDelay is set far in the future so only the size trigger can fire.
func TestIngest_FlushTriggeredByBatchSize(t *testing.T) {
	const batchSize = 5

	gotLines := make(chan int, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotLines <- len(parseIndices(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(batchSize)
	c.SetMaxDelay(10 * time.Second) // timer must not be the cause
	defer c.Close()

	for i := 0; i < batchSize; i++ {
		c.Ingest(map[string]any{"index": i})
	}

	select {
	case n := <-gotLines:
		if n != batchSize {
			t.Errorf("flushed %d records, want %d", n, batchSize)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch-size flush did not occur before timeout")
	}
}

// Core: a partial batch flushes once the timer fires, even though the batch size
// is never reached.
func TestIngest_FlushTriggeredByTimer(t *testing.T) {
	gotReq := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		gotReq <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000) // size trigger must not fire
	c.SetMaxDelay(50 * time.Millisecond)
	defer c.Close()

	c.Ingest(
		map[string]any{"index": 0},
		map[string]any{"index": 1},
		map[string]any{"index": 2},
	)

	select {
	case <-gotReq:
	case <-time.After(2 * time.Second):
		t.Fatal("timer flush did not occur before timeout")
	}
}

// Core: Close() flushes whatever remains buffered, even when neither the size nor
// timer trigger has fired.
func TestIngest_FlushRemainingOnClose(t *testing.T) {
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
	c.SetBatchSize(1000)            // never reached
	c.SetMaxDelay(10 * time.Second) // never fires before Close
	c.Ingest(
		map[string]any{"index": 0},
		map[string]any{"index": 1},
		map[string]any{"index": 2},
	)
	c.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("received %d items after Close, want 3", len(received))
	}
	for i, idx := range received {
		if idx != i {
			t.Errorf("position %d: got index %d, want %d", i, idx, i)
		}
	}
}

// Core: a failing flush retries (during normal operation, indefinitely) until the
// server accepts it; the record must not be lost.
func TestIngest_RetriesFailedFlushUntilSuccess(t *testing.T) {
	var mu sync.Mutex
	var attempts int
	var delivered []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		attempts++
		a := attempts
		if a >= 3 {
			delivered = append(delivered, parseIndices(body)...)
		}
		mu.Unlock()
		if a < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1) // each item triggers a flush
	c.Ingest(map[string]any{"index": 42})
	c.Close()

	mu.Lock()
	defer mu.Unlock()
	if attempts < 3 {
		t.Fatalf("server saw %d attempts, want at least 3 (2 failures then success)", attempts)
	}
	if len(delivered) != 1 || delivered[0] != 42 {
		t.Errorf("delivered = %v, want [42]", delivered)
	}
}

// Core: the user-supplied auth hook is applied to every ingest request.
func TestIngest_AppliesAuthHook(t *testing.T) {
	var mu sync.Mutex
	var gotAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetAuth(func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer test-token")
	})
	c.Ingest(map[string]any{"index": 0})
	c.Close()

	mu.Lock()
	defer mu.Unlock()
	if gotAuth != "Bearer test-token" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer test-token")
	}
}

// Core: with gzip enabled, the request carries Content-Encoding: gzip and the
// decompressed body still delivers every record in order.
func TestIngest_GzipCompressesBody(t *testing.T) {
	const numItems = 250

	var mu sync.Mutex
	var received []int
	sawGzip := true // require every request to be gzip-encoded

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "gzip" {
			mu.Lock()
			sawGzip = false
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gr, err := gzip.NewReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(gr)
		gr.Close()

		mu.Lock()
		received = append(received, parseIndices(body)...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(50)
	c.SetGzip(true)

	for i := 0; i < numItems; i++ {
		c.Ingest(map[string]any{"index": i})
	}
	c.Close()

	mu.Lock()
	defer mu.Unlock()

	if !sawGzip {
		t.Fatal("a request arrived without Content-Encoding: gzip")
	}
	if len(received) != numItems {
		t.Fatalf("received %d items, want %d", len(received), numItems)
	}
	for i, idx := range received {
		if idx != i {
			t.Errorf("position %d: got index %d, want %d", i, idx, i)
		}
	}
}

// Core: with multiple concurrent workers, every record is delivered exactly once
// (no loss, no duplication).
func TestIngest_ConcurrentWorkersDeliverEachItemOnce(t *testing.T) {
	const numItems = 1000

	var mu sync.Mutex
	counts := make(map[int]int)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		idxs := parseIndices(body)
		mu.Lock()
		for _, idx := range idxs {
			counts[idx]++
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(4)
	c.SetBatchSize(10)
	for i := 0; i < numItems; i++ {
		c.Ingest(map[string]any{"index": i})
	}
	c.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(counts) != numItems {
		t.Fatalf("delivered %d distinct items, want %d", len(counts), numItems)
	}
	for i := 0; i < numItems; i++ {
		if counts[i] != 1 {
			t.Errorf("index %d delivered %d times, want 1", i, counts[i])
		}
	}
}

// Core: in discard mode, items that arrive while the buffer is full are dropped
// and surfaced through OnDiscard instead of blocking the caller. The server is held
// so the single worker stays blocked mid-flush and cannot drain the buffer.
func TestIngest_DiscardsWhenBufferFull(t *testing.T) {
	const bufferSize = 2
	const numItems = 200

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // block the worker mid-flush
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	var mu sync.Mutex
	var discarded int

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1) // first item triggers the (blocking) flush
	c.SetIngestBufferSize(bufferSize)
	c.SetDiscard(true)
	c.OnDiscard(func(any) {
		mu.Lock()
		discarded++
		mu.Unlock()
	})

	// Each Ingest does a non-blocking send; OnDiscard fires synchronously here.
	for i := 0; i < numItems; i++ {
		c.Ingest(map[string]any{"index": i})
	}

	mu.Lock()
	n := discarded
	mu.Unlock()

	// At most bufferSize items sit in the channel plus the one the worker pulled
	// before blocking, so the overwhelming majority must have been discarded.
	if n < numItems-bufferSize-2 {
		t.Errorf("discarded %d items, want at least %d", n, numItems-bufferSize-2)
	}

	close(release) // let the worker finish so Close can return
	c.Close()
}

// Core: a 413 response with auto-reduce enabled re-sends the buffer in smaller
// chunks while still delivering every record in order.
func TestIngest_413ReducesBatchSizeAndChunks(t *testing.T) {
	const batchSize = 10
	const numItems = batchSize

	var mu sync.Mutex
	var requestCount int
	var successLineCounts []int
	var received []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requestCount++
		first := requestCount == 1
		if !first {
			idxs := parseIndices(body)
			successLineCounts = append(successLineCounts, len(idxs))
			received = append(received, idxs...)
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
	c.SetBatchSize(batchSize)
	c.SetMaxDelay(10 * time.Second)
	for i := 0; i < numItems; i++ {
		c.Ingest(map[string]any{"index": i})
	}
	c.Close()

	mu.Lock()
	defer mu.Unlock()

	if requestCount < 3 {
		t.Fatalf("requestCount = %d, want at least 3 (one 413, then smaller chunks)", requestCount)
	}
	for _, n := range successLineCounts {
		if n >= batchSize {
			t.Errorf("a post-413 request carried %d records, want fewer than %d", n, batchSize)
		}
	}
	if len(received) != numItems {
		t.Fatalf("received %d items, want %d", len(received), numItems)
	}
	for i, idx := range received {
		if idx != i {
			t.Errorf("position %d: got index %d, want %d", i, idx, i)
		}
	}
}
