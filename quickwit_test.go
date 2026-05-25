package quickwit_test

import (
	"bytes"
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

func TestClient(t *testing.T) {
	c := quickwit.NewClient("http://localhost:7280/api/v1/test")
	c.Ingest(map[string]any{
		"s": "test",
		"i": 0,
		"t": time.Now().Format(time.RFC3339),
	})
	c.Close()
}

// Regression #7: Close() panicked when called before any Ingest.
func TestClose_BeforeIngest(t *testing.T) {
	c := quickwit.NewClient("http://localhost:7280/api/v1/test")
	c.Close()
}

// Regression #7: subsequent Close() calls panicked due to closing already-closed channels.
func TestClose_Idempotent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.Ingest(map[string]any{"key": "value"})
	c.Close()
	c.Close()
	c.Close()
}

// Regression #8: json.Encoder already appends a newline per record; an extra manual newline
// produced blank lines between NDJSON records, making the payload invalid.
func TestIngest_NDJSONNoDoubleNewline(t *testing.T) {
	var mu sync.Mutex
	var capturedBodies [][]byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		capturedBodies = append(capturedBodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetBatchSize(100)
	c.SetMaxDelay(10 * time.Second)
	c.SetConcurrent(1)
	c.Ingest(
		map[string]any{"index": 0},
		map[string]any{"index": 1},
		map[string]any{"index": 2},
	)
	c.Close()

	mu.Lock()
	bodies := capturedBodies
	mu.Unlock()

	if len(bodies) == 0 {
		t.Fatal("no HTTP requests received")
	}
	for _, body := range bodies {
		if bytes.Contains(body, []byte("\n\n")) {
			t.Errorf("NDJSON body contains double newlines:\n%q", body)
		}
	}
	combined := bytes.Join(bodies, nil)
	lines := strings.Split(strings.TrimRight(string(combined), "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines total, got %d:\n%q", len(lines), combined)
	}
}

// Regression #9: JSON encode errors were silently ignored; the unencodable record was never
// forwarded to the OnDiscard callback, so callers had no visibility into data loss.
func TestIngest_JSONEncodeError_CallsOnDiscard(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	var mu sync.Mutex
	var discarded []any

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.OnDiscard(func(data any) {
		mu.Lock()
		discarded = append(discarded, data)
		mu.Unlock()
	})

	ch := make(chan int) // channels are not JSON-encodable
	c.Ingest(map[string]any{"before": true}, ch, map[string]any{"after": true})
	c.Close()

	mu.Lock()
	n := len(discarded)
	mu.Unlock()

	if n != 1 {
		t.Errorf("OnDiscard called %d times, want 1", n)
	}
}

// Regression #11: unencodable items caused OnDiscard to fire on every retry when the HTTP
// request also failed — the callback must fire exactly once per item.
func TestIngest_EncodeError_DiscardedExactlyOnce_OnHTTPFailure(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	var mu sync.Mutex
	var discarded []any

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.OnDiscard(func(data any) {
		mu.Lock()
		discarded = append(discarded, data)
		mu.Unlock()
	})

	ch := make(chan int) // not JSON-encodable
	c.Ingest(map[string]any{"ok": true}, ch)
	c.Close()

	mu.Lock()
	n := len(discarded)
	mu.Unlock()

	if n != 1 {
		t.Errorf("OnDiscard called %d times, want exactly 1 (got %d HTTP attempts)", n, attempts)
	}
}

// Regression #12: when all items in a batch fail to encode, flush sent an empty HTTP body.
// If the server rejects an empty body, retryFlush looped indefinitely re-discarding items.
func TestIngest_AllEncodeErrors_NoHTTPRequest(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusBadRequest) // server rejects empty body
	}))
	defer server.Close()

	var mu sync.Mutex
	var discarded []any

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.OnDiscard(func(data any) {
		mu.Lock()
		discarded = append(discarded, data)
		mu.Unlock()
	})

	ch1 := make(chan int)
	ch2 := make(chan int)
	c.Ingest(ch1, ch2)
	c.Close()

	mu.Lock()
	n := len(discarded)
	mu.Unlock()

	if requestCount != 0 {
		t.Errorf("expected no HTTP requests when all items are unencodable, got %d", requestCount)
	}
	if n != 2 {
		t.Errorf("OnDiscard called %d times, want 2", n)
	}
}

// Regression #10: when a 413 triggered batch-size reduction, re-flushing the oversized
// buffer in smaller chunks silently reversed or lost the tail of the record sequence.
func TestIngest_OversizeBatchPreservesOrder(t *testing.T) {
	const batchSize = 5
	const numItems = batchSize

	var mu sync.Mutex
	requestCount := 0
	var received []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		requestCount++
		if requestCount == 1 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if line == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				continue
			}
			received = append(received, int(m["index"].(float64)))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetAutoReduceBatchSize(true)
	c.SetBatchSize(batchSize)
	c.SetConcurrent(1)

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
