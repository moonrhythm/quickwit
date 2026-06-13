package quickwit_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

// Core: the close-flush window rides out a transient failure during shutdown.
// A fire-and-forget record is used so the FIRST flush happens during Close (a
// tracked item would idle-flush earlier and be retried by the normal path, not
// the close path). The server 5xxs the first attempts, then recovers within the
// window, and the record must be delivered (not dropped).
func TestClose_RidesOutTransientFailure(t *testing.T) {
	var attempts, delivered atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if attempts.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError) // transient blip
			return
		}
		delivered.Add(1)
		io.WriteString(w, `{"num_ingested_docs":1,"num_rejected_docs":0}`)
	}))
	defer server.Close()

	var discarded atomic.Int64
	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000)            // never reached: flushed only at Close
	c.SetMaxDelay(10 * time.Second) // never fires before Close
	c.SetCloseTimeout(3 * time.Second)
	c.OnDiscard(func(any) { discarded.Add(1) })

	c.Ingest(map[string]any{"index": 0})
	c.Close()

	if delivered.Load() != 1 {
		t.Errorf("delivered = %d, want 1 — Close should ride out the blip", delivered.Load())
	}
	if discarded.Load() != 0 {
		t.Errorf("discarded = %d, want 0", discarded.Load())
	}
}

// Core: Close gives up roughly at the close timeout when the server stays down,
// discarding the buffered record rather than blocking forever or using the old
// fixed ~0.8s budget. Fire-and-forget keeps the flush on the close path.
func TestClose_GivesUpAtTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	var discarded atomic.Int64
	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1000)
	c.SetMaxDelay(10 * time.Second)
	c.SetCloseTimeout(400 * time.Millisecond)
	c.OnDiscard(func(any) { discarded.Add(1) })

	c.Ingest(map[string]any{"index": 0})

	start := time.Now()
	c.Close()
	elapsed := time.Since(start)

	// Bounded by the retry window (plus fast in-flight 500s); not unbounded.
	if elapsed > 2*time.Second {
		t.Errorf("Close took %v, want bounded near the 400ms close timeout", elapsed)
	}
	if discarded.Load() != 1 {
		t.Errorf("OnDiscard fired %d times, want 1 (record dropped after the window)", discarded.Load())
	}
}
