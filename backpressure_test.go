package quickwit_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

// Core: a 429 with Retry-After paces the next attempt to the server's request
// instead of the much shorter default backoff, and the record still delivers.
func TestIngest_HonorsRetryAfterOn429(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	var delivered []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		times = append(times, time.Now())
		first := len(times) == 1
		if !first {
			delivered = append(delivered, parseIndices(body)...)
		}
		mu.Unlock()

		if first {
			w.Header().Set("Retry-After", "1") // 1s, well above the 100ms default backoff
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1) // the single item triggers a flush
	defer c.Close()

	c.Ingest(map[string]any{"index": 0})

	// Wait for the retry to land (normal operation, not shutdown) before Close,
	// so the Retry-After sleep is not cut short by the close signal.
	deadline := time.After(4 * time.Second)
	for {
		mu.Lock()
		n := len(times)
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("retry did not arrive before timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()
	gap := times[1].Sub(times[0])
	if gap < 700*time.Millisecond {
		t.Errorf("retry gap = %v, want >= ~1s (Retry-After honored, not the 100ms backoff)", gap)
	}
	if len(delivered) != 1 || delivered[0] != 0 {
		t.Errorf("delivered = %v, want [0]", delivered)
	}
}
