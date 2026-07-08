package quickwit_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

type capturingHandler struct {
	mu   *sync.Mutex
	recs *[]slog.Record
}

func (h capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, r.Clone())
	return nil
}
func (h capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h capturingHandler) WithGroup(string) slog.Handler       { return h }

// The "flush failed, retrying indefinitely" line must carry the failure cause so
// an operator can tell a 5xx/backpressure apart from a transport error.
func TestFlushFailure_RetryLogIncludesCause(t *testing.T) {
	var mu sync.Mutex
	var recs []slog.Record
	prev := slog.Default()
	slog.SetDefault(slog.New(capturingHandler{mu: &mu, recs: &recs}))
	defer slog.SetDefault(prev)

	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError) // transient: two 500s
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

	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, r := range recs {
		if r.Message != "quickwit: flush failed, retrying indefinitely" {
			continue
		}
		found = true
		var cause string
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "error" {
				cause = a.Value.String()
				return false
			}
			return true
		})
		if cause == "" || cause == "<nil>" {
			t.Errorf("retry log has no error cause, want the 5xx status; record=%+v", r)
			continue
		}
		if !strings.Contains(cause, "500") {
			t.Errorf("retry log error = %q, want it to name the 500 status", cause)
		}
	}
	if !found {
		t.Fatal("did not capture a 'flush failed, retrying indefinitely' log line")
	}
}
