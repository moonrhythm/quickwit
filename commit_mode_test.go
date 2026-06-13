package quickwit_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/moonrhythm/quickwit"
)

// queryCapture records the query string of the first ingest request.
func queryCapture() (http.HandlerFunc, func() string) {
	var mu sync.Mutex
	var q string
	var got bool
	h := func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if !got {
			q = r.URL.RawQuery
			got = true
		}
		mu.Unlock()
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, `{"num_ingested_docs":1,"num_rejected_docs":0}`)
	}
	return h, func() string { mu.Lock(); defer mu.Unlock(); return q }
}

// Core: by default no commit parameter is sent (commit=auto behavior).
func TestIngest_CommitAutoByDefault(t *testing.T) {
	h, query := queryCapture()
	server := httptest.NewServer(h)
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	c.Ingest(map[string]any{"index": 0})
	c.Close()

	if q := query(); q != "" {
		t.Errorf("query = %q, want empty (no commit param by default)", q)
	}
}

// Core: a tracked batch carries the configured commit mode.
func TestIngestSync_CommitWaitForOnTracked(t *testing.T) {
	h, query := queryCapture()
	server := httptest.NewServer(h)
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetIngestCommit(quickwit.CommitWaitFor)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.IngestSync(ctx, map[string]any{"index": 0}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}

	if q := query(); !hasParam(q, "commit", "wait_for") {
		t.Errorf("query = %q, want commit=wait_for", q)
	}
}

// Core: a fire-and-forget batch keeps commit=auto even when a tracked commit
// mode is configured — it carries no tracked item, so it must not pay the cost.
func TestIngest_FireAndForgetUsesAutoWithWaitForSet(t *testing.T) {
	h, query := queryCapture()
	server := httptest.NewServer(h)
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetBatchSize(1)
	c.SetIngestCommit(quickwit.CommitWaitFor)
	c.Ingest(map[string]any{"index": 0}) // fire-and-forget => no tracked item
	c.Close()

	if q := query(); hasParam(q, "commit", "wait_for") {
		t.Errorf("query = %q, want no commit param for a fire-and-forget batch", q)
	}
}

// Core: commit and detailed_response combine into one well-formed query.
func TestIngestSync_CommitAndDetailedResponseCombine(t *testing.T) {
	h, query := queryCapture()
	server := httptest.NewServer(h)
	defer server.Close()

	c := quickwit.NewClient(server.URL + "/api/v1/test")
	c.SetConcurrent(1)
	c.SetIngestCommit(quickwit.CommitForce)
	c.SetIngestDetailedResponse(true)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.IngestSync(ctx, map[string]any{"index": 0}); err != nil {
		t.Fatalf("IngestSync returned %v, want nil", err)
	}

	q := query()
	if !hasParam(q, "commit", "force") {
		t.Errorf("query = %q, want commit=force", q)
	}
	if !hasParam(q, "detailed_response", "true") {
		t.Errorf("query = %q, want detailed_response=true", q)
	}
}

func hasParam(rawQuery, key, val string) bool {
	v, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	return v.Get(key) == val
}
