package quickwit

import (
	"net/http"
	"testing"
	"time"
)

// The default transport bounds the wait for response headers to a third of the
// ingest timeout, so a server that stalls before responding frees the worker
// early instead of holding it for the full deadline.
func TestNewDefaultClient_ResponseHeaderTimeout(t *testing.T) {
	c := NewClient("http://example/api/v1/test")
	c.SetIngestTimeout(30 * time.Second)

	tr, ok := c.httpClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", c.httpClient().Transport)
	}
	if want := 10 * time.Second; tr.ResponseHeaderTimeout != want {
		t.Errorf("ResponseHeaderTimeout = %v, want %v (ingestTimeout/3)", tr.ResponseHeaderTimeout, want)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	httpDate := now.Add(5 * time.Second).UTC().Format(http.TimeFormat)
	pastDate := now.Add(-5 * time.Second).UTC().Format(http.TimeFormat)

	cases := []struct {
		name   string
		header string
		want   time.Duration
		ok     bool
	}{
		{"seconds", "2", 2 * time.Second, true},
		{"seconds with space", "  3 ", 3 * time.Second, true},
		{"http date future", httpDate, 5 * time.Second, true},
		{"http date past", pastDate, 0, false},
		{"empty", "", 0, false},
		{"garbage", "soon", 0, false},
		{"zero", "0", 0, false},
		{"negative", "-1", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseRetryAfter(tc.header, now)
			if ok != tc.ok || got != tc.want {
				t.Errorf("parseRetryAfter(%q) = (%v, %v), want (%v, %v)", tc.header, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestJitterBackoff(t *testing.T) {
	if got := jitterBackoff(0); got != 0 {
		t.Errorf("jitterBackoff(0) = %v, want 0", got)
	}
	const d = 100 * time.Millisecond
	for i := 0; i < 1000; i++ {
		got := jitterBackoff(d)
		if got < d/2 || got > d {
			t.Fatalf("jitterBackoff(%v) = %v, want within [%v, %v]", d, got, d/2, d)
		}
	}
}
