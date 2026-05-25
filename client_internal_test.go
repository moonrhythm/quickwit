package quickwit

import (
	"net/http"
	"testing"
)

// The default client's transport must size its idle-connection pool to the
// worker count, so raising SetConcurrent does not get throttled by the stdlib
// default of MaxIdleConnsPerHost = 2.
func TestHTTPClient_DefaultTransportSizedToConcurrent(t *testing.T) {
	const concurrent = 16

	c := NewClient("http://example/api/v1/test")
	c.SetConcurrent(concurrent)

	got := c.httpClient()
	tr, ok := got.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", got.Transport)
	}
	if tr.MaxIdleConnsPerHost != concurrent {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d", tr.MaxIdleConnsPerHost, concurrent)
	}
	if tr.MaxIdleConns < concurrent {
		t.Errorf("MaxIdleConns = %d, want >= %d", tr.MaxIdleConns, concurrent)
	}
}

// The default client is built once and reused, so workers share one connection
// pool rather than each creating its own.
func TestHTTPClient_DefaultClientCached(t *testing.T) {
	c := NewClient("http://example/api/v1/test")

	if c.httpClient() != c.httpClient() {
		t.Error("httpClient returned different default clients across calls")
	}
}

// A user-supplied client is used verbatim, untouched by the default tuning.
func TestHTTPClient_UserClientPassthrough(t *testing.T) {
	custom := &http.Client{}

	c := NewClient("http://example/api/v1/test")
	c.SetHTTPClient(custom)

	if c.httpClient() != custom {
		t.Error("httpClient did not return the user-supplied client")
	}
}
