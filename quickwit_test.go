package quickwit_test

import (
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
