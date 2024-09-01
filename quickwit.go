package quickwit

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	IngestBufferSize = 1000
	IngestBatchSize  = 1000
	IngestMaxDelay   = time.Second
)

type Client struct {
	*http.Client

	auth      func(req *http.Request)
	BatchSize int
	MaxDelay  time.Duration

	endpoint     string // http://{host}/api/v1/{index_name}
	ingestBuffer chan any
	onceSetup    sync.Once
}

func NewClient(endpoint string, ingestBufferSize int) *Client {
	if ingestBufferSize <= 0 {
		ingestBufferSize = IngestBufferSize
	}
	return &Client{
		endpoint:     endpoint,
		ingestBuffer: make(chan any, ingestBufferSize),
	}
}

func (c *Client) SetAuth(auth func(req *http.Request)) {
	c.auth = auth
}

func (c *Client) httpClient() *http.Client {
	if c.Client == nil {
		return http.DefaultClient
	}
	return c.Client
}

func (c *Client) maxDelay() time.Duration {
	if c.MaxDelay <= 0 {
		return IngestMaxDelay
	}
	return c.MaxDelay
}

func (c *Client) batchSize() int {
	if c.BatchSize <= 0 {
		return IngestBatchSize
	}
	return c.BatchSize
}

func (c *Client) doAuth(req *http.Request) {
	if c.auth != nil {
		c.auth(req)
	}
}

// Ingest sends data to the quickwit server.
// The data can be any type, and will be marshalled to JSON.
// The data will be buffered until the buffer is full, then sent to the server.
// If the buffer is full, Ingest will block until the buffer is no longer full.
func (c *Client) Ingest(data ...any) {
	c.onceSetup.Do(c.setupLoop)
	for _, x := range data {
		c.ingestBuffer <- x
	}
}

func (c *Client) Close() {
	close(c.ingestBuffer)
}

func (c *Client) setupLoop() {
	go c.loop()
}

func (c *Client) loop() {
	var buf bytes.Buffer
	jsonEnc := json.NewEncoder(&buf)

	batchSize := c.batchSize()
	buffer := make([]any, 0, batchSize)

	endpoint := c.endpoint
	endpoint = strings.TrimSuffix(endpoint, "/")
	endpoint = endpoint + "/ingest"

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		buf.Reset()

		req, err := http.NewRequest(http.MethodPost, endpoint, &buf)
		if err != nil {
			panic(err)
			return
		}
		c.doAuth(req)

		for _, x := range buffer {
			jsonEnc.Encode(x)
			buf.WriteString("\n")
		}

		resp, err := c.httpClient().Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			slog.Error("quickwit: ingest status not ok", "status", resp.Status)
			return
		}

		buffer = buffer[:0]
	}

	ticker := time.NewTicker(c.maxDelay())

	go func() {
		for {
			select {
			case <-ticker.C:
				flush()
			case x, ok := <-c.ingestBuffer:
				if !ok { // channel closed
					flush()
					return
				}
				buffer = append(buffer, x)
				if len(buffer) >= batchSize {
					flush()
				}
			}
		}
	}()
}
