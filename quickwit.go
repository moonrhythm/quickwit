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
	IngestBufferSize = 10000
	IngestBatchSize  = 1000
	IngestMaxDelay   = time.Second
)

type Client struct {
	client           *http.Client
	auth             func(req *http.Request)
	endpoint         string // http://{host}/api/v1/{index_name}
	batchSize        int
	maxDelay         time.Duration
	ingestBufferSize int
	discard          bool
	ingestBuffer     chan any
	onceSetup        sync.Once
}

func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
	}
}

func (c *Client) SetAuth(auth func(req *http.Request)) {
	c.auth = auth
}

func (c *Client) SetHTTPClient(client *http.Client) {
	c.client = client
}

func (c *Client) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

func (c *Client) SetMaxDelay(maxDelay time.Duration) {
	c.maxDelay = maxDelay
}

func (c *Client) SetIngestBufferSize(size int) {
	c.ingestBufferSize = size
}

func (c *Client) SetDiscard(discard bool) {
	c.discard = discard
}

func (c *Client) httpClient() *http.Client {
	if c.client == nil {
		return http.DefaultClient
	}
	return c.client
}

func (c *Client) getMaxDelay() time.Duration {
	if c.maxDelay <= 0 {
		return IngestMaxDelay
	}
	return c.maxDelay
}

func (c *Client) getBatchSize() int {
	if c.batchSize <= 0 {
		return IngestBatchSize
	}
	return c.batchSize
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
	c.onceSetup.Do(c.setup)
	for _, x := range data {
		if c.discard {
			select {
			case c.ingestBuffer <- x:
			default:
			}
		} else {
			c.ingestBuffer <- x
		}
	}
}

func (c *Client) Close() {
	close(c.ingestBuffer)
}

func (c *Client) setup() {
	if c.ingestBufferSize <= 0 {
		c.ingestBufferSize = IngestBufferSize
	}
	c.ingestBuffer = make(chan any, c.ingestBufferSize)
	go c.loop()
}

func (c *Client) loop() {
	var buf bytes.Buffer
	jsonEnc := json.NewEncoder(&buf)

	batchSize := c.getBatchSize()
	buffer := make([]any, 0, batchSize)

	endpoint := c.endpoint
	endpoint = strings.TrimSuffix(endpoint, "/")
	endpoint = endpoint + "/ingest"

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		buf.Reset()

		for _, x := range buffer {
			jsonEnc.Encode(x)
			buf.WriteString("\n")
		}

		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(buf.Bytes()))
		if err != nil {
			panic(err)
			return
		}
		c.doAuth(req)

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

	ticker := time.NewTicker(c.getMaxDelay())

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
