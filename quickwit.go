package quickwit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	IngestConcurrent = 2
)

type OnDiscardFunc func(any)

type Client struct {
	client           *http.Client
	auth             func(req *http.Request)
	endpoint         string // http://{host}/api/v1/{index_name}
	batchSize        int
	maxDelay         time.Duration
	ingestBufferSize int
	discard          bool
	concurrent       int
	ingestBuffer     chan any
	onceSetup        sync.Once
	stopWg           sync.WaitGroup
	onDiscard        OnDiscardFunc
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

func (c *Client) SetConcurrent(concurrent int) {
	c.concurrent = concurrent
}

func (c *Client) OnDiscard(f OnDiscardFunc) {
	c.onDiscard = f
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

func (c *Client) getConcurrent() int {
	if c.concurrent <= 0 {
		return IngestConcurrent
	}
	return c.concurrent
}

func (c *Client) doAuth(req *http.Request) {
	if c.auth != nil {
		c.auth(req)
	}
}

func (c *Client) invokeOnDiscard(data any) {
	if c.onDiscard != nil {
		c.onDiscard(data)
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
				c.invokeOnDiscard(x)
			}
		} else {
			c.ingestBuffer <- x
		}
	}
}

func (c *Client) Close() {
	close(c.ingestBuffer)
	c.stopWg.Wait()
}

func (c *Client) setup() {
	if c.ingestBufferSize <= 0 {
		c.ingestBufferSize = IngestBufferSize
	}
	c.ingestBuffer = make(chan any, c.ingestBufferSize)

	for range c.getConcurrent() {
		go c.loop()
	}
}

func (c *Client) loop() {
	c.stopWg.Add(1)
	defer c.stopWg.Done()

	var buf bytes.Buffer
	jsonEnc := json.NewEncoder(&buf)

	batchSize := c.getBatchSize()
	buffer := make([]any, 0, batchSize)

	endpoint := c.endpoint
	endpoint = strings.TrimSuffix(endpoint, "/")
	endpoint = endpoint + "/ingest"

	flush := func() bool {
		if len(buffer) == 0 {
			return true
		}

		buf.Reset()

		for _, x := range buffer {
			jsonEnc.Encode(x)
			buf.WriteString("\n")
		}

		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(buf.Bytes()))
		if err != nil {
			return false
		}
		c.doAuth(req)

		resp, err := c.httpClient().Do(req)
		if err != nil {
			return false
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			slog.Error("quickwit: ingest status not ok", "status", resp.Status)
			return false
		}

		buffer = buffer[:0]
		return true
	}

	ticker := time.NewTicker(c.getMaxDelay())

	go func() {
		for {
			select {
			case <-ticker.C:
				flush()
			case x, ok := <-c.ingestBuffer:
				if !ok { // channel closed
					if !flush() {
						slog.Error("quickwit: flush failed while closing")
						for _, x := range buffer {
							c.invokeOnDiscard(x)
						}
					}
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

type SearchOpt struct {
	StartTimestamp int64
	EndTimestamp   int64
	StartOffset    int64
	MaxHits        int64
	SearchField    []string
	SnippetFields  []string
	SortBy         []string
	Format         string
}

type SearchResult struct {
	Hits              json.RawMessage
	NumHits           int64
	ElapsedTimeMicros int64
}

type searchRequestQueryString struct {
	Query          string   `json:"query"`
	StartTimestamp *int64   `json:"start_timestamp,omitempty"`
	EndTimestamp   *int64   `json:"end_timestamp,omitempty"`
	StartOffset    *int64   `json:"start_offset,omitempty"`
	MaxHits        *int64   `json:"max_hits,omitempty"`
	SearchField    []string `json:"search_field,omitempty"`
	SnippetFields  []string `json:"snippet_fields,omitempty"`
	SortBy         []string `json:"sort_by,omitempty"`
	Format         *string  `json:"format,omitempty"`
}

type searchResponseRest struct {
	Hits              json.RawMessage `json:"hits"`
	NumHits           int64           `json:"num_hits"`
	ElapsedTimeMicros int64           `json:"elapsed_time_micros"`
}

func (c *Client) Search(ctx context.Context, query string, opt *SearchOpt) (*SearchResult, error) {
	params := searchRequestQueryString{
		Query:  query,
		Format: Ptr("json"),
	}
	if opt != nil {
		params.StartTimestamp = &opt.StartTimestamp
		params.EndTimestamp = &opt.EndTimestamp
		params.StartOffset = &opt.StartOffset
		params.MaxHits = &opt.MaxHits
		params.SearchField = opt.SearchField
		params.SnippetFields = opt.SnippetFields
		params.SortBy = opt.SortBy
		params.Format = &opt.Format
	}

	reqBody, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/search", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	c.doAuth(req)

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("quickwit: search status not ok, code: %d", resp.StatusCode)
	}

	var res searchResponseRest
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &SearchResult{
		Hits:              res.Hits,
		NumHits:           res.NumHits,
		ElapsedTimeMicros: res.ElapsedTimeMicros,
	}, nil
}

func Ptr[T any](t T) *T {
	return &t
}
