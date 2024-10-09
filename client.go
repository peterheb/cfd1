/*
Package cfd1 provides a client and database/sql driver for interacting with
Cloudflare's D1 database service.

D1 is a serverless SQL database from Cloudflare that implements a
SQLite-compatible query engine. This package offers two ways to interact with D1
databases:

 1. A direct implementation of the Cloudflare API
 2. A [database/sql] compatible driver

# Direct API Usage

To use the direct API implementation, create a new client using [NewClient] with
your Cloudflare account ID and API token:

	client := cfd1.NewClient("your-account-id", "your-api-token")

You can then use this client to create, manage, and query D1 databases. The D1
API supports multiple statements in one [Query] operation, which are executed as
a batch.

# database/sql Driver Usage

To use the [database/sql] driver, import this library with the blank identifier
so that its init function registers the driver:

	import (
	    database/sql
	    _ "github.com/peterheb/cfd1"
	)

Uou can then open a connection to a D1 database using a DSN string in URI
format:

	db, err := sql.Open("cfd1",
	    "d1://your-account-id:your-api-token@database-uuid")

Note that this driver does not support transactions through db.Begin(), as
connections to D1 over the REST API are not persistent -- every query is a new
round-trip and connection. Multiple semicolon- separated statements in a single
query are supported, however, and can include transactions.

# Disclaimer

This is an unofficial implementation of the Cloudflare D1 API, and its author is
not affiliated with Cloudflare. For the official Cloudflare API Go client, see:

	https://github.com/cloudflare/cloudflare-go

For more information about Cloudflare D1, see the [Cloudflare D1 documentation].

[Cloudflare D1 documentation]: https://developers.cloudflare.com/d1/
*/
package cfd1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultCloudflareBaseURL = "https://api.cloudflare.com/client/v4"
	defaultHttpTimeout       = 30 * time.Second
	defaultIdleConnTimeout   = 90 * time.Second
	defaultMaxIdleConns      = 100
)

// CFD1Client defines the interface for interacting with a CFD1 database. It
// provides methods for database operations, querying, and performance
// monitoring. This interface can be used to create mock implementations for
// testing purposes.
type CFD1Client interface {
	CreateDatabase(ctx context.Context, name string, primaryLocationHint LocationHint) (*DatabaseDetails, error)
	DeleteDatabase(ctx context.Context, databaseID string) error
	GetDatabase(ctx context.Context, databaseID string) (*DatabaseDetails, error)
	ListDatabases(ctx context.Context, name string) ([]DatabaseDetails, error)
	Query(ctx context.Context, databaseID, sql string, params ...any) (*QueryResult, error)
	RawQuery(ctx context.Context, databaseID, sql string, params ...any) (*RawQueryResult, error)
	ResetCounters()
	RowsRead() int
	RowsWritten() int
}

// Client interacts with the Cloudflare D1 API. It provides methods for managing
// databases and executing queries. The client keeps track of rows read and
// written across all operations, which can be useful for cost monitoring and
// optimization.
type Client struct {
	accountID   string
	apiToken    string
	baseURL     string
	httpClient  *http.Client
	rowsRead    int
	rowsWritten int
	mux         sync.RWMutex
}

// ClientOption is a function type for configuring a Client.
type ClientOption func(*Client)

// apiResponse represents the structure of the API response.
type apiResponse struct {
	Result     json.RawMessage `json:"result"`
	Success    bool            `json:"success"`
	Errors     []D1Error       `json:"errors"`
	ResultInfo apiResponseInfo `json:"result_info"`
}

// apiResponseInfo contains metadata about a paginated API response.
type apiResponseInfo struct {
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	Count      int `json:"count"`
	TotalCount int `json:"total_count"`
}

// WithEndpoint sets a custom endpoint URL for the D1 client. The default
// endpoint is "https://api.cloudflare.com/client/v4".
func WithEndpoint(url string) ClientOption {
	return func(c *Client) {
		c.baseURL = strings.TrimSuffix(url, "/")
	}
}

// WithHTTPClient sets a custom HTTP client for the D1 client. The default
// client uses a 30 second timeout and maintains up to 100 max idle connections,
// with a 90 second idle timeout. This option can be used to configure custom
// timeouts, transport settings, or other client options.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithDebugLogger enables debug logging for the D1 client. The provided logger
// is given copies of HTTP request and response bodies exchanged with the
// Cloudflare D1 API for logging and inspection.
func WithDebugLogger(logger DebugLogger) ClientOption {
	return func(c *Client) {
		transport := c.httpClient.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		c.httpClient.Transport = &debugTransport{
			transport: transport,
			logger:    logger,
		}
	}
}

// NewClient returns a new D1 client using the provided account ID and API
// token. Use ClientOption functions to configure the client.
func NewClient(accountID string, apiToken string, options ...ClientOption) *Client {
	c := &Client{
		accountID:  accountID,
		apiToken:   apiToken,
		baseURL:    defaultCloudflareBaseURL,
		httpClient: defaultHTTPClient(),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// defaultHTTPClient returns a http.Client with reasonable defaults for a
// database client.
func defaultHTTPClient() *http.Client {
	return &http.Client{
		Timeout: defaultHttpTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        defaultMaxIdleConns,
			MaxIdleConnsPerHost: defaultMaxIdleConns, // host stays the same
			IdleConnTimeout:     defaultIdleConnTimeout,
		},
	}
}

// RowsRead returns the number of rows read since client creation, or the last
// reset.
func (c *Client) RowsRead() int {
	c.mux.RLock()
	defer c.mux.Unlock()
	return c.rowsRead
}

// RowsWritten returns the number of rows written since client creation, or the
// last reset.
func (c *Client) RowsWritten() int {
	c.mux.RLock()
	defer c.mux.Unlock()
	return c.rowsWritten
}

// ResetCounters resets the client's internal row counters to zero.
func (c *Client) ResetCounters() {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.rowsRead = 0
	c.rowsWritten = 0
}

// sendRequest sends an HTTP request to the Cloudflare API and processes the
// response.
func (c *Client) sendRequest(ctx context.Context, method, path string, body any, v any, pagInfo *apiResponseInfo) error {
	url := fmt.Sprintf("%s/accounts/%s/d1/%s", c.baseURL, c.accountID, strings.TrimPrefix(path, "/"))

	var reqBytes []byte
	var err error
	if body != nil {
		if reqBytes, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBytes))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiToken)
	} else {
		// This library doesn't support using an email + API key.
		return fmt.Errorf("no API token provided")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	var apiResp apiResponse
	if err := json.Unmarshal(responseBody, &apiResp); err != nil {
		return fmt.Errorf("decoding response: %w\n%s", err, string(responseBody))
	}

	if !apiResp.Success {
		if len(apiResp.Errors) > 0 {
			return &apiResp.Errors[0]
		}
		return fmt.Errorf("API request failed without specific error")
	}

	if pagInfo != nil {
		*pagInfo = apiResp.ResultInfo
	}

	if v != nil {
		if err := json.Unmarshal(apiResp.Result, v); err != nil {
			return fmt.Errorf("decoding JSON result: %w", err)
		}
		if qr, ok := v.(*QueryResult); ok { // Update counters for queries
			c.mux.Lock()
			defer c.mux.Unlock()
			c.rowsRead += qr.Meta.RowsRead
			c.rowsWritten += qr.Meta.RowsWritten
		}
	}

	return nil
}
