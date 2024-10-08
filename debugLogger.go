package cfd1

import (
	"bytes"
	"io"
	"net/http"
)

// DebugLogger is an interface for logging HTTP request debug information.
// Implement this interface and pass it to WithDebugLogger when creating a new
// Client.
type DebugLogger interface {
	LogRequest(method string, url string, requestBody, responseBody []byte, statusCode int)
}

// debugTransport is an http.RoundTripper that captures request and response data
type debugTransport struct {
	transport http.RoundTripper
	logger    DebugLogger
}

// RoundTrip executes an HTTP request and captures request and response data.
func (d *debugTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reqBody, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(bytes.NewBuffer(reqBody))

	resp, err := d.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))

	d.logger.LogRequest(req.Method, req.URL.String(), reqBody, respBody, resp.StatusCode)
	return resp, nil
}
