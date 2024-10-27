package cfd1

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// ExportOptions represents the options for exporting a D1 database.
type ExportOptions struct {
	NoData   bool     `json:"no_data"`          // Export only table definitions, not contents
	NoSchema bool     `json:"no_schema"`        // Export only table contents, not definitions
	Tables   []string `json:"tables,omitempty"` // Tables to export; if empty, all tables are exported
}

// ExportResponse represents the API response for export operations.
type exportResponse struct {
	Success    bool     `json:"success"`
	AtBookmark string   `json:"at_bookmark"`
	Messages   []string `json:"messages"`
	Error      string   `json:"error"`
	Status     string   `json:"status"`
	Result     *struct {
		Filename  string `json:"filename"`
		SignedURL string `json:"signed_url"`
	} `json:"result,omitempty"`
}

// Export initiates an export (SQL dump) on a D1 database. It accepts the
// database ID and optional [ExportOptions] as parameters. The method waits
// until the export is complete, polling the Cloudflare D1 API as necessary. It
// returns the download URL for the exported database as a string.
//
// The download is a text file of SQL statements suitable to be executed to
// reconstruct the database, and is made available by the underlying API in a
// Cloudflare-owned R2 bucket, via a signed URL, typically for about an hour.
//
// The export process may take some time for larger databases, during which the
// D1 database will be unavailable to serve queries.
//
// [ExportOptions] specifies whether to include data, schema, or both, and an
// optional list of which tables to export. If no tables are specified, all
// tables will be exported. Specifying nil for [ExportOptions] exports
// everything.
//
// Export returns an error if the export process fails or is canceled via the
// context. Note that the underlying API does not provide a mechanism to cancel
// an in-progress export on the API server; a context cancellation will only
// stop the polling loop.
//
// Example usage:
//
//	opts := &d1.ExportOptions{
//	    Tables: []string{"users", "orders"},
//	}
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
//	defer cancel()
//
//	downloadURL, err := client.Export(ctx, "your-account-id", "db-uuid", opts)
//	if err != nil {
//	    // handle error
//	}
//	fmt.Printf("Database export complete. Download URL: %s\n", downloadURL)
func (c *Client) Export(ctx context.Context, databaseID string, opts *ExportOptions) (string, error) {
	path := fmt.Sprintf("/database/%s/export", databaseID)
	if opts == nil {
		opts = &ExportOptions{} // default to export everything
	}
	if opts.NoData && opts.NoSchema {
		return "", newD1Error(99999, "cannot export with both no_data and no_schema")
	}

	body := struct {
		OutputFormat string         `json:"output_format"`
		DumpOptions  *ExportOptions `json:"dump_options"`
	}{
		OutputFormat: "polling",
		DumpOptions:  opts,
	}

	var response exportResponse
	err := c.sendRequest(ctx, http.MethodPost, path, body, &response, nil)
	if err != nil {
		return "", fmt.Errorf("initiating export: %w", err)
	}

	if response.Status == "complete" {
		// Export completed immediately, no polling necessary
		return response.Result.SignedURL, nil
	}

	return c.pollExportStatus(ctx, path, response.AtBookmark)
}

// ExportAsync initiates a D1 database export process asynchronously and calls
// the provided callback function when complete. It uses the same parameters as
// [Export] but returns immediately, with the export continuing in the
// background. The callback function is called with the download URL and any
// error that occurred during the export process.
//
// Example usage:
//
//	// set up ctx and opts if required
//	client.ExportAsync(context.Background(), "your-account-id", "db-uuid", nil,
//	    func(downloadURL string, err error) {
//	        if err != nil {
//	            log.Printf("Export failed: %v", err)
//	            return
//	        }
//	        fmt.Printf("Export completed. Download URL: %s\n", downloadURL)
//	    })
func (c *Client) ExportAsync(ctx context.Context, databaseID string, options *ExportOptions, callback func(string, error)) {
	go func() {
		downloadURL, err := c.Export(ctx, databaseID, options)
		callback(downloadURL, err)
	}()
}

func (c *Client) pollExportStatus(ctx context.Context, path, bookmark string) (string, error) {
	waitTime := time.Second / 4
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			var response exportResponse
			body := map[string]string{
				"output_format":    "polling",
				"current_bookmark": bookmark,
			}
			err := c.sendRequest(ctx, http.MethodPost, path, body, &response, nil)
			if err != nil {
				return "", fmt.Errorf("polling export: %w", err)
			}

			switch response.Status {
			case "active":
				time.Sleep(waitTime) // Wait before polling again
				if waitTime < time.Second {
					waitTime *= 2 // Ramp up from 0.25s, 0.5, to 1s
				}
			case "complete":
				return response.Result.SignedURL, nil
			case "error":
				return "", fmt.Errorf("export failed: %w", newD1Error(99999, response.Error))
			default:
				return "", fmt.Errorf("unknown status: %q", response.Status)
			}
		}
	}
}

// SaveExportToDisk is a helper function that downloads an export from the given
// URL and saves it to the specified location on disk. It returns an error if
// the download fails or the file cannot be written.
func SaveExportToDisk(url, filename string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("http.Get failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("copying data: %w", err)
	}

	return nil
}
