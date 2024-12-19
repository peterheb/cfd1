package cfd1

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// importResponse represents the response from the D1 Import API
type importResponse struct {
	Success    bool   `json:"success"`
	UploadURL  string `json:"upload_url,omitempty"`
	Filename   string `json:"filename,omitempty"`
	AtBookmark string `json:"at_bookmark,omitempty"`
	Status     string `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
	Result     struct {
		FinalBookmark string `json:"final_bookmark,omitempty"`
		NumQueries    int    `json:"num_queries,omitempty"`
		Meta          QueryMeta
	} `json:"result,omitempty"`
	Messages []string `json:"messages,omitempty"`
}

// ImportResult represents the result of a successful import operation
type ImportResult struct {
	NumQueries        int
	RowsRead          int
	RowsWritten       int
	DatabaseSizeBytes int
	FinalBookmark     string
	Duration          time.Duration
}

// Import initiates an import for a D1 database. It accepts the database ID,
// the path to the SQL file to import, and optional ImportOptions as parameters.
// The method waits until the import is complete, polling the Cloudflare D1 API
// if necessary.
//
// The import process may take some time for larger databases, during which the
// D1 database will be unavailable to serve queries.
//
// Import returns an error if the import process fails or is canceled via the
// context. Note that the underlying API does not provide a mechanism to cancel
// an in-progress import on the API server; a context cancellation will only
// stop the polling loop.
//
// Example usage:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
//	defer cancel()
//
//	result, err := client.Import(ctx, "your-database-id", "/path/to/your/file.sql", nil)
//	if err != nil {
//	    // handle error
//	}
//	fmt.Printf("Database import complete. %d queries executed.\n", result.NumQueries)
func (c *Client) Import(ctx context.Context, databaseID, sqlFilePath string) (*ImportResult, error) {
	// Calculate MD5 hash of the file
	fileHash, err := calculateMD5(sqlFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate MD5: %w", err)
	}

	// Initial API call (action: "init")
	path := fmt.Sprintf("/database/%s/import", databaseID)
	initResp, err := c.importInit(ctx, path, fileHash)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize import: %w", err)
	}

	var firstPollResp *importResponse
	if initResp.UploadURL != "" {
		// Upload required
		if err := uploadFileToR2(ctx, initResp.UploadURL, sqlFilePath); err != nil {
			return nil, fmt.Errorf("failed to upload file to R2: %w", err)
		}

		// Start ingestion
		ingestResp, err := c.importIngest(ctx, path, fileHash, initResp.Filename)
		if err != nil {
			return nil, fmt.Errorf("failed to start ingestion: %w", err)
		}
		firstPollResp = ingestResp
	} else {
		// File already uploaded
		firstPollResp = initResp
	}

	// Poll for status updates
	finalResp, err := c.pollImportStatus(ctx, path, firstPollResp)
	if err != nil {
		return nil, err
	}

	c.mux.Lock()
	defer c.mux.Unlock()
	c.rowsRead += finalResp.Result.Meta.RowsRead
	c.rowsWritten += finalResp.Result.Meta.RowsWritten

	return &ImportResult{
		NumQueries:        finalResp.Result.NumQueries,
		RowsRead:          finalResp.Result.Meta.RowsRead,
		RowsWritten:       finalResp.Result.Meta.RowsWritten,
		DatabaseSizeBytes: finalResp.Result.Meta.SizeAfter,
		FinalBookmark:     finalResp.Result.FinalBookmark,
		Duration:          time.Duration(finalResp.Result.Meta.Duration) * time.Millisecond,
	}, nil
}

func (c *Client) importInit(ctx context.Context, path, fileHash string) (*importResponse, error) {
	body := map[string]string{
		"action": "init",
		"etag":   fileHash,
	}

	var response importResponse
	err := c.sendRequest(ctx, http.MethodPost, path, body, &response, nil)
	if err != nil {
		return nil, fmt.Errorf("initiating import: %w", err)
	}

	return &response, nil
}

func uploadFileToR2(ctx context.Context, uploadURL, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, file)
	if err != nil {
		return err
	}
	req.ContentLength = stat.Size()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to upload file, status: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) importIngest(ctx context.Context, path, fileHash, filename string) (*importResponse, error) {
	body := map[string]interface{}{
		"action":   "ingest",
		"etag":     fileHash,
		"filename": filename,
	}

	var response importResponse
	err := c.sendRequest(ctx, http.MethodPost, path, body, &response, nil)
	if err != nil {
		return nil, fmt.Errorf("ingesting import: %w", err)
	}

	return &response, nil
}

func (c *Client) pollImportStatus(ctx context.Context, path string, initialResp *importResponse) (*importResponse, error) {
	resp := initialResp
	waitTime := time.Second / 4

	for {
		switch resp.Status {
		case "active":
			time.Sleep(waitTime) // Wait before polling again
			if waitTime < time.Second {
				waitTime *= 2 // Ramp up from 0.25s, 0.5, to 1s
			}
		case "complete":
			return resp, nil
		case "error":
			return nil, fmt.Errorf("import failed: %s", resp.Error)
		default:
			return nil, fmt.Errorf("unknown status: %q", resp.Status)
		}

		// Poll for updates
		body := map[string]string{
			"action":           "poll",
			"current_bookmark": resp.AtBookmark,
		}

		var newResp importResponse
		err := c.sendRequest(ctx, http.MethodPost, path, body, &newResp, nil)
		if err != nil {
			return nil, fmt.Errorf("polling import: %w", err)
		}

		resp = &newResp
	}
}

func calculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
