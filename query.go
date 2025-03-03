package cfd1

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// QueryMeta represents metadata about a database query execution.
type QueryMeta struct {
	ChangedDB   bool    `json:"changed_db"`
	Changes     int     `json:"changes"`
	Duration    float64 `json:"duration"`
	LastRowID   int     `json:"last_row_id"`
	RowsRead    int     `json:"rows_read"`
	RowsWritten int     `json:"rows_written"`
	SizeAfter   int     `json:"size_after"`
}

// QueryResult represents the result of a database query. Each row is returned
// as a map[string]any where the key is the column name.
type QueryResult struct {
	Meta    QueryMeta        `json:"meta"`
	Results []map[string]any `json:"results"`
	Success bool             `json:"success"`
}

// RawQueryResult represents the raw result of a database query. The row values
// and column names are returned in separate structures.
type RawQueryResult struct {
	Meta    QueryMeta `json:"meta"`
	Results struct {
		Columns []string `json:"columns"`
		Rows    [][]any  `json:"rows"`
	} `json:"results"`
	Success bool `json:"success"`
}

func convertTypes(input []any) []any {
	result := make([]any, len(input))

	for i, v := range input {
		switch val := v.(type) {
		case time.Time:
			if val.IsZero() {
				result[i] = 0
				continue
			}
			result[i] = int(val.UTC().Unix())
		case bool:
			if val {
				result[i] = 1
			} else {
				result[i] = 0
			}
		default:
			result[i] = v
		}
	}

	return result
}

// Query executes a SQL query on the specified database and returns the results.
// Each row is returned as a map[string]any, where the key is the column name.
// Parameterized queries are supported to prevent SQL injection.
//
// Returns a [QueryResult] containing the query results and metadata.
func (c *Client) Query(ctx context.Context, databaseID, sql string, params ...any) (*QueryResult, error) {
	p2 := convertTypes(params)
	body := map[string]any{
		"sql":    sql,
		"params": convertTypes(p2),
	}
	var result []QueryResult
	err := c.sendRequest(ctx, http.MethodPost, fmt.Sprintf("/database/%s/query", databaseID), body, &result, nil)
	if err != nil {
		return nil, convertSQLiteError(err, sql, p2)
	}
	return &result[0], nil
}

// RawQuery executes a SQL query and returns results in raw format. Returns a
// [RawQueryResult] containing the query results and metadata. This is useful
// for more control over result processing or for large result sets.
//
// Example usage:
//
//	result, err := client.RawQuery(ctx, "database-uuid", "SELECT id, name FROM users WHERE age > ?", 30)
//	if err != nil {
//	    // handle error
//	}
//	for _, row := range result.Results.Rows {
//	    fmt.Printf("User: ID=%v, Name=%v\n", row[0], row[1])
//	}
func (c *Client) RawQuery(ctx context.Context, databaseID, sql string, params ...any) ([]RawQueryResult, error) {
	p2 := convertTypes(params)
	body := map[string]any{
		"sql":    sql,
		"params": p2,
	}
	var result []RawQueryResult
	err := c.sendRequest(ctx, http.MethodPost, fmt.Sprintf("/database/%s/raw", databaseID), body, &result, nil)
	if err != nil {
		return nil, convertSQLiteError(err, sql, p2)
	}
	return result, nil
}
