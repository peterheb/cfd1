package cfd1

import (
	"context"
	"sync"
)

// Handle represents a psuedo-connection to a single D1 database, similar to a
// connection with other database engines. The D1 API does not provide for
// persistent connections, however Handle is still a useful abstraction for
// accessing a single database for multiple operations. To get a database
// handle, use [Client.GetHandle].
type Handle struct {
	client      *Client
	dbID        string
	rowsRead    int
	rowsWritten int
	lastRowID   int
	lastMeta    QueryMeta
	mux         sync.RWMutex
}

// Ping sends a ping request to the database to check if it is reachable.
func (h *Handle) Ping(ctx context.Context) error {
	_, err := h.Query(ctx, "SELECT 1")
	return err
}

// Query executes a SQL query on this database and returns the results. The
// query can contain multiple semicolon-separated statements, which will be
// executed as a batch, and be up to 100KB. A maximum of 100 placeholder
// parameters can be used.
func (h *Handle) Query(ctx context.Context, sql string, params ...any) ([]map[string]any, error) {
	result, err := h.client.Query(ctx, h.dbID, sql, params...)
	if err != nil {
		return nil, err
	}

	h.mux.Lock()
	defer h.mux.Unlock()
	h.rowsRead += result.Meta.RowsRead
	h.rowsWritten += result.Meta.RowsWritten
	h.lastRowID = result.Meta.LastRowID
	h.lastMeta = result.Meta

	return result.Results, nil
}

// Execute executes a SQL query on this database that has no results. The query
// can contain multiple semicolon-separated statements, which will be executed
// as a batch, and be up to 100KB. A maximum of 100 placeholder parameters can
// be used.
func (h *Handle) Execute(ctx context.Context, sql string, params ...any) error {
	_, err := h.client.Query(ctx, h.dbID, sql, params...)
	return err
}

// QueryRow executes a SQL query on this database and returns a single row of
// results as a Row object, suitable for calling Scan. If the query returns
// multiple rows, only the first row is reachable.
func (h *Handle) QueryRow(ctx context.Context, sql string, params ...any) *Row {
	result, err := h.client.RawQuery(ctx, h.dbID, sql, params...)
	return newRow(&result[0], err)
}

// QueryRows executes a SQL query on this database and returns a Rows object
// that can iterate the resultsets and rows.
func (h *Handle) QueryRows(ctx context.Context, sql string, params ...any) *Rows {
	result, err := h.client.RawQuery(ctx, h.dbID, sql, params...)
	return newRows(result, err)
}

// Export initiates an export (SQL dump) on this database. It accepts an
// optional [ExportOptions] to limit the scope of the export; passing nil for
// this parameter will export the data and schema of all tables. The method
// waits until the export is complete, and returns the download URL for the
// completed SQL dump as a string. The database will be unavailable for other
// queries for the duration of the export.
func (h *Handle) Export(ctx context.Context, opts *ExportOptions) (string, error) {
	return h.client.Export(ctx, h.dbID, opts)
}

// Import initiates an import of an SQL dump into this database. The method
// accepts the SQL dump as filename, reads it from disk, and waits until the
// import is complete. The database will be unavailable for other queries for
// the duration of the import.
func (h *Handle) Import(ctx context.Context, sqlFilePath string) (*ImportResult, error) {
	result, err := h.client.Import(ctx, h.dbID, sqlFilePath)
	if err != nil {
		return nil, err
	}

	h.mux.Lock()
	defer h.mux.Unlock()
	h.rowsRead += result.RowsRead
	h.rowsWritten += result.RowsWritten

	return result, nil
}

// UUID returns the unique identifier for the database represented by this
// handle. This is a 36-character hex string of the form
// "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".
func (h *Handle) UUID() string {
	return h.dbID
}

// LastRowID returns the last row ID that was inserted into the database by this
// handle.
func (h *Handle) LastRowID() int {
	h.mux.RLock()
	defer h.mux.RUnlock()
	return h.lastRowID
}

// LastMeta returns the [QueryMeta] for the last query executed by this handle.
func (h *Handle) LastMeta() QueryMeta {
	h.mux.RLock()
	defer h.mux.RUnlock()
	return h.lastMeta
}

// GetDetails returns the current DatabaseDetails describing this database,
// including the number of tables and size on disk.
func (h *Handle) GetDetails(ctx context.Context) (*DatabaseDetails, error) {
	return h.client.GetDatabase(ctx, h.dbID)
}

// RowsRead returns the total number of rows read during the lifetime of this
// handle.
func (h *Handle) RowsRead() int {
	h.mux.RLock()
	defer h.mux.Unlock()
	return h.rowsRead
}

// RowsWritten returns the total number of rows written during the lifetime of
// this handle.
func (h *Handle) RowsWritten() int {
	h.mux.RLock()
	defer h.mux.Unlock()
	return h.rowsWritten
}
