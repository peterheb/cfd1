package cfd1

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
)

func init() {
	sql.Register("cfd1", &d1Driver{})
}

// d1Driver implements a database/sql driver for D1.
type d1Driver struct {
	mu            sync.Mutex
	clientFactory func(cfg *config) (CFD1Client, error)
}

// Open returns a new connection to the database.
func (d *d1Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext interface.
func (d *d1Driver) OpenConnector(name string) (driver.Connector, error) {
	cfg, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	return &connector{
		driver: d,
		cfg:    cfg,
	}, nil
}

// Connector implements driver.Connector interface.
type connector struct {
	driver *d1Driver
	cfg    *config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := c.driver.createClient(c.cfg)
	if err != nil {
		return nil, err
	}

	h, err := client.GetHandle(ctx, c.cfg.DatabaseNameOrUUID)
	if err != nil {
		return nil, err
	}

	newConn := &conn{
		handle: h,
	}
	return newConn, nil
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

// createClient creates a new CFD1Client instance.
func (d *d1Driver) createClient(cfg *config) (CFD1Client, error) {
	if d.clientFactory != nil {
		return d.clientFactory(cfg)
	}
	return NewClient(cfg.AccountID, cfg.APIToken), nil
}

type config struct {
	AccountID          string
	APIToken           string
	DatabaseNameOrUUID string
}

func parseDSN(dsn string) (*config, error) {
	cfg := &config{}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid DSN: %v", err)
	}

	// Extract account_id and api_token from user info
	if u.User != nil {
		cfg.AccountID = u.User.Username()
		cfg.APIToken, _ = u.User.Password()
	}

	// Extract database_id from host
	cfg.DatabaseNameOrUUID = u.Host

	// Validate the config
	if cfg.AccountID == "" {
		return nil, errors.New("account_id (username) is required in the DSN")
	}
	if cfg.APIToken == "" {
		return nil, errors.New("api_token (password) is required in the DSN")
	}
	if cfg.DatabaseNameOrUUID == "" {
		return nil, errors.New("database_id (host) is required in the DSN")
	}

	return cfg, nil
}

type conn struct {
	handle *Handle
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{
		conn:  c,
		query: query,
	}, nil
}

func (c *conn) Close() error {
	// no-op
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, driver.ErrSkip
}

// Implement ExecerContext interface
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !c.IsValid() {
		return nil, driver.ErrBadConn
	}
	params := namedValuesToAny(args)
	_, err := c.handle.Query(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	return &driverResult{
		rowsAffected: int64(c.handle.LastMeta().RowsWritten),
		lastInsertID: int64(c.handle.LastRowID()),
	}, nil
}

// Implement QueryerContext interface
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !c.IsValid() {
		return nil, driver.ErrBadConn
	}
	params := namedValuesToAny(args)
	result, err := c.handle.Query(ctx, query, params...)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0, len(result[0]))
	for col := range result[0] {
		columns = append(columns, col)
	}

	return &rows{
		columns: columns,
		rows:    result,
	}, nil
}

// Implement Pinger interface
func (c *conn) Ping(ctx context.Context) error {
	return c.handle.Ping(ctx)
}

func (c *conn) ResetSession(ctx context.Context) error {
	// We don't keep connections open so this is a no-op
	return nil
}

func (c *conn) IsValid() bool {
	return c.handle != nil
}

type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

type rows struct {
	columns []string
	rows    []map[string]any
	current int
}

func (r *rows) Columns() []string { return r.columns }
func (r *rows) Close() error      { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.current >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.current]
	for i, col := range r.columns {
		dest[i] = row[col]
	}
	r.current++
	return nil
}

type driverResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r *driverResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r *driverResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

func valuesToNamedValues(vals []driver.Value) []driver.NamedValue {
	nvs := make([]driver.NamedValue, len(vals))
	for i, v := range vals {
		nvs[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return nvs
}

func namedValuesToAny(nvs []driver.NamedValue) []any {
	params := make([]any, len(nvs))
	for i, nv := range nvs {
		params[i] = nv.Value
	}
	return params
}
