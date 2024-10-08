package cfd1

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// LocationHint represents the geographical location hint for database creation.
type LocationHint string

// LocationHint constants specify the data center when creating a database.
// LocationHintAuto lets D1 choose the nearest location automatically.
const (
	LocationHintAuto                LocationHint = ""
	LocationHintEasternNorthAmerica              = "enam"
	LocationHintWesternNorthAmerica              = "wnam"
	LocationHintWesternEurope                    = "weur"
	LocationHintEasternEurope                    = "eeur"
	LocationHintAsiaPacific                      = "apac"
	LocationHintOceania                          = "oc"
)

// DatabaseDetails represents information about a D1 database.
type DatabaseDetails struct {
	CreatedAt time.Time `json:"created_at"`
	Name      string    `json:"name"`
	UUID      string    `json:"uuid"`
	Version   string    `json:"version"`
	FileSize  int       `json:"file_size"`
	NumTables int       `json:"num_tables"`
}

// ListDatabases returns all databases associated with the account. If name is
// non-empty, it filters results to databases including that name ('LIKE
// %name%'). Returns a slice of [DatabaseDetails]. Note that although the
// underlying D1 API supports pagination, this method automatically fetches all
// pages of results.
//
// Example usage:
//
//	allDatabases, err := client.ListDatabases(ctx, "")
//	if err != nil {
//	    // handle error
//	}
//	for _, db := range allDatabases {
//	    fmt.Printf("Database: %s (UUID: %s)\n", db.Name, db.UUID)
//	}
func (c *Client) ListDatabases(ctx context.Context, name string) ([]DatabaseDetails, error) {
	var allDatabases []DatabaseDetails
	page := 1
	perPage := 100

	for {
		databases, hasMore, err := c.listDatabasesPage(ctx, page, perPage, name)
		if err != nil {
			return nil, fmt.Errorf("listing databases (page %d): %w", page, err)
		}

		allDatabases = append(allDatabases, databases...)
		if !hasMore {
			break
		}

		page++
	}

	return allDatabases, nil
}

// CreateDatabase creates a new database with the given name and [LocationHint].
// Returns a [DatabaseDetails] struct containing information about the new
// database, including its UUID, which is required for future operations.
//
// Example usage:
//
//	dbDetails, err := client.CreateDatabase(ctx, "my-database", cfd1.LocationHintAuto)
//	if err != nil {
//	    // handle error
//	}
//	fmt.Printf("Created database: %s (UUID: %s)\n", dbDetails.Name, dbDetails.UUID)
func (c *Client) CreateDatabase(ctx context.Context, name string, primaryLocationHint LocationHint) (*DatabaseDetails, error) {
	body := map[string]string{"name": name}
	if primaryLocationHint != "" {
		body["primary_location_hint"] = string(primaryLocationHint)
	}
	var result DatabaseDetails
	err := c.sendRequest(ctx, http.MethodPost, "/database", body, &result, nil)
	if err != nil {
		return nil, fmt.Errorf("creating database: %w", err)
	}
	return &result, nil
}

// GetDatabase retrieves details about the database identified by databaseID.
// Returns a [DatabaseDetails] struct.
func (c *Client) GetDatabase(ctx context.Context, databaseID string) (*DatabaseDetails, error) {
	var result DatabaseDetails
	err := c.sendRequest(ctx, http.MethodGet, fmt.Sprintf("/database/%s", databaseID), nil, &result, nil)
	if err != nil {
		return nil, fmt.Errorf("getting database details: %w", err)
	}
	return &result, nil
}

// DeleteDatabase permanently deletes the database identified by databaseID.
func (c *Client) DeleteDatabase(ctx context.Context, databaseID string) error {
	err := c.sendRequest(ctx, http.MethodDelete, fmt.Sprintf("/database/%s", databaseID), nil, nil, nil)
	if err != nil {
		return fmt.Errorf("deleting database: %w", err)
	}
	return nil
}

// listDatabasesPage retrieves a single page of databases.
func (c *Client) listDatabasesPage(ctx context.Context, page, perPage int, name string) ([]DatabaseDetails, bool, error) {
	queryParams := url.Values{}
	queryParams.Set("page", strconv.Itoa(page))
	queryParams.Set("per_page", strconv.Itoa(perPage))
	if name != "" {
		queryParams.Set("name", name)
	}

	path := fmt.Sprintf("/database?%s", queryParams.Encode())

	var pageInfo apiResponseInfo
	var pageData []DatabaseDetails
	err := c.sendRequest(ctx, http.MethodGet, path, nil, &pageData, &pageInfo)
	if err != nil {
		return nil, false, err
	}

	hasMore := pageInfo.Count > 0 && pageInfo.Page*pageInfo.PerPage < pageInfo.TotalCount
	return pageData, hasMore, nil
}
