package cfd1

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"
)

// Use `go test -v -integration` to run integration tests.
// Ensure CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID are in environment.

var runIntegration = flag.Bool("integration", false, "run integration tests against real D1 service")

func requireEnv(t *testing.T, key string) string {
	is := is.New(t)
	value := os.Getenv(key)
	is.True(value != "") // missing environment variable
	return value
}

func TestIntegrationD1(t *testing.T) {
	if !*runIntegration {
		t.Skip("Skipping integration tests. Use -integration flag to run")
	}
	is := is.New(t)

	// Get credentials from environment
	token := requireEnv(t, "CLOUDFLARE_API_TOKEN")
	accountID := requireEnv(t, "CLOUDFLARE_ACCOUNT_ID")

	// Create client
	client := NewClient(accountID, token)
	ctx := context.Background()

	// Use a unique prefix for all test databases
	prefix := fmt.Sprintf("cfd1-test-%08x", time.Now().Unix())

	t.Run("Full database lifecycle", func(t *testing.T) {
		// Create database
		dbName := prefix + "-lifecycle"
		db, err := client.CreateDatabase(ctx, dbName, LocationHintAuto)
		is.NoErr(err)      // lifecycle database creation failed
		is.True(db != nil) // lifecycle database creation soft fail
		t.Logf("Created database: %s (UUID: %s)", db.Name, db.UUID)

		// Clean up at the end of the test
		defer func() {
			err := client.DeleteDatabase(ctx, db.UUID)
			if err != nil {
				t.Logf("Failed to clean up database %s: %v", db.UUID, err)
			}
		}()

		// Create table
		_, err = client.Query(ctx, db.UUID, `
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `)
		is.NoErr(err) // table creation failed

		// Insert data
		result, err := client.Query(ctx, db.UUID, `
            INSERT INTO test_users (name, email) VALUES 
            (?, ?),
            (?, ?)
        `, "Alice", "alice@example.com", "Bob", "bob@example.com")
		is.NoErr(err)                    // INSERT failed
		is.True(result.Success)          // INSERT soft fail
		is.Equal(result.Meta.Changes, 2) // INSERT unexpected change count

		// Query data with regular query
		result, err = client.Query(ctx, db.UUID, "SELECT * FROM test_users ORDER BY name")
		is.NoErr(err)                                // SELECT failed
		is.True(result.Success)                      // SELECT soft fail
		is.Equal(len(result.Results), 2)             // SELECT wrong record count
		is.Equal("Alice", result.Results[0]["name"]) // SELECT wrong data returned
		is.Equal("Bob", result.Results[1]["name"])   // SELECT wrong data returned

		// Query data with raw query
		rawResult, err := client.RawQuery(ctx, db.UUID, "SELECT name, email FROM test_users WHERE id = ?", 1)
		is.NoErr(err)                                                     // raw SELECT failed
		is.Equal(len(rawResult), 1)                                       // raw SELECT soft fail
		is.Equal(rawResult[0].Results.Columns, []string{"name", "email"}) // raw SELECT wrong columns
		is.Equal(rawResult[0].Results.Rows[0][0], "Alice")                // raw SELECT wrong data returned
	})

	t.Run("Database listing and filtering", func(t *testing.T) {
		// Create multiple databases
		dbNames := []string{
			prefix + "-list-1",
			prefix + "-list-2",
			prefix + "-other",
		}

		var createdDBs []string
		for _, name := range dbNames {
			db, err := client.CreateDatabase(ctx, name, LocationHintAuto)
			is.NoErr(err) // database creation failed
			createdDBs = append(createdDBs, db.UUID)
		}

		// Clean up at the end
		defer func() {
			for _, uuid := range createdDBs {
				err := client.DeleteDatabase(ctx, uuid)
				if err != nil {
					t.Logf("Failed to clean up database %s: %v", uuid, err)
				}
			}
		}()

		// List all test databases
		dbs, err := client.ListDatabases(ctx, prefix)
		is.NoErr(err)                    // listing failed
		is.Equal(len(dbs), len(dbNames)) // wrong number of databases returned

		// List with more specific filter
		filtered, err := client.ListDatabases(ctx, prefix+"-list")
		is.NoErr(err)              // listing failed
		is.Equal(len(filtered), 2) // wrong number of databases returned
	})

	t.Run("Error handling", func(t *testing.T) {
		// Create test database
		dbName := prefix + "-errors"
		db, err := client.CreateDatabase(ctx, dbName, LocationHintAuto)
		is.NoErr(err)

		defer func() {
			err := client.DeleteDatabase(ctx, db.UUID)
			if err != nil {
				t.Logf("Failed to clean up database %s: %v", db.UUID, err)
			}
			t.Logf("Deleted database: %s", db.UUID)
		}()

		// Test SQL syntax error
		_, err = client.Query(ctx, db.UUID, "SELECT * FROM nonexistent_table")
		is.True(err != nil)                                    // exoected error
		is.True(strings.Contains(err.Error(), "SQLITE_ERROR")) // expected SQLITE_ERROR

		// Test constraint violation
		_, err = client.Query(ctx, db.UUID, `
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE
            )
        `)
		is.NoErr(err)

		_, err = client.Query(ctx, db.UUID, `
            INSERT INTO users (email) VALUES (?), (?)
        `, "same@email.com", "same@email.com")
		is.True(err != nil)                                         // expected error
		is.True(strings.Contains(err.Error(), "SQLITE_CONSTRAINT")) // expected SQLITE_CONSTRAINT
	})
}
