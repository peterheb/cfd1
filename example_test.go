//go:build examples
// +build examples

// Example demonstrates basic usage of the cfd1 package
package cfd1_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/peterheb/cfd1"
)

func Example() {
	// Create a new client
	client := cfd1.NewClient(os.Getenv("CLOUDFLARE_ACCOUNT_ID"),
		os.Getenv("CLOUDFLARE_API_TOKEN"))

	fatalIf := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}

	// Create a new database
	db, err := client.CreateDatabase(context.Background(),
		"my-example-database", cfd1.LocationHintAuto)
	fatalIf(err)
	h, err := client.GetHandle(context.Background(), db.UUID)
	fatalIf(err)

	// Create a table
	_, err = h.Query(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)`)
	fatalIf(err)
	fmt.Println("Table created")

	// Insert data
	_, err = h.Query(context.Background(),
		`INSERT INTO users (name,age) VALUES (?,?)`, "John Doe", 35)
	fatalIf(err)
	fmt.Printf("Inserted user. Last row ID: %d\n", h.LastRowID())

	// Query data
	var id, age int
	var name string
	err = h.QueryRow(context.Background(),
		`SELECT id, name, age FROM users WHERE id=?`, h.LastRowID()).Scan(&id, &name, &age)
	fatalIf(err)
	fmt.Printf("User: ID=%v, Name=%q, Age=%v\n", id, name, age)

	// Output:
	// Table created
	// Inserted user. Last row ID: 1
	// User: ID=1, Name="John Doe", Age=35
}

func ExampleClient_Query() {
	client := cfd1.NewClient(os.Getenv("CLOUDFLARE_ACCOUNT_ID"),
		os.Getenv("CLOUDFLARE_API_TOKEN"))

	result, err := client.Query(context.Background(), "11111111-2222-3333-4444-555555555555",
		`SELECT id, name, age FROM users WHERE age > ?`, 30)
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range result.Results {
		fmt.Printf("User: ID=%v, Name=%v, Age=%v\n",
			row["id"], row["name"], row["age"])
	}

	fmt.Printf("Rows read: %d\n", result.Meta.RowsRead)
	// Output:
	// User: ID=1, Name=John Doe, Age=35
	// Rows read: 1
}

type ConsoleDebugLogger struct{}

func (dl ConsoleDebugLogger) LogRequest(method string, url string, requestBody, responseBody []byte, statusCode int) {
	indentJSON := func(data []byte) string {
		if len(data) == 0 {
			return "<empty>"
		}

		var buf bytes.Buffer
		err := json.Indent(&buf, data, "", "  ")
		if err != nil {
			return string(data)
		}
		return buf.String()
	}

	fmt.Printf("%s\n", time.Now().Format(time.RFC3339))
	fmt.Printf("%d: %s %s\n", statusCode, method, url)
	fmt.Printf("Request Body:\n%s\n", indentJSON(requestBody))
	fmt.Printf("Response Body:\n%s\n", indentJSON(responseBody))
	fmt.Println("----------------------------------------------")
}

func ExampleWithDebugLogger() {
	// Create a new client with debug logging enabled
	client := cfd1.NewClient(
		os.Getenv("CLOUDFLARE_ACCOUNT_ID"),
		os.Getenv("CLOUDFLARE_API_TOKEN"),
		cfd1.WithDebugLogger(ConsoleDebugLogger{}), // see example_test.go
	)

	// Perform a simple operation to demonstrate the debug logger
	databases, err := client.ListDatabases(context.Background(), "")
	if err != nil {
		log.Fatalf("Error listing databases: %v\n", err)
	}

	fmt.Printf("Found %d databases\n", len(databases))
	// Output:
	// 2024-10-11T16:00:00-07:00
	// 200: GET https://api.cloudflare.com/client/v4/accounts/your-account-id/d1/database?page=1&per_page=100
	// Request Body:
	// <empty>
	// Response Body:
	// {
	//   "result": [
	// 	   {
	// 	     "uuid": "e4e4e4e4-4555-4777-b222-1a2b3c4d5e6f",
	// 	     "name": "example",
	// 	     "version": "production",
	// 	     "created_at": "2009-11-10T16:00:00.388Z",
	// 	     "file_size": 16384,
	// 	     "num_tables": 1
	// 	   }
	//   ],
	//   "result_info": {
	// 	   "page": 1,
	// 	   "per_page": 100,
	// 	   "count": 1,
	// 	   "total_count": 1
	//   },
	//   "success": true,
	//   "errors": [],
	//   "messages": []
	// }
	// ----------------------------------------------
	// Found 1 databases
}
