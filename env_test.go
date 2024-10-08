package cfd1_test

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	// Load .env.test if it exists
	err := loadEnv("../../.env.test")
	if err != nil {
		panic("Failed to load .env.test file: " + err.Error())
	}

	// Run tests
	os.Exit(m.Run())
}

// This is a very simple .env file parser.
func loadEnv(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		// If the file does not exist, return without error
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		// Split into key-value pair
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue // Skip invalid lines
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if len(key) < 1 || len(key) > 128 || len(value) > 128 {
			continue // Skip invalid lines or long keys/values
		}

		// Set the environment variable
		err = os.Setenv(key, value)
		if err != nil {
			return err
		}
	}

	return scanner.Err()
}
