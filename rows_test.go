package cfd1

import (
	"reflect"
	"testing"
	"time"
)

type TestBaseInt int
type TestBaseString string

func TestAssign(t *testing.T) {
	tests := []struct {
		name        string
		dest        any
		src         any
		expected    any
		expectError bool
	}{
		// Direct Assignments
		{"Assign int to int", new(int), 42, 42, false},
		{"Assign uint to uint", new(uint), uint(42), uint(42), false},
		{"Assign string to string", new(string), "test", "test", false},
		{"Assign float64 to float64", new(float64), 3.14, 3.14, false},
		{"Assign bool to bool", new(bool), true, true, false},

		// Type Conversions
		{"Convert int to string", new(string), int(42), "42", false},
		{"Convert uint to string", new(string), uint(42), "42", false},
		{"Convert bool to string", new(string), true, "true", false},
		{"Convert float64 to string", new(string), 3.14, "3.14", false},
		{"Convert uint to int", new(int), uint(42), 42, false},
		{"Convert string to int", new(int), "123", 123, false},
		{"Convert float64 to int", new(int), 3.99, 3, false},
		{"Convert bool true to int", new(int), true, 1, false},
		{"Convert bool false to int", new(int), false, 0, false},
		{"Convert int to uint", new(uint), 42, uint(42), false},
		{"Convert string to uint", new(uint), "123", uint(123), false},
		{"Convert float64 to uint", new(uint), 3.99, uint(3), false},
		{"Convert bool true to uint", new(uint), true, uint(1), false},
		{"Convert bool false to uint", new(uint), false, uint(0), false},
		{"Convert int to float64", new(float64), 42, 42.0, false},
		{"Convert uint to float64", new(float64), uint(42), 42.0, false},
		{"Convert bool true to float64", new(float64), true, 1.0, false},
		{"Convert bool false to float64", new(float64), false, float64(0), false},
		{"Convert string to float64", new(float64), "3.14", 3.14, false},
		{"Convert int to bool", new(bool), 1, true, false},
		{"Convert uint to bool", new(bool), uint(1), true, false},
		{"Convert float64 to bool", new(bool), 1.0, true, false},
		{"Convert string true to bool", new(bool), "true", true, false},

		// Custom Types
		{"Assign float64 to custom int", new(TestBaseInt), 2.0, TestBaseInt(2), false},
		{"Assign int to custom int", new(TestBaseInt), 3, TestBaseInt(3), false},
		{"Assign string to custom string", new(TestBaseString), "test", TestBaseString("test"), false},

		// Time.time (treat input as UTC seconds)
		{"Assign time.Time from int", new(time.Time), int(1257894000), time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC), false},
		{"Assign time.Time from uint", new(time.Time), uint(1257894000), time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC), false},
		{"Assign time.Time from float64", new(time.Time), float64(1257894000), time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC), false},
		{"Assign time.Time from RFC3339 string", new(time.Time), "2009-11-10T23:00:00Z", time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC), false},
		{"Assign time.Time from int string", new(time.Time), "1257894000", time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC), false},

		// Edge Cases
		{"Assign nil to int", new(int), nil, 0, false},
		{"Assign nil to string", new(string), nil, "", false},
		{"Assign nil to float64", new(float64), nil, 0.0, false},
		{"Assign large float64 to int", new(int), 1e9, 1000000000, false},

		// Invalid Conversions
		{"Nil dest", nil, "test", nil, true},
		{"Invalid conversion string to int", new(int), "invalid", nil, true},
		{"Invalid conversion string to float64", new(float64), "invalid", nil, true},
		{"Invalid conversion bool to string", new(string), struct{}{}, nil, true},

		// Byte Slice
		{"Convert []byte to string", new(string), []byte("abc"), "abc", false},
		{"Assign string to []byte", new([]byte), "hello", []byte("hello"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := assign(tt.dest, tt.src)
			if (err != nil) != tt.expectError {
				t.Errorf("unexpected error state: got %v, want error: %v", err, tt.expectError)
			}
			if !tt.expectError {
				destVal := reflect.ValueOf(tt.dest).Elem().Interface()
				if !reflect.DeepEqual(destVal, tt.expected) {
					t.Errorf("unexpected result: got %v, want %v", destVal, tt.expected)
				}
			}
		})
	}
}
