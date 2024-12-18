package cfd1

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// Row is a single row of query results.
type Row struct {
	result *RawQueryResult
	err    error
}

// Rows is a collection of rows of query results.
type Rows struct {
	result     []RawQueryResult
	rs         *RawQueryResult
	current    int
	currentSet int
	err        error
}

func newRow(result *RawQueryResult, err error) *Row {
	if err != nil {
		return &Row{err: err}
	}

	return &Row{result: result}
}

func newRows(result []RawQueryResult, err error) *Rows {
	if err != nil {
		return &Rows{err: err}
	}

	ret := Rows{
		current: -1,
		result:  result,
	}
	if len(result) > 0 {
		ret.rs = &result[0]
	}

	return &ret
}

// Err returns the error, if any, that was encountered during iteration.
func (r *Row) Err() error {
	if r == nil {
		return sql.ErrNoRows
	}

	if r.err != nil {
		return r.err
	}

	if r.result == nil || len(r.result.Results.Rows) == 0 {
		return sql.ErrNoRows
	}

	return nil
}

// Scan copies the columns in the current row into the values pointed at by dest.
func (r *Row) Scan(dest ...interface{}) error {
	if r.Err() != nil {
		return r.Err()
	}

	row := r.result.Results.Rows[0]
	for i, col := range row {
		if i >= len(dest) {
			break
		}
		if err := assign(dest[i], col); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}

	return nil
}

// Err returns the error, if any, that was encountered during iteration.
func (r *Rows) Err() error {
	if r == nil {
		return sql.ErrNoRows
	}

	if r.err != nil {
		return r.err
	}

	if r.result == nil || r.currentSet >= len(r.result) || len(r.rs.Results.Rows) == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (r *Rows) Next() bool {
	if r.Err() != nil {
		return false
	}

	r.current++
	if r.current >= len(r.rs.Results.Rows) {
		return false
	}

	return true
}

func (r *Rows) NextSet() bool {
	if r.Err() != nil {
		return false
	}

	r.current = -1
	r.currentSet++
	if r.currentSet >= len(r.result) {
		return false
	}

	r.rs = &r.result[r.currentSet]
	return true
}

func (r *Rows) Scan(dest ...interface{}) error {
	if r.Err() != nil {
		return r.Err()
	}

	if r.current >= len(r.rs.Results.Rows) {
		return sql.ErrNoRows
	}

	row := r.rs.Results.Rows[r.current]
	for i, col := range row {
		if i >= len(dest) {
			break
		}
		if err := assign(dest[i], col); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}

	return nil
}

func assign(dest, src any) error {
	// Fast path for nil
	if src == nil {
		reflect.ValueOf(dest).Elem().Set(reflect.Zero(reflect.TypeOf(dest).Elem()))
		return nil
	}

	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}
	dv = dv.Elem()
	dt := dv.Type()

	sv := reflect.ValueOf(src)
	st := sv.Type()

	fmt.Printf("assign %v (kind %v type %v) to kind %v type %v\n", src, st.Kind(), st, dt.Kind(), dt)

	// Handle scannable interfaces (sql.Scanner, etc)
	if scanner, ok := dv.Interface().(sql.Scanner); ok {
		return scanner.Scan(src)
	}

	// If types match directly, fast path
	if st.ConvertibleTo(dt) {
		dv.Set(sv.Convert(dt))
		return nil
	}

	// Special case conversions
	switch dt.Kind() {
	case reflect.String:
		switch sv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetString(strconv.FormatInt(sv.Int(), 10))
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dv.SetString(strconv.FormatUint(sv.Uint(), 10))
			return nil
		case reflect.Float32, reflect.Float64:
			dv.SetString(strconv.FormatFloat(sv.Float(), 'f', -1, 64))
			return nil
		case reflect.Bool:
			dv.SetString(strconv.FormatBool(sv.Bool()))
			return nil
		case reflect.Slice:
			if sv.Type().Elem().Kind() == reflect.Uint8 { // []byte
				dv.SetString(string(sv.Bytes()))
				return nil
			}
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch sv.Kind() {
		case reflect.String:
			if i, err := strconv.ParseInt(sv.String(), 0, dt.Bits()); err == nil {
				dv.SetInt(i)
				return nil
			}
		case reflect.Float32, reflect.Float64:
			dv.SetInt(int64(sv.Float()))
			return nil
		case reflect.Bool:
			if sv.Bool() {
				dv.SetInt(1)
			} else {
				dv.SetInt(0)
			}
			return nil
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch sv.Kind() {
		case reflect.String:
			if i, err := strconv.ParseUint(sv.String(), 0, dt.Bits()); err == nil {
				dv.SetUint(i)
				return nil
			}
		case reflect.Float32, reflect.Float64:
			dv.SetUint(uint64(sv.Float()))
			return nil
		case reflect.Bool:
			if sv.Bool() {
				dv.SetUint(1)
			} else {
				dv.SetUint(0)
			}
			return nil
		}

	case reflect.Float32, reflect.Float64:
		switch sv.Kind() {
		case reflect.String:
			if f, err := strconv.ParseFloat(sv.String(), dt.Bits()); err == nil {
				dv.SetFloat(f)
				return nil
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetFloat(float64(sv.Int()))
			return nil
		case reflect.Bool:
			if sv.Bool() {
				dv.SetFloat(1)
			} else {
				dv.SetFloat(0)
			}
			return nil
		}

	case reflect.Bool:
		switch sv.Kind() {
		case reflect.String:
			if b, err := strconv.ParseBool(sv.String()); err == nil {
				dv.SetBool(b)
				return nil
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetBool(sv.Int() != 0)
			return nil
		case reflect.Float32, reflect.Float64:
			dv.SetBool(sv.Float() != 0)
			return nil
		}

	case reflect.Slice:
		if dt.Elem().Kind() == reflect.Uint8 { // []byte
			switch sv.Kind() {
			case reflect.String:
				dv.SetBytes([]byte(sv.String()))
				return nil
			}
		}

	case reflect.Struct:
		// If an int is mapped to a time.Time, it is treated as a unix timestamp
		if dt == reflect.TypeOf(time.Time{}) {
			switch sv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				dv.Set(reflect.ValueOf(time.Unix(sv.Int(), 0).UTC()))
				return nil
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				dv.Set(reflect.ValueOf(time.Unix(int64(sv.Uint()), 0).UTC()))
				return nil
			case reflect.Float64:
				dv.Set(reflect.ValueOf(time.Unix(int64(sv.Float()), 0).UTC()))
			}
		}
	}

	return fmt.Errorf("cannot convert %v (type %v) to type %v", src, st, dt)
}
