package cfd1

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Row is a single row of query results.
type Row struct {
	result   *RawQueryResult
	fieldMap map[string]int
	err      error
}

// Rows is a collection of rows of query results.
type Rows struct {
	result     []RawQueryResult
	rs         *RawQueryResult
	current    int
	currentSet int
	fieldMap   map[string]int
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

// ScanStruct scans the current row into a struct. The struct fields are matched
// to the column names in the result set. The struct fields can be tagged with
// `db`, `sql`, or `json` to specify the column name. If no tag is present, the
// field name is used.
func (r *Row) ScanStruct(dest interface{}) error {
	if r.Err() != nil {
		return r.Err()
	}

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to struct")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to struct")
	}

	if r.fieldMap == nil {
		r.fieldMap = createFieldMap(v.Type())
	}
	return scanStructWithMap(r.result.Results.Columns, r.result.Results.Rows[0], v, r.fieldMap)
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

// ScanStruct scans the current row into a struct. The struct fields are matched
// to the column names in the result set. The struct fields can be tagged with
// `db`, `sql`, or `json` to specify the column name. If no tag is present, the
// field name is used.
func (r *Rows) ScanStruct(dest interface{}) error {
	if r.Err() != nil {
		return r.Err()
	}

	if r.current >= len(r.rs.Results.Rows) {
		return sql.ErrNoRows
	}

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to struct")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("dest must be a pointer to struct")
	}

	if r.fieldMap == nil {
		r.fieldMap = createFieldMap(v.Type())
	}
	return scanStructWithMap(r.rs.Results.Columns, r.rs.Results.Rows[r.current], v, r.fieldMap)
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

	// Handle scannable interfaces (sql.Scanner, etc)
	if scanner, ok := dv.Interface().(sql.Scanner); ok {
		return scanner.Scan(src)
	}

	// Handle special cases (e.g., int -> string) before ConvertibleTo().
	// Otherwise, 42 converts to "*" not "42".
	if dt.Kind() == reflect.String && (st.Kind() == reflect.Int || st.Kind() == reflect.Uint) {
		switch st.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dv.SetString(strconv.FormatInt(sv.Int(), 10))
			return nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dv.SetString(strconv.FormatUint(sv.Uint(), 10))
			return nil
		}
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
		case reflect.Float32, reflect.Float64:
			dv.SetString(strconv.FormatFloat(sv.Float(), 'f', -1, 64))
			return nil
		case reflect.Bool:
			dv.SetString(strconv.FormatBool(sv.Bool()))
			return nil
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch sv.Kind() {
		case reflect.String:
			if i, err := strconv.ParseInt(sv.String(), 0, dt.Bits()); err == nil {
				dv.SetInt(i)
				return nil
			}
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
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dv.SetBool(sv.Uint() != 0)
			return nil
		case reflect.Float32, reflect.Float64:
			dv.SetBool(sv.Float() != 0)
			return nil
		}

	case reflect.Struct:
		// If a numeric is mapped to a time.Time, it is treated as a unix timestamp
		if dt == reflect.TypeOf(time.Time{}) {
			if !sv.IsValid() {
				dv.Set(reflect.Zero(dt))
				return nil
			}
			switch sv.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				dv.Set(reflect.ValueOf(time.Unix(sv.Int(), 0).UTC()))
				return nil
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				dv.Set(reflect.ValueOf(time.Unix(int64(sv.Uint()), 0).UTC()))
				return nil
			case reflect.Float64:
				dv.Set(reflect.ValueOf(time.Unix(int64(sv.Float()), 0).UTC()))
				return nil
			case reflect.String:
				if t, err := time.Parse(time.RFC3339, sv.String()); err == nil {
					dv.Set(reflect.ValueOf(t))
					return nil
				} else if i, err := strconv.ParseInt(sv.String(), 0, 64); err == nil {
					dv.Set(reflect.ValueOf(time.Unix(i, 0).UTC()))
					return nil
				}
			}
		}
	}
	return fmt.Errorf("cannot convert value %v (type %v.%v) to destination type %v.%v", src, st.PkgPath(), st.Name(), dt.PkgPath(), dt.Name())
}

func createFieldMap(t reflect.Type) map[string]int {
	fieldMap := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Check for db tag first
		if tag := field.Tag.Get("db"); tag != "" {
			if tag == "-" {
				continue
			}
			fieldMap[tag] = i
			continue
		}

		// Check for sql tag next
		if tag := field.Tag.Get("sql"); tag != "" {
			if tag == "-" {
				continue
			}
			fieldMap[tag] = i
			continue
		}

		// Check for json tag next
		if tag := field.Tag.Get("json"); tag != "" {
			if tag == "-" {
				continue
			}
			fieldMap[tag] = i
			continue
		}

		// Fall back to field name
		fieldMap[strings.ToLower(field.Name)] = i
	}
	return fieldMap
}

func scanStructWithMap(cols []string, row []any, v reflect.Value, fieldMap map[string]int) error {
	for i, col := range cols {
		if fieldIndex, ok := fieldMap[strings.ToLower(col)]; ok {
			field := v.Field(fieldIndex)
			if field.CanSet() {
				if row[i] == nil {
					field.Set(reflect.Zero(field.Type()))
					continue
				}
				src := reflect.ValueOf(row[i]).Interface()
				if err := assign(field.Addr().Interface(), src); err != nil {
					return fmt.Errorf("error assigning column %s: %w", col, err)
				}
			}
		}
	}
	return nil
}

func ScanStructs(cols []string, rows [][]any, dest interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("dest must be a non-nil pointer to slice")
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		return fmt.Errorf("dest must be a pointer to slice")
	}

	// Create field map once for the struct type
	elemType := v.Type().Elem()
	fieldMap := createFieldMap(elemType)

	// Create a new slice with the right capacity
	newSlice := reflect.MakeSlice(v.Type(), len(rows), len(rows))

	// Process each row
	for i, row := range rows {
		if err := scanStructWithMap(cols, row, newSlice.Index(i), fieldMap); err != nil {
			return fmt.Errorf("error scanning row %d: %w", i, err)
		}
	}

	v.Set(newSlice)
	return nil
}
