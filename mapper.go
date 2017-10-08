package flexsql

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

var (
	ErrPtrToOutputRowMustBePtr    = errors.New("ptrToOutputRow must be pointer")
	ErrOutputRowMustBeStruct      = errors.New("outputRow must be struct")
	ErrInvalidColumnName          = errors.New("Invalid column name")
	ErrUnknownField               = errors.New("Unknown field")
	ErrOutputRowFieldMustBeStruct = errors.New("Fields of outputRow must be struct")
)

// Mapper maps columns into structs using reflection.
//
// The zero value is ready for use. Mapper lazily
// initializes itself upon the first invocation of Scan.
// The internal state of Mapper is bound to the column names and
// the type of the struct. Therefore it should be constructed
// everytime you obtain an instance of sql.Rows.
//
// Example
//  type TwitterUser struct {
//  	Name string
//  }
//  type OutputRow struct {
//  	Follower      TwitterUser
//  	BeingFollowed TwiiterUser
//  }
//  stmt := `SELECT 'Alice' "Follower_Name", 'Rob' "BeingFollowed_Name"`
//  rows, _ := db.Query(stmt)
//  mapper := &Mapper{}
//  for rows.Next() {
//  	var outputRow OutputRow
//  	_ = mapper.Scan(rows, &outputRow)
//  }
type Mapper struct {
	indexPaths [][]int
}

func (m *Mapper) Scan(rows *sql.Rows, ptrToOutputRow interface{}) error {
	value := reflect.ValueOf(ptrToOutputRow)
	if value.Kind() != reflect.Ptr {
		return ErrPtrToOutputRowMustBePtr
	}

	elem := value.Elem()
	if elem.Kind() != reflect.Struct {
		return ErrOutputRowMustBeStruct
	}

	if m.indexPaths == nil {
		outputRowType := reflect.TypeOf(ptrToOutputRow).Elem()
		if err := m.init(rows, outputRowType); err != nil {
			return err
		}
	}

	dest := make([]interface{}, len(m.indexPaths))
	for i, indexPath := range m.indexPaths {
		dest[i] = elem.FieldByIndex(indexPath).Addr().Interface()
	}
	return rows.Scan(dest...)
}

func (m *Mapper) init(rows *sql.Rows, outputRowType reflect.Type) error {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	indexPaths := make([][]int, len(columnTypes))
	for i, c := range columnTypes {
		name := c.Name()
		splits := strings.SplitN(name, "_", 2)
		if len(splits) < 2 {
			return ErrInvalidColumnName
		}
		structField, ok := outputRowType.FieldByName(splits[0])
		if !ok {
			return ErrUnknownField
		}
		if structField.Type.Kind() != reflect.Struct {
			return ErrOutputRowFieldMustBeStruct
		}
		nestedField, ok := structField.Type.FieldByName(splits[1])
		if !ok {
			return ErrUnknownField
		}
		indexPaths[i] = []int{structField.Index[0], nestedField.Index[0]}
	}

	m.indexPaths = indexPaths
	return nil
}
