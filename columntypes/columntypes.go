// Copyright © 2023 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package columntypes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// sap hana date, time column types.
	dateType       = "DATE"
	timeType       = "TIME"
	secondDateType = "SECONDDATE"
	timestampType  = "TIMESTAMP"

	// sap hana string types.
	varcharType  = "VARCHAR"
	nvarcharType = "NVARCHAR"
	clobType     = "CLOB"
	nclobType    = "NCLOB"

	// deprecated types for sap hana cloud.
	alphanumType  = "ALPHANUM"
	shortTextType = "SHORTTEXT"

	// sap hana binary types.
	varbinaryType = "VARBINARY"

	// sap hana decimal type.
	decimalType = "DECIMAL"
)

const (
	querySchemaColumnTypes = `
		SELECT 
		  COLUMN_NAME, 
		  DATA_TYPE_NAME,
		  LENGTH,
		  SCALE
		FROM 
		  TABLE_COLUMNS 
		WHERE 
		  TABLE_NAME = $1
`
	queryGetPrimaryKeys = `
		SELECT 
		  COLUMN_NAME 
		FROM 
		  CONSTRAINTS 
		WHERE 
		  TABLE_NAME = $1 
		  AND IS_PRIMARY_KEY = 'TRUE'
`
	queryIfTableExist = `SELECT count(*) AS count FROM TABLES WHERE TABLE_NAME = $1`
)

// column types where length is required parameter.
var typesWithLength = []string{varcharType, nvarcharType, varbinaryType, alphanumType, shortTextType}

// TableInfo - information about colum types, primary keys from table.
type TableInfo struct {
	// ColumnTypes - column name with column type.
	ColumnTypes map[string]string
	// PrimaryKeys - primary keys column names.
	PrimaryKeys []string
	// ColumnLengths - column name with length.
	ColumnLengths map[string]int
	// ColumnScales - column name with scale.
	ColumnScales map[string]*int
}

// GetColumnQueryPart prepare query part about creation column for tracking table.
func (t TableInfo) GetColumnQueryPart() string {
	var columns []string
	for key, val := range t.ColumnTypes {
		cl := fmt.Sprintf("%s %s", key, val)
		// add length value
		if isTypeWithRequiredLength(val) {
			cl = fmt.Sprintf("%s(%d)", cl, t.ColumnLengths[key])
		}
		// add length and scale, only for decimal type
		if val == decimalType && t.ColumnScales[key] != nil {
			cl = fmt.Sprintf("%s(%d,%d)", cl, t.ColumnLengths[key], *t.ColumnScales[key])
		}

		columns = append(columns, cl)
	}

	return strings.Join(columns, ",")
}

func isTypeWithRequiredLength(elem string) bool {
	for _, val := range typesWithLength {
		if val == elem {
			return true
		}
	}

	return false
}

// time layouts.
var layouts = []string{
	time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate,
	time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339,
	time.RFC3339Nano, time.Kitchen, time.Stamp, time.StampMilli, time.StampMicro, time.StampNano,
}

// Querier is a database querier interface needed for the GetColumnTypes function.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// GetTableInfo returns a map containing all table's columns and their database types
// and returns primary columns names.
//
//nolint:funlen,nolintlint
func GetTableInfo(ctx context.Context, querier Querier, tableName string) (TableInfo, error) {
	var primaryKeys []string

	// check if table exist.
	rows, err := querier.QueryContext(ctx, queryIfTableExist, tableName)
	if err != nil {
		return TableInfo{}, fmt.Errorf("execute query exist table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	for rows.Next() {
		var count int
		er := rows.Scan(&count)
		if er != nil {
			return TableInfo{}, fmt.Errorf("scan: %w", err)
		}

		if count == 0 {
			return TableInfo{}, fmt.Errorf("table %s doesn't exist", tableName)
		}
	}

	columnTypes := make(map[string]string)
	columnLengths := make(map[string]int)
	columnScales := make(map[string]*int)

	rows, err = querier.QueryContext(ctx, querySchemaColumnTypes, strings.ToUpper(tableName))
	if err != nil {
		return TableInfo{}, fmt.Errorf("query get column types: %w", err)
	}

	for rows.Next() {
		var (
			columnName, dataType string
			length               int
			scale                *int
		)

		if er := rows.Scan(&columnName, &dataType, &length, &scale); er != nil {
			return TableInfo{}, fmt.Errorf("scan rows: %w", er)
		}

		columnTypes[columnName] = dataType
		columnLengths[columnName] = length
		columnScales[columnName] = scale
	}

	rows, err = querier.QueryContext(ctx, queryGetPrimaryKeys, strings.ToUpper(tableName))
	if err != nil {
		return TableInfo{}, fmt.Errorf("query get column types: %w", err)
	}

	for rows.Next() {
		var columnName string

		if er := rows.Scan(&columnName); er != nil {
			return TableInfo{}, fmt.Errorf("scan rows: %w", er)
		}

		primaryKeys = append(primaryKeys, columnName)
	}

	return TableInfo{
		ColumnTypes:   columnTypes,
		PrimaryKeys:   primaryKeys,
		ColumnLengths: columnLengths,
		ColumnScales:  columnScales,
	}, nil
}

// ConvertStructureData converts a sdk.StructureData values to a proper database types.
func ConvertStructureData(
	ctx context.Context,
	columnTypes map[string]string,
	data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		// sap hana doesn't have json type or similar.
		// string types can replace it.
		switch reflect.TypeOf(value).Kind() { //nolint:exhaustive // need to check only these cases
		case reflect.Map, reflect.Slice:
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshal: %w", err)
			}

			result[key] = string(bs)

			continue
		}

		// Converting value to time if it is string.
		switch columnTypes[strings.ToLower(key)] {
		case dateType, timeType, secondDateType, timestampType:
			_, ok := value.(time.Time)
			if ok {
				result[key] = value

				continue
			}

			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			timeValue, err := parseToTime(valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert value to time.Time: %w", err)
			}

			result[key] = timeValue
		default:
			result[key] = value
		}
	}

	return result, nil
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
func TransformRow(ctx context.Context, row map[string]any, columnTypes map[string]string) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = value

			continue
		}

		switch columnTypes[key] {
		// Convert to string.
		case clobType, varcharType, nclobType, nvarcharType, alphanumType, shortTextType:
			valueBytes, ok := value.([]byte)
			if !ok {
				return nil, convertValueToBytesErr(key)
			}

			result[key] = string(valueBytes)

		default:
			result[key] = value
		}
	}

	return result, nil
}

func parseToTime(val string) (time.Time, error) {
	for _, l := range layouts {
		timeValue, err := time.Parse(l, val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, ErrInvalidTimeLayout)
}
