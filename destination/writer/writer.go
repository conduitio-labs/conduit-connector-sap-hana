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

package writer

import (
	"context"
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-sap-hana/columntypes"
)

const (
	// metadata related.
	metadataTable = "saphana.table"
)

// Writer implements a writer logic for Sap hana destination.
type Writer struct {
	db          *sqlx.DB
	table       string
	columnTypes map[string]string
}

// Params is an incoming params for the New function.
type Params struct {
	DB    *sqlx.DB
	Table string
}

// New creates new instance of the Writer.
func New(ctx context.Context, params Params) (*Writer, error) {
	writer := &Writer{
		db:    params.DB,
		table: params.Table,
	}

	tableInfo, err := columntypes.GetTableInfo(ctx, writer.db, writer.table)
	if err != nil {
		return nil, fmt.Errorf("get table info: %w", err)
	}

	writer.columnTypes = tableInfo.ColumnTypes

	return writer, nil
}

// Close closes the underlying db connection.
func (w *Writer) Close(ctx context.Context) error {
	err := w.db.Close()
	if err != nil {
		return fmt.Errorf("close db: %w", err)
	}

	return nil
}

// Delete deletes records by a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	keys, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	if len(keys) == 0 {
		return ErrNoKey
	}

	query, args := w.buildDeleteQuery(tableName, keys)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}

// Update updates records by a key.
func (w *Writer) Update(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrNoPayload
	}

	payload, err = columntypes.ConvertStructureData(ctx, w.columnTypes, payload)
	if err != nil {
		return fmt.Errorf("convert structure data: %w", err)
	}

	keys, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	if len(keys) == 0 {
		return ErrNoKey
	}

	query, args := w.buildUpdateQuery(tableName, keys, payload)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec update: %w", err)
	}

	return nil
}

// gettableName returns either the records metadata value for table
// or the default configured value for table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.table
	}

	return tableName
}

// Insert row to sql server db.
func (w *Writer) Insert(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrNoPayload
	}

	payload, err = columntypes.ConvertStructureData(ctx, w.columnTypes, payload)
	if err != nil {
		return fmt.Errorf("convert structure data: %w", err)
	}

	columns, values := w.extractColumnsAndValues(payload)

	query, args := w.buildInsertQuery(tableName, columns, values)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec insert: %w", err)
	}

	return nil
}

// buildDeleteQuery generates an SQL DELETE statement query,
// based on the provided table, and keys.
func (w *Writer) buildDeleteQuery(table string, keys map[string]any) (string, []any) {
	db := sqlbuilder.NewDeleteBuilder()

	db.DeleteFrom(table)

	for key, val := range keys {
		db.Where(
			db.Equal(key, val),
		)
	}

	query, args := db.Build()

	return query, args
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// extractColumnsAndValues turns the payload into slices of
// columns and values.
func (w *Writer) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		columns []string
		values  []any
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}

func (w *Writer) buildInsertQuery(table string, columns []string, values []any) (string, []any) {
	sb := sqlbuilder.NewInsertBuilder()

	sb.InsertInto(table)
	sb.Cols(columns...)
	sb.Values(values...)

	return sb.Build()
}

func (w *Writer) buildUpdateQuery(table string, keys, payload map[string]any) (string, []any) {
	up := sqlbuilder.NewUpdateBuilder()

	up.Update(table)

	setVal := make([]string, 0)
	for key, val := range payload {
		setVal = append(setVal, up.Assign(key, val))
	}

	up.Set(setVal...)

	for key, val := range keys {
		up.Where(
			up.Equal(key, val),
		)
	}

	return up.Build()
}
