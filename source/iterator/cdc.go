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

package iterator

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-sap-hana/columntypes"
	"github.com/conduitio-labs/conduit-connector-sap-hana/source/position"
)

const (
	trackingTablePattern = "CONDUIT_TRACKING_%s"
	triggerNamePattern   = "cd_%s_%s"

	// tracking table columns.
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTrackingID    = "CONDUIT_TRACKING_ID"

	// operation type
	insertOperation = "INSERT"
	updateOperation = "UPDATE"
	deleteOperation = "DELETE"
)

// cdcIterator - cdc iterator.
type cdcIterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// table - table name.
	table string
	// trackingTable - tracking table name.
	trackingTable string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// keys Names of columns what iterator use for setting key in record.
	keys []string
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
	// columnTypes column types from table.
	columnTypes map[string]string
}

// setupCDC - create tracking table, add columns.
func setupCDC(
	ctx context.Context,
	db *sqlx.DB,
	tableName, trackingTableName string,
	tableInfo columntypes.TableInfo,
) error {
	var (
		trackingTableExist bool
	)

	triggerInsertName := fmt.Sprintf(triggerNamePattern, tableName, insertOperation)
	triggerUpdateName := fmt.Sprintf(triggerNamePattern, tableName, updateOperation)
	triggerDeleteName := fmt.Sprintf(triggerNamePattern, tableName, deleteOperation)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	// check if table exist.
	rows, err := tx.QueryContext(ctx, queryIfTableExist, trackingTableName)
	if err != nil {
		return fmt.Errorf("query exist table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	for rows.Next() {
		var count int
		er := rows.Scan(&count)
		if er != nil {
			return fmt.Errorf("scan: %w", err)
		}

		if count == 1 {
			trackingTableExist = true
		}
	}

	if !trackingTableExist {
		// create tracking table
		_, err = tx.ExecContext(ctx, fmt.Sprintf(queryCreateTable, trackingTableName, tableInfo.GetColumnQueryPart(),
			columnOperationType, columnTrackingID))
		if err != nil {
			return fmt.Errorf("create tracking table: %w", err)
		}
	}

	columnNames := make([]string, len(tableInfo.ColumnTypes))
	nwVal := make([]string, len(tableInfo.ColumnTypes))
	olVal := make([]string, len(tableInfo.ColumnTypes))

	i := 0
	for key := range tableInfo.ColumnTypes {
		columnNames[i] = key
		nwVal[i] = fmt.Sprintf(":nw.%s", key)
		olVal[i] = fmt.Sprintf(":rw.%s", key)
		i++
	}

	columnNames = append(columnNames, columnOperationType)

	// add trigger to catch insert.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddInsertTrigger, triggerInsertName, tableName, trackingTableName,
		strings.Join(columnNames, ","), strings.Join(nwVal, ",")))
	if err != nil {
		sdk.Logger(ctx).Error().Msgf("failed query:%s", fmt.Sprintf(queryAddInsertTrigger,
			triggerInsertName, tableName, trackingTableName,
			strings.Join(columnNames, ","), strings.Join(nwVal, ",")))
		return fmt.Errorf("add trigger catch insert: %w", err)
	}

	// add trigger to catch update.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryUpdateTrigger, triggerUpdateName, tableName, trackingTableName,
		strings.Join(columnNames, ","), strings.Join(nwVal, ",")))
	if err != nil {
		return fmt.Errorf("add trigger catch update: %w", err)
	}

	// add trigger to catch delete.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryDeleteTrigger, triggerDeleteName, tableName, trackingTableName,
		strings.Join(columnNames, ","), strings.Join(nwVal, ",")))
	if err != nil {
		return fmt.Errorf("add trigger catch delete: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
