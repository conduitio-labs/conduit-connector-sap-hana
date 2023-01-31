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

package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

const (
	driverName = "hdb"
	dsnKey     = "auth.dsn"
)

const (
	queryCreateTable = `
		CREATE TABLE %s(
			id INT NOT NULL PRIMARY KEY,
			cl_bigint BIGINT,
			cl_tinyint TINYINT,
			cl_varchar VARCHAR(40),
			cl_nvarchar VARCHAR(10),
			cl_boolean BOOLEAN,
			cl_date DATE,
			cl_decimal DECIMAL,
			cl_custom_decimal DECIMAL(4,1),
			cl_varbinary VARBINARY(20)
		)
`
	queryInsertFirstRow = `INSERT INTO %s VALUES ( 1, 145, 11, 'tr1', 'ntr1', true, '2018-01-01', 
                          1646.67, 14.1, x'47a26163a06176f6' )`
	queryInsertSecondRow = `INSERT INTO %s VALUES ( 2, 245, 22, 'tr2', 'ntr2', true, '2019-01-01', 
                          2646.67, 24.1, x'47a26163a06176f6' )`
	queryInsertThirdRow = `INSERT INTO %s VALUES ( 3, 345, 32, 'tr3', 'ntr3', false, '2020-01-01', 
                          3646.67, 34.1, x'47a26163a06176f6' )`

	queryDropTable = `DROP TABLE %s`

	queryFindTrackingTable = `SELECT TABLE_NAME as NAME FROM TABLES WHERE 
                                  TABLE_NAME LIKE 'CONDUIT_%s_%%' LIMIT 1`

	queryUpdate = `
		UPDATE %s SET cl_varchar = 'update' WHERE id = 1
		
`
	queryDelete = `
		DELETE FROM %s WHERE id = 1
		`
)

func TestSource_Snapshot_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sqlx.Open(driverName, cfg[dsnKey])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// prepare data
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertFirstRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertSecondRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertThirdRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		er := clearData(ctx, db, tableName)
		if er != nil {
			t.Log(er)
		}

		db.Close()
	})

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// check first record.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedFirstRecord := map[string]any{
		"CL_BIGINT": 145, "CL_BOOLEAN": true, "CL_CUSTOM_DECIMAL": "141/10",
		"CL_DATE": "2018-01-01T00:00:00Z", "CL_DECIMAL": "164667/100", "CL_NVARCHAR": "ntr1", "CL_TINYINT": 11,
		"CL_VARBINARY": "R6JhY6BhdvY=", "CL_VARCHAR": "tr1", "ID": 1,
	}

	firstRecordBytes, err := json.Marshal(wantedFirstRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(firstRecordBytes, r.Payload.After.Bytes())

	// check second record.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedSecondRecord := map[string]any{
		"CL_BIGINT": 245, "CL_BOOLEAN": true, "CL_CUSTOM_DECIMAL": "241/10",
		"CL_DATE": "2019-01-01T00:00:00Z", "CL_DECIMAL": "264667/100", "CL_NVARCHAR": "ntr2", "CL_TINYINT": 22,
		"CL_VARBINARY": "R6JhY6BhdvY=", "CL_VARCHAR": "tr2", "ID": 2,
	}

	secondRecordBytes, err := json.Marshal(wantedSecondRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(secondRecordBytes, r.Payload.After.Bytes())

	// check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// let's continue from last processed position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Fatal(err)
	}

	// check third record.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedThirdRecord := map[string]any{
		"CL_BIGINT": 345, "CL_BOOLEAN": false, "CL_CUSTOM_DECIMAL": "341/10",
		"CL_DATE": "2020-01-01T00:00:00Z", "CL_DECIMAL": "364667/100", "CL_NVARCHAR": "ntr3", "CL_TINYINT": 32,
		"CL_VARBINARY": "R6JhY6BhdvY=", "CL_VARCHAR": "tr3", "ID": 3,
	}

	wantedThirdBytes, err := json.Marshal(wantedThirdRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(wantedThirdBytes, r.Payload.After.Bytes())

	// check ErrBackoffRetry.
	r, err = s.Read(ctx)
	is.Equal(sdk.ErrBackoffRetry, err)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Empty_Table(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sqlx.Open(driverName, cfg[dsnKey])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// prepare data
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		er := clearData(ctx, db, tableName)
		if er != nil {
			t.Log(er)
		}

		db.Close()
	})

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// check ErrBackoffRetry.
	_, err = s.Read(ctx)
	is.Equal(sdk.ErrBackoffRetry, err)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Key_From_Config(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	// set key.
	cfg["primaryKeys"] = "cl_tinyint"

	db, err := sqlx.Open(driverName, cfg[dsnKey])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// prepare data
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertFirstRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		er := clearData(ctx, db, tableName)
		if er != nil {
			t.Log(er)
		}

		db.Close()
	})

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// check ErrBackoffRetry.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey := map[string]any{"CL_TINYINT": int64(11)}

	wantedKeyBytes, err := json.Marshal(wantedKey)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(r.Key.Bytes(), wantedKeyBytes)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Key_From_Table(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sqlx.Open(driverName, cfg[dsnKey])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// prepare data
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertFirstRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		er := clearData(ctx, db, tableName)
		if er != nil {
			t.Log(er)
		}

		db.Close()
	})

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey := map[string]any{"ID": int64(1)}

	wantedKeyBytes, err := json.Marshal(wantedKey)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(r.Key.Bytes(), wantedKeyBytes)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sqlx.Open(driverName, cfg[dsnKey])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// prepare data
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		er := clearData(ctx, db, tableName)
		if er != nil {
			t.Log(er)
		}

		db.Close()
	})

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertFirstRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// update row.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryUpdate, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// delete row.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryDelete, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	is.Equal(sdk.ErrBackoffRetry, err)

	// check inserted data.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedRecord := map[string]any{
		"CL_BIGINT": 145, "CL_BOOLEAN": true, "CL_CUSTOM_DECIMAL": "141/10",
		"CL_DATE": "2018-01-01T00:00:00Z", "CL_DECIMAL": "164667/100", "CL_NVARCHAR": "ntr1", "CL_TINYINT": 11,
		"CL_VARBINARY": "R6JhY6BhdvY=", "CL_VARCHAR": "tr1", "ID": 1,
	}

	wantedRecordBytes, err := json.Marshal(wantedRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(wantedRecordBytes, r.Payload.After.Bytes())
	is.Equal(sdk.OperationCreate, r.Operation)

	// check updated data.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedRecord = map[string]any{
		"CL_BIGINT": 145, "CL_BOOLEAN": true, "CL_CUSTOM_DECIMAL": "141/10",
		"CL_DATE": "2018-01-01T00:00:00Z", "CL_DECIMAL": "164667/100", "CL_NVARCHAR": "ntr1", "CL_TINYINT": 11,
		"CL_VARBINARY": "R6JhY6BhdvY=", "CL_VARCHAR": "update", "ID": 1,
	}

	wantedRecordBytes, err = json.Marshal(wantedRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(wantedRecordBytes, r.Payload.After.Bytes())
	is.Equal(sdk.OperationUpdate, r.Operation)

	// check deleted data.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(sdk.OperationDelete, r.Operation)

	// check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Off(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sqlx.Open(driverName, cfg[dsnKey])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	// prepare data
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, tableName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertFirstRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		er := clearData(ctx, db, tableName)
		if er != nil {
			t.Log(er)
		}

		db.Close()
	})

	// turn off snapshot
	cfg["snapshot"] = "false"

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// update.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryUpdate, tableName))
	if err != nil {
		t.Fatal(err)
	}

	r, err := s.Read(ctx)
	if !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatal(err)
	}

	// Check read. Snapshot data must be missed.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(r.Operation, sdk.OperationUpdate) {
		t.Fatal(errors.New("not wanted type"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareConfigMap(table string) (map[string]string, error) {
	dsn := os.Getenv("SAP_HANA_DSN")

	if dsn == "" {
		return map[string]string{}, errors.New("missed env variable SAP_HANA_DSN")
	}

	return map[string]string{
		"auth.mechanism": "DSN",
		"auth.dsn":       dsn,
		"table":          table,
		"orderingColumn": "id",
		"snapshot":       "true",
		"batchSize":      "100",
	}, nil
}

func clearData(ctx context.Context, db *sqlx.DB, tableName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
	if err != nil {
		return fmt.Errorf("exec query drop table: %w", err)
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf(queryFindTrackingTable, tableName))
	if err != nil {
		return fmt.Errorf("exec query find table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	var name string
	for rows.Next() {
		er := rows.Scan(&name)
		if er != nil {
			return fmt.Errorf("rows scan: %w", er)
		}
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, name))
	if err != nil {
		return fmt.Errorf("exec drop table query: %w", err)
	}

	return nil
}

func randomIdentifier(t *testing.T) string {
	t.Helper()

	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
