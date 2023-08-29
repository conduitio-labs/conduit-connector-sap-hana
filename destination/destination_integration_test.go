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

package destination

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

const (
	// queries.
	queryCreateTable = `
		CREATE TABLE %s(
			id INT NOT NULL PRIMARY KEY,
			cl_bigint BIGINT,
			cl_tinyint TINYINT,
			cl_varchar VARCHAR(40),
			cl_nvarchar VARCHAR(25),
			cl_boolean BOOLEAN,
			cl_date DATE,
			cl_decimal DECIMAL,
			cl_varbinary VARBINARY(20)
		)
`
	queryCreateTableDecimalTest = `
		CREATE TABLE %s(
			dec_1 DECIMAL,
			dec_2 DECIMAL,
			dec_3 DECIMAL,
			dec_4 DECIMAL,
			dec_5 DECIMAL,
			dec_6 DECIMAL
		)
`
	queryDropTable = `
  		 DROP TABLE %s;
`
)

const (
	driverName = "hdb"
	dsnKey     = "auth.dsn"
)

func TestIntegrationDestination_Write_Insert_Success(t *testing.T) {
	preparedID := 1

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

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	})

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":          preparedID,
		"cl_bigint":   321765482,
		"cl_tinyint":  2,
		"cl_varchar":  "test",
		"cl_nvarchar": "some test text",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_boolean":   true,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
	}

	count, err := dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	// check if row exist by id
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id FROM %s", tableName))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	var id int
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			t.Error(err)
		}
	}
	if rows.Err() != nil {
		t.Errorf("iterate rows error: %s", rows.Err())
	}

	if id != preparedID {
		t.Error(errors.New("id and prepared id not equal"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestIntegrationDestination_Write_Update_Success(t *testing.T) {
	preparedVarchar := "updated_test"

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

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	})

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":          1,
		"cl_bigint":   321765482,
		"cl_tinyint":  2,
		"cl_varchar":  "test",
		"cl_nvarchar": "some test text",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_boolean":   true,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
	}

	count, err := dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	preparedData["cl_varchar"] = preparedVarchar

	_, err = dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationUpdate,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	// check if value was updated
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT cl_varchar FROM %s", tableName))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	var clVarchar string
	for rows.Next() {
		err = rows.Scan(&clVarchar)
		if err != nil {
			t.Error(err)
		}
	}
	if rows.Err() != nil {
		t.Errorf("iterate rows error: %s", rows.Err())
	}

	if clVarchar != preparedVarchar {
		t.Error(errors.New("clVarchar and preparedVarchar not equal"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestIntegrationDestination_Write_Update_Composite_Keys_Success(t *testing.T) {
	preparedVarchar := "updated_test"

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

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	})

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":          1,
		"cl_bigint":   321765482,
		"cl_tinyint":  2,
		"cl_varchar":  "test",
		"cl_nvarchar": "some test text",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_boolean":   true,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
	}

	count, err := dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	preparedData["cl_varchar"] = preparedVarchar

	_, err = dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationUpdate,
			Key:       sdk.StructuredData{"id": "1", "cl_tinyint": 2},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	// check if value was updated
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT cl_varchar FROM %s", tableName))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	var clVarchar string
	for rows.Next() {
		err = rows.Scan(&clVarchar)
		if err != nil {
			t.Error(err)
		}
	}
	if rows.Err() != nil {
		t.Errorf("iterate rows error: %s", rows.Err())
	}

	if clVarchar != preparedVarchar {
		t.Error(errors.New("clVarchar and preparedVarchar not equal"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestIntegrationDestination_Write_Delete_Success(t *testing.T) {
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

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	})

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":          1,
		"cl_bigint":   321765482,
		"cl_tinyint":  2,
		"cl_varchar":  "test",
		"cl_nvarchar": "some test text",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_boolean":   true,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
	}

	count, err := dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	count, err = dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationDelete,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}

	// check if row exist by id
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tableName))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Error(err)
		}
	}
	if rows.Err() != nil {
		t.Errorf("iterate rows error: %s", rows.Err())
	}

	if count != 0 {
		t.Error(errors.New("count not zero"))
	}
}

func TestIntegrationDestination_Decimal_Transformation(t *testing.T) {
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
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTableDecimalTest, tableName))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
		if err != nil {
			t.Fatal(err)
		}

		db.Close()
	})

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"dec_1": 103.6548,
		"dec_2": "103.6548",
		"dec_3": "1036548/1000",
		"dec_4": int64(103),
		"dec_5": int32(103),
		"dec_6": float32(103.6548),
	}

	count, err := dest.Write(ctx, []sdk.Record{
		{
			Payload:   sdk.Change{After: sdk.StructuredData(preparedData)},
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func prepareConfigMap(table string) (map[string]string, error) {
	dsn := os.Getenv("SAP_HANA_DSN")

	if dsn == "" {
		return map[string]string{}, errors.New("empty env variable SAP_HANA_DSN")
	}

	return map[string]string{
		"auth.mechanism": "DSN",
		"auth.dsn":       dsn,
		"table":          table,
	}, nil
}

func randomIdentifier(t *testing.T) string {
	t.Helper()

	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
