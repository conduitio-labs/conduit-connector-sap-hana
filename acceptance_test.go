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

package saphana

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/jmoiron/sqlx"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"
)

const (
	queryCreateTestTable       = `CREATE TABLE %s (id int, name VARCHAR(100))`
	queryDropTestTable         = `DROP TABLE %s`
	queryFindTrackingTableName = `SELECT TABLE_NAME as NAME FROM TABLES WHERE 
                                  TABLE_NAME LIKE 'CONDUIT_%s_%%' LIMIT 1`

	driverName = "hdb"
	tableKey   = "table"
	dsnKey     = "auth.dsn"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int64
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	t.Helper()

	atomic.AddInt64(&d.counter, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			tableKey: d.Config.DestinationConfig[tableKey],
		},
		Key: sdk.StructuredData{
			"ID": d.counter,
		},
		Payload: sdk.Change{
			After: sdk.RawData(
				fmt.Sprintf(
					`{"ID":%d,"NAME":"%s"}`, d.counter, gofakeit.Name(),
				),
			),
		},
	}
}

func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(t, cfg),
				AfterTest:         afterTest(t, cfg),
				// Sap Hana db on cloud has bad performance about inserting rows. This timeout helps avoid context
				// deadline issue.
				WriteTimeout: 25 * time.Second,
				GoleakOptions: []goleak.Option{
					// go-hdb library leak.
					goleak.IgnoreTopFunction("github.com/SAP/go-hdb/driver.(*metrics).collect"),
				},
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(_ *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		cfg[tableKey] = table

		err := prepareData(t, cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func afterTest(_ *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		db, err := sqlx.Open(driverName, cfg[dsnKey])
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(fmt.Sprintf(queryDropTestTable, cfg[tableKey]))
		if err != nil {
			t.Errorf("exec query drop table: %v", err)
		}

		rows, err := db.Query(fmt.Sprintf(queryFindTrackingTableName, cfg[tableKey]))
		if err != nil {
			t.Errorf("exec query find table: %v", err)
		}

		defer rows.Close() //nolint:staticcheck,nolintlint

		var name string
		for rows.Next() {
			er := rows.Scan(&name)
			if er != nil {
				t.Errorf("rows scan: %v", er)
			}
		}
		if name != "" {
			_, err = db.Exec(fmt.Sprintf(queryDropTestTable, name))
			if err != nil {
				t.Errorf("exec drop table query: %v", err)
			}
		}

		if err = db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}
}

func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	dsn := os.Getenv("SAP_HANA_DSN")

	if dsn == "" {
		t.Skip("missed env variable SAP_HANA_DSN")
	}

	return map[string]string{
		"auth.mechanism": "DSN",
		"auth.dsn":       dsn,
		"orderingColumn": "ID",
		"snapshot":       "true",
		"batchSize":      "100",
	}
}

func prepareData(t *testing.T, cfg map[string]string) error {
	t.Helper()

	db, err := sql.Open(driverName, cfg[dsnKey])
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestTable, cfg[tableKey]))
	if err != nil {
		return fmt.Errorf("execute create test table query: %w", err)
	}

	db.Close()

	return nil
}

func randomIdentifier(t *testing.T) string {
	t.Helper()

	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
