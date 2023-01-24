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
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-sap-hana/columntypes"
	"github.com/conduitio-labs/conduit-connector-sap-hana/source/position"
)

const (
	metadataTable = "saphana.table"
)

// CombinedIterator combined iterator.
type CombinedIterator struct {
	db *sqlx.DB

	snapshot *snapshotIterator
	cdc      *cdcIterator

	// table - table name.
	table string
	// trackingTable - tracking table name.
	trackingTable string
	// keys Names of columns what iterator use for setting key in record.
	keys []string
	// orderingColumn Name of column what iterator use for sorting data.
	orderingColumn string
	// batchSize size of batch.
	batchSize int
	// tableInfo - general information about column types, primary keys.
	tableInfo columntypes.TableInfo
}

// CombinedParams is an incoming params for the [NewCombinedIterator] function.
type CombinedParams struct {
	DB             *sqlx.DB
	Table          string
	OrderingColumn string
	CfgKeys        []string
	BatchSize      int
	Snapshot       bool
	SdkPosition    sdk.Position
}

// NewCombinedIterator - create new iterator.
func NewCombinedIterator(ctx context.Context, params CombinedParams) (*CombinedIterator, error) {
	pos, err := position.ParseSDKPosition(params.SdkPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	suffixName := getSuffixName(pos)

	it := &CombinedIterator{
		db:             params.DB,
		table:          params.Table,
		orderingColumn: params.OrderingColumn,
		batchSize:      params.BatchSize,
		trackingTable:  fmt.Sprintf(trackingTablePattern, params.Table, suffixName),
	}

	it.tableInfo, err = columntypes.GetTableInfo(ctx, params.DB, params.Table)
	if err != nil {
		return nil, fmt.Errorf("get table info: %w", err)
	}

	it.setKeys(params.CfgKeys)

	err = setupCDC(ctx, it.db, it.table, it.trackingTable, suffixName, it.tableInfo)
	if err != nil {
		return nil, fmt.Errorf("setup cdc: %w", err)
	}

	if params.Snapshot && (pos == nil || pos.IteratorType == position.TypeSnapshot) {
		it.snapshot, err = newSnapshotIterator(ctx, snapshotParams{
			db:             it.db,
			table:          it.table,
			orderingColumn: it.orderingColumn,
			keys:           it.keys,
			batchSize:      it.batchSize,
			position:       pos,
			columnTypes:    it.tableInfo.ColumnTypes,
			suffixName:     suffixName,
		})
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	} else {
		it.cdc, err = newCDCIterator(
			ctx,
			cdcParams{
				db:            it.db,
				table:         it.table,
				trackingTable: it.trackingTable,
				keys:          it.keys,
				batchSize:     it.batchSize,
				columnTypes:   it.tableInfo.ColumnTypes,
				position:      pos,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("new cdc iterator: %w", err)
		}
	}

	return it, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
// If the underlying snapshot iterator returns false, the combined iterator will try to switch to the cdc iterator.
func (c *CombinedIterator) HasNext(ctx context.Context) (bool, error) {
	switch {
	case c.snapshot != nil:
		hasNext, err := c.snapshot.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if !hasNext {
			if er := c.switchToCDCIterator(ctx); er != nil {
				return false, fmt.Errorf("switch to cdc iterator: %w", err)
			}

			return false, nil
		}

		return true, nil

	case c.cdc != nil:
		return c.cdc.HasNext(ctx)

	default:
		return false, nil
	}
}

// Next returns the next record.
func (c *CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.Next(ctx)

	case c.cdc != nil:
		return c.cdc.Next(ctx)

	default:
		return sdk.Record{}, ErrNoInitializedIterator
	}
}

// Stop the underlying iterators.
func (c *CombinedIterator) Stop() error {
	if c.snapshot != nil {
		return c.snapshot.Stop()
	}

	if c.cdc != nil {
		return c.cdc.Stop()
	}

	return nil
}

// Ack check if record with position was recorded.
func (c *CombinedIterator) Ack(ctx context.Context, rp sdk.Position) error {
	pos, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	if pos.IteratorType == position.TypeCDC {
		return c.cdc.Ack(ctx, pos)
	}

	return nil
}

func (c *CombinedIterator) switchToCDCIterator(ctx context.Context) error {
	err := c.snapshot.CloseRows()
	if err != nil {
		return fmt.Errorf("close rows: %w", err)
	}

	c.snapshot = nil

	c.cdc, err = newCDCIterator(
		ctx,
		cdcParams{
			db:            c.db,
			table:         c.table,
			trackingTable: c.trackingTable,
			keys:          c.keys,
			batchSize:     c.batchSize,
			columnTypes:   c.tableInfo.ColumnTypes,
			position:      nil,
		},
	)
	if err != nil {
		return fmt.Errorf("new cdc iterator: %w", err)
	}

	return nil
}

func (c *CombinedIterator) setKeys(cfgKeys []string) {
	// first priority keys from config.
	if len(cfgKeys) > 0 {
		for i := range cfgKeys {
			cfgKeys[i] = strings.ToUpper(cfgKeys[i])
		}

		c.keys = cfgKeys

		return
	}

	// second priority primary keys from table.
	if len(c.keys) > 0 {
		return
	}

	// last priority ordering column.
	c.keys = []string{c.orderingColumn}
}

func getSuffixName(pos *position.Position) string {
	// get suffix from position
	if pos != nil {
		return pos.SuffixName
	}

	// create new suffix
	return time.Now().Format("150405")
}
