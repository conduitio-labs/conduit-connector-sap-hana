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
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-sap-hana/helper"
	"github.com/conduitio-labs/conduit-connector-sap-hana/source/iterator"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	if err := sdk.Util.ParseConfig(cfg, &s.config); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if err := s.config.Auth.Validate(); err != nil {
		return fmt.Errorf("validate auth config: %w", err)
	}

	// Column names and table are uppercase for Sap Hana database.
	s.config.OrderingColumn = strings.ToUpper(s.config.OrderingColumn)
	s.config.Table = strings.ToUpper(s.config.Table)

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	db, err := helper.ConnectToDB(s.config.Auth)
	if err != nil {
		return fmt.Errorf("connect to db: %w", err)
	}

	if err = db.Ping(); err != nil {
		if err != nil {
			return fmt.Errorf("ping db: %w", err)
		}
	}

	s.iterator, err = iterator.NewCombinedIterator(
		ctx,
		iterator.CombinedParams{
			DB:             db,
			Table:          s.config.Table,
			OrderingColumn: s.config.OrderingColumn,
			CfgKeys:        s.config.PrimaryKeys,
			BatchSize:      s.config.BatchSize,
			Snapshot:       s.config.Snapshot,
			SdkPosition:    rp,
		},
	)
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	return nil
}

// Read gets the next object from the Sap Hana db.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("source has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("source next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return fmt.Errorf("stop iterator %w", err)
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p sdk.Position) error {
	err := s.iterator.Ack(ctx, p)
	if err != nil {
		return fmt.Errorf("iterator ack: %w", err)
	}

	return nil
}
