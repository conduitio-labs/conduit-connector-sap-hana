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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-sap-hana/destination/writer"
	"github.com/conduitio-labs/conduit-connector-sap-hana/helper"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Destination SAP HANA Connector persists records to a sap hana database.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config Config
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Parameters returns a map of named config.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() config.Parameters {
	return d.config.Parameters()
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	if err := sdk.Util.ParseConfig(ctx, cfg, &d.config, New().Parameters()); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	if err := d.config.Auth.Validate(); err != nil {
		return fmt.Errorf("validate auth config: %w", err)
	}

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := helper.ConnectToDB(d.config.Auth)
	if err != nil {
		return fmt.Errorf("connect to db: %w", err)
	}

	if err = db.Ping(); err != nil {
		if err != nil {
			return fmt.Errorf("ping db: %w", err)
		}
	}

	d.writer, err = writer.New(ctx, writer.Params{
		DB:    db,
		Table: d.config.Table,
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		err := d.writer.Close(ctx)
		if err != nil {
			return fmt.Errorf("destination teardown : %w", err)
		}
	}

	return nil
}
