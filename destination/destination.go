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
	"database/sql"
	"fmt"

	"github.com/SAP/go-hdb/driver"
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-sap-hana/config"
	"github.com/conduitio-labs/conduit-connector-sap-hana/destination/writer"
)

const (
	driverName = "hdb"

	maxTableNameLength float64 = 128
)

// Destination SAP HANA Connector persists records to a sap hana database.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config config.Config
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyTable: {
			Description: "Name of the table that the connector should write to.",
			Default:     "",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		config.KeyAuthMechanism: {
			Description: "Type of the auth mechanism that connector should use",
			Default:     string(config.DSNAuthType),
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
				sdk.ValidationInclusion{List: []string{
					string(config.DSNAuthType), string(config.BasicAuthType),
					string(config.JWTAuthType), string(config.X509AuthType),
				}},
			},
		},
		config.KeyDSN: {
			Description: "The url for connection to database, required for DSN auth type",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
		config.KeyHost: {
			Description: "The host of db, required for Basic, JWT, X509 mechanisms",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
		config.KeyUsername: {
			Description: "The username, required for Basic auth mechanism",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
		config.KeyPassword: {
			Description: "The password, required for Basic auth mechanism",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
		config.KeyToken: {
			Description: "The token, required for JWT auth mechanism",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
		config.KeyClientKeyFile: {
			Description: "The path for client key file, required for X509 auth mechanism",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
		config.KeyClientCertFile: {
			Description: "The path for cert file, required for X509 auth mechanism",
			Default:     "",
			Type:        sdk.ParameterTypeString,
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := d.connectToDB(d.config.Auth)
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
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
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

func (d *Destination) connectToDB(c config.AuthConfig) (*sql.DB, error) {
	switch c.Mechanism {
	case config.DSNAuthType:
		db, err := sql.Open(driverName, c.DSN)
		if err != nil {
			return nil, fmt.Errorf("open db, DSN auth: %w", err)
		}

		return db, nil
	case config.BasicAuthType:
		connector := driver.NewBasicAuthConnector(c.Host, c.Username, c.Password)

		return sql.OpenDB(connector), nil
	case config.JWTAuthType:
		connector := driver.NewJWTAuthConnector(c.Host, c.Token)

		return sql.OpenDB(connector), nil
	case config.X509AuthType:
		connector, err := driver.NewX509AuthConnectorByFiles(c.Host, c.ClientCertFilePath, c.ClientKeyFilePath)
		if err != nil {
			return nil, fmt.Errorf("new X509 auth: %w", err)
		}

		return sql.OpenDB(connector), nil
	default:
		return nil, config.ErrInvalidAuthMechanism
	}
}
