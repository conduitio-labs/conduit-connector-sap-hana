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

package helper

import (
	"database/sql"
	"fmt"

	"github.com/SAP/go-hdb/driver"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-sap-hana/config"
)

const (
	driverName = "hdb"
)

// ConnectToDB - connect to Sap Hana db.
func ConnectToDB(c config.AuthConfig) (*sqlx.DB, error) {
	switch c.Mechanism {
	case config.DSNAuthType:
		db, err := sqlx.Open(driverName, c.DSN)
		if err != nil {
			return nil, fmt.Errorf("open db, DSN auth: %w", err)
		}

		return db, nil
	case config.BasicAuthType:
		con := driver.NewBasicAuthConnector(c.Host, c.Username, c.Password)

		return sqlx.NewDb(sql.OpenDB(con), driverName), nil
	case config.JWTAuthType:
		con := driver.NewJWTAuthConnector(c.Host, c.Token)

		return sqlx.NewDb(sql.OpenDB(con), driverName), nil
	case config.X509AuthType:
		con, err := driver.NewX509AuthConnectorByFiles(c.Host, c.ClientCertFilePath, c.ClientKeyFilePath)
		if err != nil {
			return nil, fmt.Errorf("new X509 auth: %w", err)
		}

		return sqlx.NewDb(sql.OpenDB(con), driverName), nil
	default:
		return nil, fmt.Errorf("invalid auth mechanism :%s", c.Mechanism)
	}
}
