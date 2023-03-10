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
	"github.com/conduitio-labs/conduit-connector-sap-hana/config"
)

// Config holds source specific configurable values.
type Config struct {
	config.Config

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" default:"1000" validate:"gt=0,lt=10001"`
	// PrimaryKeys list of column names should use for their `Key` fields.
	PrimaryKeys []string `json:"primaryKeys"`
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Snapshot bool `json:"snapshot" default:"true"`
}
