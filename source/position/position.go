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

package position

import (
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
)

// IteratorType describe position type.
type IteratorType string

const (
	TypeSnapshot = "s"
	TypeCDC      = "c"
)

// Position represents SAP Hana position.
type Position struct {
	// IteratorType - shows in what iterator was created position.
	IteratorType IteratorType

	// Snapshot information.
	// SnapshotLastProcessedVal - last processed value from ordering column.
	SnapshotLastProcessedVal any
	// SnapshotMaxValue - max value from ordering column.
	SnapshotMaxValue any

	// CDC information.
	// CDCLastID - last processed id from tracking table.
	CDCLastID int
	// TrackingTableName tracking table name.
	TrackingTableName string
}

// ParseSDKPosition parses SDK position and returns Position.
func ParseSDKPosition(p opencdc.Position) (*Position, error) {
	var pos Position

	if p == nil {
		return nil, nil //nolint:nilnil // it is ok to return nils on this case
	}

	err := json.Unmarshal(p, &pos)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshaling: %w", err)
	}

	switch pos.IteratorType {
	case TypeSnapshot, TypeCDC:
		return &pos, nil
	default:
		return nil, fmt.Errorf("%w : %s", ErrUnknownIteratorType, pos.IteratorType)
	}
}

// ConvertToSDKPosition formats and returns opencdc.Position.
func (p Position) ConvertToSDKPosition() (opencdc.Position, error) {
	pos, err := json.Marshal(p)
	if err != nil {
		return opencdc.Position{}, fmt.Errorf("marshall: %w", err)
	}

	return pos, nil
}
