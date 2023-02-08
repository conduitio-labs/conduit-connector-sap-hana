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
	"errors"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	snapshotPos := Position{
		IteratorType:             TypeSnapshot,
		SnapshotLastProcessedVal: 1,
		SnapshotMaxValue:         4,
		CDCLastID:                0,
		TrackingTableName:        "test",
	}

	wrongPosType := Position{
		IteratorType:             "i",
		SnapshotLastProcessedVal: 1,
		SnapshotMaxValue:         4,
		CDCLastID:                0,
		TrackingTableName:        "test",
	}

	snapshotPosBytes, _ := json.Marshal(snapshotPos)

	wrongPosBytes, _ := json.Marshal(wrongPosType)

	tests := []struct {
		name        string
		in          sdk.Position
		want        Position
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid position",
			in:   sdk.Position(snapshotPosBytes),
			want: snapshotPos,
		},
		{
			name:        "unknown iterator type",
			in:          sdk.Position(wrongPosBytes),
			wantErr:     true,
			expectedErr: errors.New("unknown iterator type : i").Error(),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseSDKPosition(tt.in)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}
		})
	}
}
