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

package columntypes

import (
	"errors"
	"fmt"
)

var (
	ErrValueIsNotAString                = errors.New("value is not a string")
	ErrCannotConvertValueToBytes        = errors.New("cannot convert value to byte slice")
	ErrCannotConvertValueToDecimal      = errors.New("cannot convert value to decimal")
	ErrInvalidDecimalStringPresentation = errors.New("invalid decimal string presentation")
	ErrCannotConvertToInt               = errors.New("cannot convert value to int type")
	ErrInvalidTimeLayout                = errors.New("invalid time layout")
)

// convertValueToBytesErr returns the formatted ErrCannotConvertValueToBytes error.
func convertValueToBytesErr(name string) error {
	return fmt.Errorf("%w: %q", ErrCannotConvertValueToBytes, name)
}
