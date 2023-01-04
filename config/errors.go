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

package config

import (
	"errors"
)

var (
	// ErrInvalidAuthMechanism occurs when there's invalid mechanism config value.
	ErrInvalidAuthMechanism = errors.New("invalid auth mechanism")
	// errRequiredDSNParameter occurs when there's empty dsn parameter for dsn auth.
	errRequiredDSNParameter = errors.New("dsn is required parameter for dsn auth")
	// errRequiredHostParameter occurs when there's empty host parameter for basic or jwt or x509 auth.
	errRequiredHostParameter = errors.New("host is required parameter for basic, jwt, x509 auth")
	// errRequiredUsernameParameter occurs when there's empty username parameter for basic auth.
	errRequiredUsernameParameter = errors.New("username is required parameter for basic auth")
	// errRequiredPasswordParameter occurs when there's empty password parameter for basic auth.
	errRequiredPasswordParameter = errors.New("password is required parameter for basic auth")
	// errRequiredTokenParameter occurs when there's empty token parameter for jwt auth.
	errRequiredTokenParameter = errors.New("token is required for jwt auth")
	// errRequiredClientCertFileParameter occurs when there's empty client cert file parameter for x509 auth.
	errRequiredClientCertFileParameter = errors.New("client cert file path is required for x509 auth")
	// errRequiredClientCertFileParameter occurs when there's empty client key file path parameter for x509 auth.
	errRequiredClientKeyFileParameter = errors.New("client key file path is required for x509 auth")
)
