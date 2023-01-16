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

const (
	// DSNAuthType name of DSN auth.
	DSNAuthType string = "DSN"
	// BasicAuthType name of Basic auth.
	BasicAuthType string = "Basic"
	// JWTAuthType name of JWT auth.
	JWTAuthType string = "JWT"
	// X509AuthType name of X509 auth.
	X509AuthType string = "X509"
)

// Config contains configurable values
// shared between source and destination SAP HANA connector.
type Config struct {
	// Table is a name of the table that the connector should write to or read from.
	Table string `json:"table" validate:"required"`

	Auth AuthConfig
}

// AuthConfig auth parameters.
type AuthConfig struct {
	// Mechanism type of auth. Valid types: DSN, Basic, JWT, X509.
	Mechanism string `json:"mechanism" default:"DSN" validate:"inclusion=DSN|Basic|JWT|X509"`
	// Host link to db.
	Host string `json:"host"`
	// DSN connection to SAP HANA database.
	DSN string `json:"dsn"`
	// Username parameter for basic auth.
	Username string `json:"username"`
	// Password parameter for basic auth.
	Password string `json:"password"`
	// Token parameter for JWT auth.
	Token string `json:"token"`
	// ClientCertFilePath path to file, parameter for X509 auth.
	ClientCertFilePath string `json:"clientCertFilePath"`
	// ClientKeyFilePath path to file, parameter for X509 auth.
	ClientKeyFilePath string `json:"clientKeyFilePath"`
}

// Validate auth config parameters.
func (a *AuthConfig) Validate() error {
	switch a.Mechanism {
	case DSNAuthType:
		if a.DSN == "" {
			return errRequiredDSNParameter
		}

		return nil
	case BasicAuthType:
		if a.Host == "" {
			return errRequiredHostParameter
		}
		if a.Username == "" {
			return errRequiredUsernameParameter
		}
		if a.Password == "" {
			return errRequiredPasswordParameter
		}

		return nil
	case JWTAuthType:
		if a.Host == "" {
			return errRequiredHostParameter
		}
		if a.Token == "" {
			return errRequiredTokenParameter
		}

		return nil

	case X509AuthType:
		if a.Host == "" {
			return errRequiredHostParameter
		}
		if a.ClientKeyFilePath == "" {
			return errRequiredClientKeyFileParameter
		}
		if a.ClientCertFilePath == "" {
			return errRequiredClientCertFileParameter
		}

		return nil
	default:
		return ErrInvalidAuthMechanism
	}
}
