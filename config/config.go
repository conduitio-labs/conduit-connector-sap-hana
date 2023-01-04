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
	"fmt"
)

const (
	// KeyTable is the config name for SAP HANA database table.
	KeyTable string = "table"
	// KeyAuthMechanism is the config name for SAP HANA database auth type.
	KeyAuthMechanism string = "auth.mechanism"
	// KeyDSN is the config name for dsn auth parameter.
	KeyDSN string = "auth.DSN"
	// KeyHost is the config name for host auth parameter.
	KeyHost string = "auth.host"
	// KeyUsername is the config name for username auth parameter.
	KeyUsername string = "auth.username"
	// KeyPassword is the config name for password auth parameter.
	KeyPassword string = "auth.password"
	// KeyToken is the config name for token auth parameter.
	KeyToken string = "auth.token"
	// KeyClientCertFile is the config name for clientCertFile auth parameter.
	KeyClientCertFile string = "auth.clientCertFile"
	// KeyClientKeyFile is the config name for clientKeyFile auth parameter.
	KeyClientKeyFile string = "auth.clientKeyFile"
)

// authType type of auth.
type authType string

const (
	// DSNAuthType name of DSN auth.
	DSNAuthType authType = "DSN"
	// BasicAuthType name of Basic auth.
	BasicAuthType authType = "Basic"
	// JWTAuthType name of JWT auth.
	JWTAuthType authType = "JWT"
	// X509AuthType name of X509 auth.
	X509AuthType authType = "X509"
)

// Config contains configurable values
// shared between source and destination SAP HANA connector.
type Config struct {
	// Table is a name of the table that the connector should write to or read from.
	Table string

	Auth AuthConfig
}

// AuthConfig auth parameters.
type AuthConfig struct {
	// Mechanism type of auth. Valid types: DSN, Basic, JWT, X509.
	Mechanism authType
	// Host link to db.
	Host string
	// DSN connection to SAP HANA database.
	DSN string
	// Username parameter for basic auth.
	Username string
	// Password parameter for basic auth.
	Password string
	// Token parameter for JWT auth.
	Token string
	// ClientCertFilePath path to file, parameter for X509 auth.
	ClientCertFilePath string
	// clientKeyFile path to file, parameter for X509 auth.
	ClientKeyFilePath string
}

// Parse attempts to parse a provided map[string]string into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Table: cfg[KeyTable],
		Auth: AuthConfig{
			Mechanism:          DSNAuthType,
			Host:               cfg[KeyHost],
			DSN:                cfg[KeyDSN],
			Username:           cfg[KeyUsername],
			Password:           cfg[KeyPassword],
			Token:              cfg[KeyToken],
			ClientCertFilePath: cfg[KeyClientCertFile],
			ClientKeyFilePath:  cfg[KeyClientKeyFile],
		},
	}

	if cfg[KeyAuthMechanism] != "" {
		config.Auth.Mechanism = authType(cfg[KeyAuthMechanism])
	}

	err := config.Auth.validate()
	if err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}

func (a *AuthConfig) validate() error {
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
