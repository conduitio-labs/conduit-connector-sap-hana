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
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}
	tests := []struct {
		name        string
		args        args
		want        Config
		wantErr     bool
		expectedErr string
	}{
		{
			name: "success, DSN Auth",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "DSN",
					KeyDSN:           "hdb://name:password@host:443?TLSServerName=name",
				},
			},
			want: Config{
				Table: "CLIENTS",
				Auth: AuthConfig{
					DSN:       "hdb://name:password@host:443?TLSServerName=name",
					Mechanism: DSNAuthType,
				},
			},
			wantErr: false,
		},
		{
			name: "success, Basic Auth",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "Basic",
					KeyHost:          "host",
					KeyUsername:      "username",
					KeyPassword:      "password",
				},
			},
			want: Config{
				Table: "CLIENTS",
				Auth: AuthConfig{
					Mechanism: BasicAuthType,
					Host:      "host",
					Username:  "username",
					Password:  "password",
				},
			},
			wantErr: false,
		},
		{
			name: "success, JWT Auth",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "JWT",
					KeyHost:          "host",
					KeyToken:         "token",
				},
			},
			want: Config{
				Table: "CLIENTS",
				Auth: AuthConfig{
					Mechanism: JWTAuthType,
					Host:      "host",
					Token:     "token",
				},
			},
			wantErr: false,
		},
		{
			name: "success, X509 Auth",
			args: args{
				cfg: map[string]string{
					KeyTable:          "CLIENTS",
					KeyAuthMechanism:  "X509",
					KeyHost:           "host",
					KeyClientCertFile: "/tmp/certfile",
					KeyClientKeyFile:  "/tmp/keyfile",
				},
			},
			want: Config{
				Table: "CLIENTS",
				Auth: AuthConfig{
					Mechanism:          X509AuthType,
					Host:               "host",
					ClientCertFilePath: "/tmp/certfile",
					ClientKeyFilePath:  "/tmp/keyfile",
				},
			},
			wantErr: false,
		},
		{
			name: "failed, DSN missed for DSN AUTH",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "DSN",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: dsn is required parameter for dsn auth",
		},
		{
			name: "failed, host missed for Basic AUTH",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "Basic",
					KeyUsername:      "username",
					KeyPassword:      "password",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: host is required parameter for basic, jwt, x509 auth",
		},
		{
			name: "failed, username missed for Basic AUTH",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "Basic",
					KeyHost:          "host",
					KeyPassword:      "password",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: username is required parameter for basic auth",
		},
		{
			name: "failed, password missed for Basic AUTH",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "Basic",
					KeyHost:          "host",
					KeyUsername:      "username",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: password is required parameter for basic auth",
		},
		{
			name: "failed, token missed for JWT AUTH",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "JWT",
					KeyHost:          "host",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: token is required for jwt auth",
		},
		{
			name: "failed, host missed for X509 auth",
			args: args{
				cfg: map[string]string{
					KeyTable:          "CLIENTS",
					KeyAuthMechanism:  "X509",
					KeyClientCertFile: "/tmp/certfile",
					KeyClientKeyFile:  "/tmp/keyfile",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: host is required parameter for basic, jwt, x509 auth",
		},
		{
			name: "failed, clientCertFile missed for X509 auth",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "X509",
					KeyHost:          "host",
					KeyClientKeyFile: "/tmp/keyfile",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: client cert file path is required for x509 auth",
		},
		{
			name: "failed, clientKeyFile missed for X509 auth",
			args: args{
				cfg: map[string]string{
					KeyTable:          "CLIENTS",
					KeyAuthMechanism:  "X509",
					KeyHost:           "host",
					KeyClientCertFile: "/tmp/certfile",
				},
			},
			wantErr:     true,
			expectedErr: "validate config: client key file path is required for x509 auth",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
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

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
