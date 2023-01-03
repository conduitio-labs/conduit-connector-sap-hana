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
		name    string
		args    args
		want    Config
		wantErr bool
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
			wantErr: true,
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
			wantErr: true,
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
			wantErr: true,
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
			wantErr: true,
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
			wantErr: true,
		},
		{
			name: "failed, host missed for JWT AUTH",
			args: args{
				cfg: map[string]string{
					KeyTable:         "CLIENTS",
					KeyAuthMechanism: "JWT",
					KeyToken:         "token",
				},
			},
			wantErr: true,
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
			wantErr: true,
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
			wantErr: true,
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
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
