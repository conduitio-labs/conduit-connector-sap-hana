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

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestConfig(t *testing.T) {
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
					"table":          "CLIENTS",
					"auth.mechanism": "DSN",
					"auth.dsn":       "hdb://name:password@host:443?TLSServerName=name",
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
					"table":          "CLIENTS",
					"auth.mechanism": "Basic",
					"auth.host":      "host",
					"auth.username":  "username",
					"auth.password":  "password",
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
					"table":          "CLIENTS",
					"auth.mechanism": "JWT",
					"auth.host":      "host",
					"auth.token":     "token",
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
					"table":                   "CLIENTS",
					"auth.mechanism":          "X509",
					"auth.host":               "host",
					"auth.clientCertFilePath": "/tmp/certfile",
					"auth.clientKeyFilePath":  "/tmp/keyfile",
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
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got Config

			err := sdk.Util.ParseConfig(ctx, tt.args.cfg, &got)
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
