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

package destination

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
	"github.com/matryer/is"

	"github.com/conduitio-labs/conduit-connector-sap-hana/config"
	"github.com/conduitio-labs/conduit-connector-sap-hana/destination/mock"
	"github.com/conduitio-labs/conduit-connector-sap-hana/destination/writer"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		expectedErr string
	}{
		{
			name: "success, DSN Auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "DSN",
					config.KeyDSN:           "hdb://name:password@host:443?TLSServerName=name",
				},
			},
			wantErr: false,
		},
		{
			name: "success, Basic Auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "Basic",
					config.KeyHost:          "host",
					config.KeyUsername:      "username",
					config.KeyPassword:      "password",
				},
			},
			wantErr: false,
		},
		{
			name: "success, JWT Auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "JWT",
					config.KeyHost:          "host",
					config.KeyToken:         "token",
				},
			},
			wantErr: false,
		},
		{
			name: "success, X509 Auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:          "CLIENTS",
					config.KeyAuthMechanism:  "X509",
					config.KeyHost:           "host",
					config.KeyClientCertFile: "/tmp/certfile",
					config.KeyClientKeyFile:  "/tmp/keyfile",
				},
			},
			wantErr: false,
		},
		{
			name: "failed, DSN missed for DSN AUTH",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "DSN",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: dsn is required parameter for dsn auth",
		},
		{
			name: "failed, host missed for Basic AUTH",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "Basic",
					config.KeyUsername:      "username",
					config.KeyPassword:      "password",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: host is required parameter for basic, jwt, x509 auth",
		},
		{
			name: "failed, username missed for Basic AUTH",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "Basic",
					config.KeyHost:          "host",
					config.KeyPassword:      "password",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: username is required parameter for basic auth",
		},
		{
			name: "failed, password missed for Basic AUTH",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "Basic",
					config.KeyHost:          "host",
					config.KeyUsername:      "username",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: password is required parameter for basic auth",
		},
		{
			name: "failed, token missed for JWT AUTH",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "JWT",
					config.KeyHost:          "host",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: token is required for jwt auth",
		},
		{
			name: "failed, host missed for X509 auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:          "CLIENTS",
					config.KeyAuthMechanism:  "X509",
					config.KeyClientCertFile: "/tmp/certfile",
					config.KeyClientKeyFile:  "/tmp/keyfile",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: host is required parameter for basic, jwt, x509 auth",
		},
		{
			name: "failed, clientCertFile missed for X509 auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:         "CLIENTS",
					config.KeyAuthMechanism: "X509",
					config.KeyHost:          "host",
					config.KeyClientKeyFile: "/tmp/keyfile",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: client cert file path is required for x509 auth",
		},
		{
			name: "failed, clientKeyFile missed for X509 auth",
			args: args{
				cfg: map[string]string{
					config.KeyTable:          "CLIENTS",
					config.KeyAuthMechanism:  "X509",
					config.KeyHost:           "host",
					config.KeyClientCertFile: "/tmp/certfile",
				},
			},
			wantErr:     true,
			expectedErr: "parse config: validate config: client key file path is required for x509 auth",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := New()

			err := d.Configure(context.Background(), tt.args.cfg)
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

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationCreate,
			Key: sdk.StructuredData{
				"ID": 1,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"ID":   1,
					"name": "test",
				},
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Insert(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		c, err := d.Write(ctx, []sdk.Record{record})
		is.NoErr(err)

		is.Equal(c, 1)
	})

	t.Run("success_update", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"ID": 1,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"ID":   1,
					"name": "test",
				},
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Update(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		c, err := d.Write(ctx, []sdk.Record{record})
		is.NoErr(err)

		is.Equal(c, 1)
	})

	t.Run("success_delete", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationDelete,
			Key: sdk.StructuredData{
				"ID": 1,
			},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"ID":   1,
					"name": "test",
				},
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Delete(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		c, err := d.Write(ctx, []sdk.Record{record})
		is.NoErr(err)

		is.Equal(c, 1)
	})

	t.Run("fail, empty payload", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationSnapshot,
			Position:  sdk.Position("1.0"),
			Key: sdk.StructuredData{
				"ID": 1,
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Insert(ctx, record).Return(writer.ErrNoPayload)

		d := Destination{
			writer: w,
		}

		_, err := d.Write(ctx, []sdk.Record{record})
		is.Equal(err != nil, true)
	})
}

func TestDestination_Teardown(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Close(ctx).Return(nil)

		d := Destination{
			writer: w,
		}

		err := d.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("success, writer is nil", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctx := context.Background()

		d := Destination{
			writer: nil,
		}

		err := d.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("fail, unexpected error", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Close(ctx).Return(errors.New("some error"))

		d := Destination{
			writer: w,
		}

		err := d.Teardown(ctx)
		is.Equal(err != nil, true)
	})
}
