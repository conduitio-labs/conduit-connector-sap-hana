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

package source

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-sap-hana/source/mock"
	"github.com/conduitio-labs/conduit-connector-sap-hana/source/position"
	"github.com/conduitio/conduit-commons/opencdc"
	"go.uber.org/mock/gomock"
)

func TestSource_Configure(t *testing.T) {
	s := Source{}

	tests := []struct {
		name        string
		cfg         map[string]string
		wantErr     bool
		expectedErr string
	}{
		{
			name: "success, DSN Auth",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "DSN",
				"auth.dsn":       "hdb://name:password@host:443?TLSServerName=name",
			},
			wantErr: false,
		},
		{
			name: "success, Basic Auth",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "Basic",
				"auth.host":      "host",
				"auth.username":  "username",
				"auth.password":  "password",
			},
			wantErr: false,
		},
		{
			name: "success, JWT Auth",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "JWT",
				"auth.host":      "host",
				"auth.token":     "token",
			},
			wantErr: false,
		},
		{
			name: "success, X509 Auth",
			cfg: map[string]string{
				"table":                   "CLIENTS",
				"orderingColumn":          "foo",
				"auth.mechanism":          "X509",
				"auth.host":               "host",
				"auth.clientCertFilePath": "/tmp/certfile",
				"auth.clientKeyFilePath":  "/tmp/keyfile",
			},
			wantErr: false,
		},
		{
			name: "failed, DSN missed for DSN AUTH",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "DSN",
			},

			wantErr:     true,
			expectedErr: "validate auth config: dsn is required parameter for dsn auth",
		},
		{
			name: "failed, host missed for Basic AUTH",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "Basic",
				"auth.username":  "username",
				"auth.password":  "password",
			},

			wantErr:     true,
			expectedErr: "validate auth config: host is required parameter for basic, jwt, x509 auth",
		},
		{
			name: "failed, username missed for Basic AUTH",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "Basic",
				"auth.host":      "host",
				"auth.password":  "password",
			},
			wantErr:     true,
			expectedErr: "validate auth config: username is required parameter for basic auth",
		},
		{
			name: "failed, password missed for Basic AUTH",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "Basic",
				"auth.host":      "host",
				"auth.username":  "username",
			},
			wantErr:     true,
			expectedErr: "validate auth config: password is required parameter for basic auth",
		},
		{
			name: "failed, token missed for JWT AUTH",
			cfg: map[string]string{
				"table":          "CLIENTS",
				"orderingColumn": "foo",
				"auth.mechanism": "JWT",
				"auth.host":      "host",
			},
			wantErr:     true,
			expectedErr: "validate auth config: token is required for jwt auth",
		},
		{
			name: "failed, host missed for X509 auth",
			cfg: map[string]string{
				"table":                   "CLIENTS",
				"orderingColumn":          "foo",
				"auth.mechanism":          "X509",
				"auth.clientCertFilePath": "/tmp/certfile",
				"auth.clientKeyFilePath":  "/tmp/keyfile",
			},
			wantErr:     true,
			expectedErr: "validate auth config: host is required parameter for basic, jwt, x509 auth",
		},
		{
			name: "failed, clientCertFile missed for X509 auth",
			cfg: map[string]string{
				"table":                  "CLIENTS",
				"orderingColumn":         "foo",
				"auth.mechanism":         "X509",
				"auth.host":              "host",
				"auth.clientKeyFilePath": "/tmp/keyfile",
			},
			wantErr:     true,
			expectedErr: "validate auth config: client cert file path is required for x509 auth",
		},
		{
			name: "failed, clientKeyFile missed for X509 auth",
			cfg: map[string]string{
				"table":                   "CLIENTS",
				"orderingColumn":          "foo",
				"auth.mechanism":          "X509",
				"auth.host":               "host",
				"auth.clientCertFilePath": "/tmp/certfile",
			},
			wantErr:     true,
			expectedErr: "validate auth config: client key file path is required for x509 auth",
		},
	}
	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {
			err := s.Configure(context.Background(), tests[i].cfg)
			if err != nil {
				if !tests[i].wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tests[i].wantErr)

					return
				}

				if err.Error() != tests[i].expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tests[i].expectedErr, err.Error())

					return
				}

				return
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(opencdc.StructuredData)
		st["key"] = "value"

		pos, _ := json.Marshal(position.Position{
			IteratorType:             position.TypeSnapshot,
			SnapshotLastProcessedVal: "1",
			CDCLastID:                0,
		})

		record := opencdc.Record{
			Position: pos,
			Metadata: nil,
			Key:      st,
			Payload:  opencdc.Change{After: st},
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Source{
			iterator: it,
		}

		r, err := s.Read(ctx)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(r, record) {
			t.Errorf("got = %v, want %v", r, record)
		}
	})

	t.Run("failed_has_next", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("run query: failed"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(opencdc.Record{}, errors.New("key does not exist"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestSource_Teardown(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(nil)

		s := Source{
			iterator: it,
		}
		err := s.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(errors.New("some error"))

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}
