// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package source

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigAuthClientCertFilePath = "auth.clientCertFilePath"
	ConfigAuthClientKeyFilePath  = "auth.clientKeyFilePath"
	ConfigAuthDsn                = "auth.dsn"
	ConfigAuthHost               = "auth.host"
	ConfigAuthMechanism          = "auth.mechanism"
	ConfigAuthPassword           = "auth.password"
	ConfigAuthToken              = "auth.token"
	ConfigAuthUsername           = "auth.username"
	ConfigBatchSize              = "batchSize"
	ConfigOrderingColumn         = "orderingColumn"
	ConfigPrimaryKeys            = "primaryKeys"
	ConfigSnapshot               = "snapshot"
	ConfigTable                  = "table"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigAuthClientCertFilePath: {
			Default:     "",
			Description: "ClientCertFilePath path to file, parameter for X509 auth.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigAuthClientKeyFilePath: {
			Default:     "",
			Description: "ClientKeyFilePath path to file, parameter for X509 auth.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigAuthDsn: {
			Default:     "",
			Description: "DSN connection to SAP HANA database.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigAuthHost: {
			Default:     "",
			Description: "Host link to db.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigAuthMechanism: {
			Default:     "DSN",
			Description: "Mechanism type of auth. Valid types: DSN, Basic, JWT, X509.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationInclusion{List: []string{"DSN", "Basic", "JWT", "X509"}},
			},
		},
		ConfigAuthPassword: {
			Default:     "",
			Description: "Password parameter for basic auth.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigAuthToken: {
			Default:     "",
			Description: "Token parameter for JWT auth.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigAuthUsername: {
			Default:     "",
			Description: "Username parameter for basic auth.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigBatchSize: {
			Default:     "1000",
			Description: "BatchSize is a size of rows batch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: 0},
				config.ValidationLessThan{V: 10001},
			},
		},
		ConfigOrderingColumn: {
			Default:     "",
			Description: "OrderingColumn is a name of a column that the connector will use for ordering rows.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigPrimaryKeys: {
			Default:     "",
			Description: "PrimaryKeys list of column names should use for their `Key` fields.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigSnapshot: {
			Default:     "true",
			Description: "Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		ConfigTable: {
			Default:     "",
			Description: "Table is a name of the table that the connector should write to or read from.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
