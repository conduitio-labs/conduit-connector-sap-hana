# conduit-connector-sap-hana

## General

The [SAP HANA](https://www.sap.com/products/technology-platform/hana/what-is-sap-hana.html) connector is one of Conduit plugins.
It provides SAP HANA source and destination connectors.

### Prerequisites

- [Go](https://go.dev/) 1.21
- [Conduit Paramgen](https://github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen)
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests.

## Source

The SAP HANA source connects to the database using the provided connection and starts creating records for each table row
and each detected change. It supports getting a snapshot from the table and detects CDC (Change Data Captured) changes.

### Configuration options

| Name                      | Description                                                                                                                                                                                           | Required                                   | Example                                           | By default |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|---------------------------------------------------|------------|
| `table`                   | The name of a table in the database that the connector should read from.                                                                                                                              | **true**                                   | users                                             |            |
| `orderingColumn`          | The name of a column that the connector will use for ordering rows. Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.                                 | **true**                                   | id                                                |            |
| `primaryKeys`             | Comma separated list of column names that records could use for their `Key` fields. By default connector uses primary keys from table, if these don't exist, the connector will use `orderingColumn`. | false                                      | id                                                |            |
| `snapshot`                | Whether or not to take a snapshot of the entire table before starting cdc mode, default value is `true`.                                                                                              | false                                      | false                                             |            |
| `batchSize`               | Size of rows batch.                                                                                                                                                                                   | false                                      | 100                                               | 1000       |
| `auth.mechanism`          | Mechanism type of auth. Valid types: DSN, Basic, JWT, X509.                                                                                                                                           | false                                      | JWT                                               | DSN        |
| `auth.dsn`                | DSN connection string                                                                                                                                                                                 | Required for DSN auth type.                | hdb://user:password@host443?TLSServerName=name    |            |
| `auth.host`               | Sap Hana database host.                                                                                                                                                                               | Required for Basic, JWT, X509 auth types.  | hdb://hanacloud.ondemand.com:443                  |            |
| `auth.username`           | Sap Hana user                                                                                                                                                                                         | Required for Basic type.                   | hbadmin                                           |            |
| `auth.password`           | Sap Hana password                                                                                                                                                                                     | Required for Basic type.                   | pass                                              |            |
| `auth.token`              | JWT token                                                                                                                                                                                             | Required for JWT type.                     | jwt_token                                         |            |
| `auth.clientCertFilePath` | Path for certification file                                                                                                                                                                           | Required for X509 type.                    | /tmp/file.cert                                    |            |
| `auth.clientKeyFilePath`  | Path for key file                                                                                                                                                                                     | Required for X509 type.                    | /tmp/key.cert                                     |            |

### Snapshot
By default, when the connector starts for the first time, snapshot mode is enabled, which means that existing data will
be read. To skip reading existing data, change config parameter `snapshot` to `false`.
All rows which exist in a table at the time the snapshot started, are considered part of the snapshot.
When all snapshot records are returned, the connector switches to listening for CDC changes.

### Change Data Capture (CDC)

This connector implements CDC features for DB2 by adding a tracking table and triggers to populate it. The tracking
table has the same name as a target table with the prefix `CONDUIT_`, and the suffix of the time when the pipeline started,
with the format "hhmmss". For example for table `PRODUCTS` the tracking table's name will look like `CONDUIT_PRODUCTS_213315`.
The tracking table has the same columns as the target table plus two additional columns:

| name                            | description                                      |
|---------------------------------|--------------------------------------------------|
| `CONDUIT_TRACKING_ID`           | Autoincrement index for the position.            |
| `CONDUIT_OPERATION_TYPE`        | Operation type: `INSERT`, `UPDATE`, or `DELETE`. |

The connector saves  information about update, delete, insert `table` operations inside tracking table.
For example if user inserts a new row into `table`, the connector will save all new columns values inside the tracking table  
with `CONDUIT_OPERATION_TYPE` = `INSERT`

Triggers have a name pattern of `CD_{{TABLENAME}}_{{OPERATION_TYPE}}_{{SUFFIXNAME}}`. For example:
`CD_PRODUCTS_INSERT_213315`


Queries to retrieve CDC from a tracking table are very similar to queries in a Snapshot iterator, but with
`CONDUIT_TRACKING_ID` ordering column.

The connector cleans up the tracking table every 5 seconds.

Iterator saves the last `CONDUIT_TRACKING_ID` to the position from the last successfully recorded row.

If connector stops, it will parse position from the last record and will try
to get row where `{{CONDUIT_TRACKING_ID}}` > `{{position.CDCLastID}}`.



<b>Please pay attention</b>

The tracking table and the triggers are not automatically removed when a pipeline is deleted.
That needs to be done manually, for example by using the following commands:
```sql
  DROP TABLE CONDUIT_{{YOUR_TABLE_NAME}}_{{SUFFIXNAME}};
  DROP TRIGGER CD_{{TABLENAME}}_INSERT_{{SUFFIXNAME}};
  DROP TRIGGER CD_{{TABLENAME}}_UPDATE_{{SUFFIXNAME}};
  DROP TRIGGER CD_{{TABLENAME}}_DELETE_{{SUFFIXNAME}};
```

### CDC FAQ

#### Is it possible to add/remove/rename column to table?

Yes. You have to stop the pipeline and do the same changes with the conduit tracking table.
For example:
```sql
ALTER TABLE CLIENTS
ADD COLUMN phone VARCHAR(18);

ALTER TABLE CONDUIT_CLIENTS_{suffix}
    ADD COLUMN phone VARCHAR(18);
```

#### I accidentally removed tracking table.
You have to restart pipeline, tracking table will be recreated by the connector.

## Destination

The Sap Hana Destination takes a `sdk.Record` and parses it into a valid SQL query.

### Configuration Options

| Name                        | Description                                                                                                                                                                                     | Required                                  | Example                                        |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------|------------------------------------------------|
| `table`                     | The name of a table in the database that the connector should  write to, by default.                                                                                                            | **true**                                  | users                                          |
| `auth.mechanism`            | Mechanism type of auth. Valid types: DSN, Basic, JWT, X509. By default is DSN.                                                                                                                  | false                                     | DSN                                            |
| `auth.dsn`                  | DSN connection string                                                                                                                                                                           | Required for DSN auth type.               | hdb://user:password@host443?TLSServerName=name |
| `auth.host`                 | Sap Hana database host.                                                                                                                                                                         | Required for Basic, JWT, X509 auth types. | hdb://hanacloud.ondemand.com:443               |
| `auth.username`             | Sap Hana user                                                                                                                                                                                   | Required for Basic type.                  | hbadmin                                        |
| `auth.password`             | Sap Hana password                                                                                                                                                                               | Required for Basic type.                  | pass                                           |
| `auth.token`                | JWT token                                                                                                                                                                                       | Required for JWT type.                    | jwt_token                                      |
| `auth.clientCertFilePath`   | Path for certification file                                                                                                                                                                     | Required for X509 type.                   | /tmp/file.cert                                 |
| `auth.ClientKeyFilePath`    | Path for key file                                                                                                                                                                               | Required for X509 type.                   | /tmp/key.cert                                  |

### Table name

If a record contains a `saphana.table` property in its metadata it will be inserted in that table, otherwise it will fall back
to use the table configured in the connector. Thus, a destination can support multiple tables in a single connector,
as long as the user has proper access to those tables.
