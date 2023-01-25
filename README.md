# conduit-connector-sap-hana

## General

The [SAP HANA](https://www.sap.com/products/technology-platform/hana/what-is-sap-hana.html) connector is one of Conduit plugins. It provides both, a source and a
destination Sap Hana connector.

### Prerequisites

- [Go](https://go.dev/) 1.19
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.50.1

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests.

## Source

The DB source connects to the database using the provided connection and starts creating records for each table row
and each detected change.

### Configuration options

| Name                        | Description                                                                                                                                                                                     | Required                                  | Example                                        |
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------|------------------------------------------------|
| `table`                     | The name of a table in the database that the connector should  write to, by default.                                                                                                            | **true**                                  | users                                          |
| `orderingColumn`            | The name of a column that the connector will use for ordering rows. Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.                           | **true**                                  | id                                             |
| `primaryKeys`               | Comma separated list of column names that records could use for their `Key` fields. By default connector uses primary keys from table if they are not exist connector will use ordering column. | false                                     | id                                             |
| `snapshot`                  | Whether or not the plugin will take a snapshot of the entire table before starting cdc mode, by default true.                                                                                   | false                                     | false                                          |
| `batchSize`                 | Size of rows batch. By default is 1000.                                                                                                                                                         | false                                     | 100                                            |
| `auth.mechanism`            | Mechanism type of auth. Valid types: DSN, Basic, JWT, X509. By default is DSN.                                                                                                                  | false                                     | DSN                                            |
| `auth.dsn`                  | DSN connection string                                                                                                                                                                           | Required for DSN auth type.               | hdb://user:password@host443?TLSServerName=name |
| `auth.host`                 | Sap Hana database host.                                                                                                                                                                         | Required for Basic, JWT, X509 auth types. | hdb://hanacloud.ondemand.com:443               |
| `auth.username`             | Sap Hana user                                                                                                                                                                                   | Required for Basic type.                  | hbadmin                                        |
| `auth.password`             | Sap Hana password                                                                                                                                                                               | Required for Basic type.                  | pass                                           |
| `auth.token`                | JWT token                                                                                                                                                                                       | Required for JWT type.                    | jwt_token                                      |
| `auth.clientCertFilePath`   | Path for certification file                                                                                                                                                                     | Required for X509 type.                   | /tmp/file.cert                                 |
| `auth.ClientKeyFilePath`    | Path for key file                                                                                                                                                                               | Required for X509 type.                   | /tmp/key.cert                                  |

### Snapshot
By default when the connector starts for the first time, snapshot mode is enabled, which means that existing data will
be read. To skip reading existing, change config parameter `snapshot` to `false`.

First time when the snapshot iterator starts work,
it is get max value from `orderingColumn` and saves this value to position.
The snapshot iterator reads all rows, where `orderingColumn` values less or equal maxValue, from the table in batches.


Values in the ordering column must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.
Iterators saves last processed value from `orderingColumn` column to position to field `SnapshotLastProcessedVal`.
If snapshot was interrupted on next start connector will parse last recorded position
to find next snapshot rows.


When all records are returned, the connector switches to the CDC iterator.

### Change Data Capture (CDC)

This connector implements CDC features for DB2 by adding a tracking table and triggers to populate it. The tracking
table has the same name as a target table with the prefix `CONDUIT_` and suffix from time when pipeline started on
format "hhmmss". For example for table `PRODUCTS` tracking will be looks like `CONDUIT_PRODUCTS_213315`. 
The tracking table has all the same columns as the target table plus two additional columns:

| name                            | description                                      |
|---------------------------------|--------------------------------------------------|
| `CONDUIT_TRACKING_ID`           | Autoincrement index for the position.            |
| `CONDUIT_OPERATION_TYPE`        | Operation type: `INSERT`, `UPDATE`, or `DELETE`. |

The connector saves  information about update, delete, insert `table` operations inside tracking table.
For example if user inserts new row into `table` connector will save all new columns values inside tracking table  
with `CONDUIT_OPERATION_TYPE` = `INSERT`

Triggers have name pattern `CD_{{TABLENAME}}_{{OPERATION_TYPE}}_{{SUFFIXNAME}}`. For example:
`CD_PRODUCTS_INSERT_213315`


Queries to retrieve change data from a tracking table are very similar to queries in a Snapshot iterator, but with
`CONDUIT_TRACKING_ID` ordering column.

CDC iterator periodically clears rows which were successfully applied from tracking table.
It collects `CONDUIT_TRACKING_ID` inside the `Ack` method into a batch and clears the tracking table every 5 seconds.

Iterator saves the last `CONDUIT_TRACKING_ID` to the position from the last successfully recorded row.

If connector stops, it will parse position from the last record and will try
to get row where `{{CONDUIT_TRACKING_ID}}` > `{{position.CDCLastID}}`.



<b>Please pay attention</b>

Tracking table and triggers are stayed after removing pipeline. You can clear it uses commands.
```sql
  DROP TABLE CONDUIT_{{YOUR_TABLE_NAME}}_{{SUFFIXNAME}};
  DROP TRIGGER CD_{{TABLENAME}}_INSERT_{{SUFFIXNAME}};
  DROP TRIGGER CD_{{TABLENAME}}_UPDATE_{{SUFFIXNAME}};
  DROP TRIGGER CD_{{TABLENAME}}_DELETE_{{SUFFIXNAME}};
```

### CDC FAQ

#### Is it possible to add/remove/rename column to table?

Yes. You have to stop the pipeline and do the same with conduit tracking table.
For example:
```sql
ALTER TABLE CLIENTS
ADD COLUMN phone VARCHAR(18);

ALTER TABLE CONDUIT_CLIENTS_{suffix}
    ADD COLUMN phone VARCHAR(18);
```

#### I accidentally removed tracking table.

You have to restart pipeline, tracking table will be recreated by connector.
