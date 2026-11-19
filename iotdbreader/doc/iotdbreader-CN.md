# DataX IoTDB Reader

`iotdbreader` reads Apache IoTDB 2.0.11 Tree or Table model query results through the Java Session client. Results are streamed row by row into DataX records and each query result set is closed before the next query runs.

## Configuration

Tree model example:

```json
{
  "name": "iotdbreader",
  "parameter": {
    "model": "tree",
    "host": "<iotdb-host>",
    "port": 6667,
    "username": "<username>",
    "password": "<password>",
    "version": "V_1_0",
    "fetchSize": 1000,
    "queryTimeoutMs": 60000,
    "device": "root.factory.device01",
    "measurements": ["temperature", "humidity"],
    "where": "time >= 0",
    "timeColumnPosition": 0
  }
}
```

For Table model, set `model` to `table`, optionally set `database`, and supply a read-only `querySql` or `querySqls` statement. `querySqls` is split across reader tasks; multiple statements must be independent and non-overlapping if they are expected to produce disjoint output.

| Parameter | Required | Description |
| --- | --- | --- |
| `model` | No | `tree` (default) or `table`. |
| `host`, `port`, `username`, `password` | Yes | IoTDB RPC endpoint and credentials. Supply secrets through the job configuration provider. |
| `version` | Tree only | Java Session protocol version enum; defaults to `V_1_0`. |
| `database` | Table only | Optional database selected when opening the Table session. |
| `fetchSize` | No | Positive client fetch size; defaults to `1000`. |
| `queryTimeoutMs` | No | Positive query timeout in milliseconds; defaults to `60000`. |
| `querySql` | One of query or Tree fields | One custom query. |
| `querySqls` | One of query or Tree fields | List of custom queries split between tasks. |
| `device`, `measurements` | Tree, when no custom query | Build `SELECT measurements FROM device [WHERE ...]`. |
| `where` | No | Predicate appended to the generated Tree query. |
| `timeColumnPosition` | No | Position of the DataX timestamp column in the output record; defaults to `0`. |

The timestamp is emitted as a DataX `DateColumn`. Supported result types are BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, STRING, TEXT, DATE, and BLOB. Null values retain their declared IoTDB type in DataX. Unsupported or untyped null fields are routed to the DataX dirty-record collector.

## Limitations

- Queries should project a time axis suitable for the downstream DataX record. Aggregations or expressions that do not yield a timestamp use the row timestamp returned by the IoTDB client.
- This plugin does not parallelize a single query; parallel reads require multiple independent statements in `querySqls`.
- Validate query syntax and model-specific features against the target IoTDB server version.

## Remote smoke test

Set `IOTDB_TEST_ENDPOINT=host:port` and optionally `IOTDB_TEST_USERNAME` / `IOTDB_TEST_PASSWORD`, then run `mvn -pl iotdbreader -am test`. The gated test runs the DataX Writer and Reader tasks against a unique Tree database, checks the timestamp and value round trip, and drops that database afterward.
