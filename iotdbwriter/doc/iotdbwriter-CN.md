# DataX IoTDB Writer

`iotdbwriter` writes DataX records to Apache IoTDB 2.0.11 using the Java Session client. It supports Tree and Table models, validates an explicit destination schema, and sends bounded Tablet batches.

## Tree model example

The input record order below is `time, temperature, status`.

```json
{
  "name": "iotdbwriter",
  "parameter": {
    "model": "tree",
    "host": "<iotdb-host>",
    "port": 6667,
    "username": "<username>",
    "password": "<password>",
    "version": "V_1_0",
    "queryTimeoutMs": 60000,
    "device": "root.factory.device01",
    "measurements": ["temperature", "status"],
    "dataTypes": ["DOUBLE", "BOOLEAN"],
    "timeColumnPosition": 0,
    "batchSize": 1000,
    "preSql": []
  }
}
```

## Table model example

The input record order below is `time, region, device_id, temperature, note`. TAG columns precede ATTRIBUTE and FIELD columns as required by the Tablet format.

```json
{
  "name": "iotdbwriter",
  "parameter": {
    "model": "table",
    "host": "<iotdb-host>",
    "port": 6667,
    "username": "<username>",
    "password": "<password>",
    "database": "factory",
    "table": "readings",
    "columnNames": ["region", "device_id", "temperature", "note"],
    "dataTypes": ["STRING", "STRING", "DOUBLE", "TEXT"],
    "columnCategories": ["TAG", "TAG", "FIELD", "FIELD"],
    "timeColumnPosition": 0,
    "batchSize": 1000
  }
}
```

`columnCategories` accepts `TAG`, `ATTRIBUTE`, and `FIELD`; omit it to mark every Table column as `FIELD`. Table writes require a pre-created destination table with matching names, categories, and types. Tree measurements are created automatically by IoTDB when permitted by server settings.

## Parameters

| Parameter | Required | Description |
| --- | --- | --- |
| `model` | No | `tree` (default) or `table`. |
| `host`, `port`, `username`, `password` | Yes | IoTDB RPC endpoint and credentials. Supply secrets through the job configuration provider. |
| `version` | Tree only | Java Session protocol version enum; defaults to `V_1_0`. |
| `queryTimeoutMs` | No | Positive timeout in milliseconds; defaults to `60000`. |
| `database` | No | Optional Table model database selected by the session. |
| `device`, `measurements` | Tree only | Destination device path and measurement names. |
| `table`, `columnNames` | Table only | Destination table and non-time columns in schema order. |
| `dataTypes` | Yes | IoTDB type for each destination measurement/column. |
| `columnCategories` | Table only | Category for each column; TAG columns must come first. |
| `timeColumnPosition` | No | Timestamp column position in the DataX record; defaults to `0`. |
| `batchSize` | No | Rows per Tablet, from 1 through 100000; defaults to `1000`. |
| `preSql` | No | Statements executed before writing. Use carefully because DDL/DML may alter existing data. |

Supported destination types are BOOLEAN, INT32, INT64, TIMESTAMP, FLOAT, DOUBLE, STRING, TEXT, DATE, and BLOB. Null cells are encoded as nulls. Invalid row widths, null timestamps, and failed type conversions are sent to the DataX dirty-record collector. Server-side write failures fail the task.

## Limitations

- Tree mode writes one configured device per task. Table mode writes one configured table per task.
- Table mode requires an existing table whose schema matches the configured column names, order, types, and categories.
- `preSql` executes once for each writer task, so statements should be safe under concurrent DataX tasks.
- Client retry behavior follows IoTDB Session defaults. A connection failure around a batch may leave the caller unable to determine whether the server accepted it; design reruns around the destination's timestamp overwrite semantics.
