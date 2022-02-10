
# DataX MimerReader


___



## 1 Brief Intro

The MimerReader plubin helps to read data from the source tables in a Mimer SQL database. It is implemented using JDBC to connect to a Mimer SQL database, and execute correspoinding SELECT statements.

## 2 Implementation

MimerReader uses JDBC to connect to a Mimer SQL database, and creates SELECT statements using user-defined configuration. Then nit sends the statements to Mimer SQL and executes them. The returned results are converted to DataX internal sets and are passed to Writers by DataX.

MimerReader composes SQL queries using `table`, `column` and `where` from the job configuration. If a `querySql` is configured, MimerReader sends this query directly.


## 3 Specification

### 3.1 Job configuration example

* This example configures a job that extracts data from a Mimer SQL database to local streamwriter (standard output):

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel": 3
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mimerreader",
                    "parameter": {
                        "username": "SYSADM",
                        "password": "SYSADM",
                        "column": [
                            "id",
                            "name"
                        ],
                        "splitPk": "db_id",
                        "connection": [
                            {
                                "table": [
                                    "table"
                                ],
                                "jdbcUrl": [
                                    "jdbc:mimer://localhost:1360/datax"
                                ]
                            }
                        ]
                    }
                },
               "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print":true
                    }
                }
            }
        ]
    }
}

```

* A job with pre-defined SQL query:

```
{
    "job": {
        "setting": {
            "speed": {
                 "channel":1
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mimerreader",
                    "parameter": {
                        "username": "SYSADM",
                        "password": "SYSADM",
                        "connection": [
                            {
                                "querySql": [
                                    "select db_id,on_line_flag from db_info where db_id < 10;"
                                ],
                                "jdbcUrl": [
                                    "jdbc:mimer://bad_ip:1360/db1",
                                    "jdbc:mimer://localhost:bad_port/datax",
                                    "jdbc:mimer://127.0.0.1:1360/db2"
                                ]
                            }
                        ]
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": false,
                        "encoding": "UTF-8"
                    }
                }
            }
        ]
    }
}
```


### 3.2 Parameters of `reader`

* **jdbcUrl**

	* JDBC connection string of the source database. There can be several URLs, and MimerReader will try the connectivity of each one in turn, until a connectable server is found. If none of them can be connected, MimerReader returns an error. 

	  JDBC URL must follow Mimer SQL specifications, and arguments can be attached in the string. More details are referred to Mimer JDBC documentation (https://developer.mimer.com/article/jdbc/).

    * Mandatory: yes <br />

    * Default: no default value <br />
* **username**

    * User name of the database <br />

    * Mandatory: yes <br />

    * Default: no default value <br />

* **password**

    * Password of the user <br />

    * Mandatory: yes <br />

    * Default: no default value <br />

* **table**

    * Destination table. This can be several tables, but they must have the same definition. MimerReader does not check this.

    * Mandatory: yes <br />

    * Default: no default value <br />

* **column**

    * Columns to be inserted in the destination table. Use `,` to separate them. For example,  `"column": ["id","name","age"]`. You can use `*` to represent all tables: `"column": ["*"]`.

    Note: columns can be constants or literals.

    * Mandatory: yes <br />

    * Default: no default value <br />

* **splitPk**

	* If splitPk is specified with a column name, the column is used to split the table into slices. DataX will parallelize the job and improve performance.

	  It is recommended to use primary key as splikPk, because they are usually evenly distributed and can prevent hot spots in the slices.

      Currently splitPk only supports integral data types.

      If splitPk is unspecified or specified with empty, then DataX will execute the job with single channel.

    * Mandatory: no <br />

    * Default: no default value <br />

* **where**

	* The where condition for the query. MimerReader composes an SQL query using the specified table, column and where.

    * Mandatory: no <br />

    * Default: no default value <br />

* **querySql**

	* Users can specify querySql with a complete SQL query. When this is specified, table, column and where specifications are ignored.

    * Mandatory: no <br />

    * Default: no default value <br />


Note: channels are set in the settings for the job through `speed`:

* **speed**

     * Controls the speed of the job. This can be configured by: 
        - channels: the number of allowed concurrent channels.
        - byte: the number of allowed bytes per second.
        - record: the number of allowed records per second.

    * Mandatory: yes <br />

    * Options: channel/byte/record <br />

### 3.3 Data tpes

MimerReader supports most data types in Mimer SQL, but may not be complete.

Data types converted between DataX internal types and Mimer SQL types:

| DataX internal | Mimer SQL data type    |
| -------- | -----  |
| Long     |bigint, integer, smallint |
| Double   |double precision, numeric, real, float, decimal |
| String   |varchar, char, clob|
| Date     |date, time, timestamp |
| Boolean  |bool|
| Bytes    |binary, varbinary, blob|