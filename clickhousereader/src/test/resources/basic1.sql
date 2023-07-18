CREATE TABLE IF NOT EXISTS default.all_type_tbl
(
`uint8_col` UInt8,
`uint16_col` UInt16,
uint32_col UInt32,
uint64_col UInt64,
int8_col Int8,
int16_col Int16,
int32_col Int32,
int64_col Int64,
float32_col Float32,
float64_col Float64,
bool_col UInt8,
str_col String,
fixedstr_col FixedString(3),
uuid_col UUID,
date_col Date,
datetime_col DateTime,
enum_col Enum('hello' = 1, 'world' = 2),
ary_uint8_col Array(UInt8),
ary_str_col Array(String),
tuple_col Tuple(UInt8, String),
nullable_col Nullable(UInt8),
nested_col Nested
    (
        nested_id UInt32,
        nested_str String
    ),
ipv4_col IPv4,
ipv6_col IPv6,
decimal_col Decimal(5,3)
)
ENGINE = MergeTree()
ORDER BY (uint8_col);