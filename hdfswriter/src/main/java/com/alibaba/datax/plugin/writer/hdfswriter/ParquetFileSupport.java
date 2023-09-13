package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.LimitLogger;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author jitongchen
 * @date 2023/9/7 9:41 AM
 */
public class ParquetFileSupport extends WriteSupport<Record> {
    public static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileSupport.class);
    private MessageType schema;
    private List<ColumnDescriptor> columns;
    private RecordConsumer recordConsumer;
    private boolean useRawDataTransf = true;
    private boolean printStackTrace = true;

    // 不通类型的nullFormat
    private String nullFormat;

    private String dateFormat;
    private boolean isUtcTimestamp;
    private SimpleDateFormat dateParse;
    private Binary binaryForNull;
    private TaskPluginCollector taskPluginCollector;
    private String dataxParquetMode;

    public ParquetFileSupport(MessageType schema, com.alibaba.datax.common.util.Configuration taskConfig, TaskPluginCollector taskPluginCollector) {
        this.schema = schema;
        this.columns = schema.getColumns();
        this.useRawDataTransf = taskConfig.getBool(Key.PARQUET_FILE_USE_RAW_DATA_TRANSF, true);

        // 不通类型的nullFormat
        this.nullFormat = taskConfig.getString(Key.NULL_FORMAT, Constant.DEFAULT_NULL_FORMAT);
        this.binaryForNull = Binary.fromString(this.nullFormat);

        this.dateFormat = taskConfig.getString(Key.DATE_FORMAT, null);
        if (StringUtils.isNotBlank(this.dateFormat)) {
            this.dateParse = new SimpleDateFormat(dateFormat);
        }

        this.isUtcTimestamp = taskConfig.getBool(Key.PARQUET_UTC_TIMESTAMP, false);

        this.taskPluginCollector = taskPluginCollector;
        if (taskConfig.getKeys().contains("dataxParquetMode")) {
            this.dataxParquetMode = taskConfig.getString("dataxParquetMode");
        } else {
            // 默认值是columns
            this.dataxParquetMode = "columns";
        }
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Record values) {
        if (dataxParquetMode.equalsIgnoreCase("fields")) {
            writeBaseOnFields(values);
            return;
        }

        // NOTE: 下面的实现其实是不对的，只是看代码注释貌似有用户已经在用
        //       所以暂时不动下面的逻辑。
        // 默认走的就是下面的这条代码路径
        if (values != null && columns != null && values.getColumnNumber() == columns.size()) {
            recordConsumer.startMessage();
            for (int i = 0; i < columns.size(); i++) {
                Column value = values.getColumn(i);
                ColumnDescriptor columnDescriptor = columns.get(i);
                Type type = this.schema.getFields().get(i);
                if (value != null) {
                    try {
                        if (this.useRawDataTransf) {
                            if (value.getRawData() == null) {
                                continue;
                            }
                            recordConsumer.startField(columnDescriptor.getPath()[0], i);
                            // 原来使用Column->RawData的方法其实是错误的类型转换策略，会将DataX的数据内部表示形象序列化出去
                            // 但是 Parquet 已经有用户使用了，故暂时只是配置项切换
                            String rawData = value.getRawData().toString();
                            switch (columnDescriptor.getType()) {
                                case BOOLEAN:
                                    recordConsumer.addBoolean(Boolean.parseBoolean(rawData));
                                    break;
                                case FLOAT:
                                    recordConsumer.addFloat(Float.parseFloat(rawData));
                                    break;
                                case DOUBLE:
                                    recordConsumer.addDouble(Double.parseDouble(rawData));
                                    break;
                                case INT32:
                                    OriginalType originalType = type.getOriginalType();
                                    if (originalType != null && StringUtils.equalsIgnoreCase("DATE", originalType.name())) {
                                        int realVal = (int) (new java.sql.Date(Long.parseLong(rawData)).toLocalDate().toEpochDay());
                                        recordConsumer.addInteger(realVal);
                                    } else {
                                        recordConsumer.addInteger(Integer.parseInt(rawData));
                                    }
                                    break;
                                case INT64:
                                    recordConsumer.addLong(Long.valueOf(rawData));
                                    break;
                                case INT96:
                                    recordConsumer.addBinary(timestampColToBinary(value));
                                    break;
                                case BINARY:
                                    recordConsumer.addBinary(Binary.fromString(rawData));
                                    break;
                                case FIXED_LEN_BYTE_ARRAY:
                                    PrimitiveType primitiveType = type.asPrimitiveType();
                                    if (primitiveType.getDecimalMetadata() != null) {
                                        // decimal
                                        recordConsumer.addBinary(decimalToBinary(value, primitiveType.getDecimalMetadata().getPrecision(), primitiveType.getDecimalMetadata().getScale()));
                                        break;
                                    }
                                    /* fall through */
                                default:
                                    recordConsumer.addBinary(Binary.fromString(rawData));
                                    break;
                            }

                            recordConsumer.endField(columnDescriptor.getPath()[0], i);
                        } else {
                            boolean isNull = null == value.getRawData();

                            if (!isNull) {
                                recordConsumer.startField(columnDescriptor.getPath()[0], i);

                                // no skip: empty fields are illegal, the field should be ommited completely instead
                                switch (columnDescriptor.getType()) {
                                    case BOOLEAN:
                                        recordConsumer.addBoolean(value.asBoolean());
                                        break;
                                    case FLOAT:
                                        recordConsumer.addFloat(value.asDouble().floatValue());
                                        break;
                                    case DOUBLE:
                                        recordConsumer.addDouble(value.asDouble());
                                        break;
                                    case INT32:
                                        OriginalType originalType = type.getOriginalType();
                                        if (originalType != null && StringUtils.equalsIgnoreCase("DATE", originalType.name())) {
                                            int realVal = (int) (new java.sql.Date(value.asLong()).toLocalDate().toEpochDay());
                                            recordConsumer.addInteger(realVal);
                                        } else {
                                            recordConsumer.addInteger(value.asLong().intValue());
                                        }
                                        break;
                                    case INT64:
                                        recordConsumer.addLong(value.asLong());
                                        break;
                                    case INT96:
                                        recordConsumer.addBinary(timestampColToBinary(value));
                                        break;
                                    case BINARY:
                                        String valueAsString2Write = null;
                                        if (Column.Type.DATE == value.getType() && null != this.dateParse) {
                                            valueAsString2Write = dateParse.format(value.asDate());
                                        } else {
                                            valueAsString2Write = value.asString();
                                        }
                                        recordConsumer.addBinary(Binary.fromString(valueAsString2Write));
                                        break;
                                    case FIXED_LEN_BYTE_ARRAY:
                                        PrimitiveType primitiveType = type.asPrimitiveType();
                                        if (primitiveType.getDecimalMetadata() != null) {
                                            // decimal
                                            recordConsumer.addBinary(decimalToBinary(value, primitiveType.getDecimalMetadata().getPrecision(), primitiveType.getDecimalMetadata().getScale()));
                                            break;
                                        }
                                        /* fall through */
                                    default:
                                        recordConsumer.addBinary(Binary.fromString(value.asString()));
                                        break;
                                }
                                recordConsumer.endField(columnDescriptor.getPath()[0], i);
                            }
                        }
                    } catch (Exception e) {
                        if (printStackTrace) {
                            printStackTrace = false;
                            LOGGER.warn("write to parquet error: {}", e.getMessage(), e);
                        }
                        // dirty data
                        if (null != this.taskPluginCollector) {
                            // job post 里面的merge taskPluginCollector 为null
                            this.taskPluginCollector.collectDirtyRecord(values, e, e.getMessage());
                        }
                    }
                } else {
                    recordConsumer.addBinary(this.binaryForNull);
                }
            }
            recordConsumer.endMessage();
        }
    }

    private Binary decimalToBinary(Column value, int precision, int scale) {
        BigDecimal bigDecimal = value.asBigDecimal();
        bigDecimal = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
        byte[] decimalBytes = bigDecimal.unscaledValue().toByteArray();

        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[precision - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];

        // padding -1 for negative number
        if (bigDecimal.compareTo(new BigDecimal("0")) < 0) {
            Arrays.fill(tgt, 0, precToBytes - decimalBytes.length, (byte) -1);
        }

        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length);
        return Binary.fromByteArray(tgt);
    }

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long MILLS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
    private static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);
    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final ZoneOffset defaultOffset = OffsetDateTime.now().getOffset();

    /**
     * int 96 is timestamp in parquet
     *
     * @param valueColumn
     * @return
     */
    private Binary timestampColToBinary(Column valueColumn) {
        if (valueColumn.getRawData() == null) {
            return Binary.EMPTY;
        }
        long mills;
        long nanos = 0;
        if (valueColumn instanceof DateColumn) {
            DateColumn dateColumn = (DateColumn) valueColumn;
            mills = dateColumn.asLong();
            nanos = dateColumn.getNanos();
        } else {
            mills = valueColumn.asLong();
        }
        int julianDay;
        long nanosOfDay;
        if (isUtcTimestamp) {
            // utc ignore current timezone (task should set timezone same as hive/hdfs)
            long seconds = mills >= 0 ? mills / MILLS_PER_SECOND : (mills / MILLS_PER_SECOND - 1);
            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, (int) nanos, defaultOffset);
            julianDay = (int) (localDateTime.getLong(ChronoField.EPOCH_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
            nanosOfDay = localDateTime.getLong(ChronoField.NANO_OF_DAY);
        } else {
            // local date
            julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
            if (mills >= 0) {
                nanosOfDay = ((mills % MILLIS_IN_DAY) / MILLS_PER_SECOND) * NANOS_PER_SECOND + nanos;
            } else {
                julianDay--;
                nanosOfDay = (((mills % MILLIS_IN_DAY) / MILLS_PER_SECOND) - 1) * NANOS_PER_SECOND + nanos;
                nanosOfDay += NANOS_PER_DAY;
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(nanosOfDay);
        buf.putInt(julianDay);
        buf.flip();
        return Binary.fromByteBuffer(buf);
    }

    private void writeBaseOnFields(Record values) {
        //LOGGER.info("Writing parquet data using fields mode(The correct mode.)");
        List<Type> types = this.schema.getFields();

        if (values != null && types != null && values.getColumnNumber() == types.size()) {
            recordConsumer.startMessage();
            writeFields(types, values);
            recordConsumer.endMessage();
        }
    }

    private void writeFields(List<Type> types, Record values) {
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Column value = values.getColumn(i);
            if (value != null) {
                try {
                    if (type.isPrimitive()) {
                        writePrimitiveType(type, value, i);
                    } else {
                        writeGroupType(type, (JSON) JSON.parse(value.asString()), i);
                    }
                } catch (Exception e) {
                    if (printStackTrace) {
                        printStackTrace = false;
                        LOGGER.warn("write to parquet error: {}", e.getMessage(), e);
                    }
                    // dirty data
                    if (null != this.taskPluginCollector) {
                        // job post 里面的merge taskPluginCollector 为null
                        this.taskPluginCollector.collectDirtyRecord(values, e, e.getMessage());
                    }
                }
            }
        }
    }

    private void writeFields(List<Type> types, JSONObject values) {
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            Object value = values.get(type.getName());

            if (value != null) {
                try {
                    if (type.isPrimitive()) {
                        writePrimitiveType(type, value, i);
                    } else {
                        writeGroupType(type, (JSON) value, i);
                    }
                } catch (Exception e) {
                    if (printStackTrace) {
                        printStackTrace = false;
                        LOGGER.warn("write to parquet error: {}", e.getMessage(), e);
                    }
                }
            } else {
                recordConsumer.addBinary(this.binaryForNull);
            }
        }
    }

    private void writeGroupType(Type type, JSON value, int index) {
        GroupType groupType = type.asGroupType();
        OriginalType originalType = groupType.getOriginalType();
        if (originalType != null) {
            switch (originalType) {
                case MAP:
                    writeMap(groupType, value, index);
                    break;
                case LIST:
                    writeList(groupType, value, index);
                    break;
                default:
                    break;
            }
        } else {
            // struct
            writeStruct(groupType, value, index);
        }
    }

    private void writeMap(GroupType groupType, JSON value, int index) {
        if (value == null) {
            return;
        }

        JSONObject json = (JSONObject) value;

        if (json.isEmpty()) {
            return;
        }

        recordConsumer.startField(groupType.getName(), index);

        recordConsumer.startGroup();

        // map
        // key_value start
        recordConsumer.startField("key_value", 0);
        recordConsumer.startGroup();

        List<Type> keyValueFields = groupType.getFields().get(0).asGroupType().getFields();
        Type keyType = keyValueFields.get(0);
        Type valueType = keyValueFields.get(1);
        for (String key : json.keySet()) {
            // key
            writePrimitiveType(keyType, key, 0);

            // value
            if (valueType.isPrimitive()) {
                writePrimitiveType(valueType, json.get(key), 1);
            } else {
                writeGroupType(valueType, (JSON) json.get(key), 1);
            }
        }

        recordConsumer.endGroup();
        recordConsumer.endField("key_value", 0);
        // key_value end

        recordConsumer.endGroup();
        recordConsumer.endField(groupType.getName(), index);
    }

    private void writeList(GroupType groupType, JSON value, int index) {
        if (value == null) {
            return;
        }

        JSONArray json = (JSONArray) value;

        if (json.isEmpty()) {
            return;
        }

        recordConsumer.startField(groupType.getName(), index);
        // list
        recordConsumer.startGroup();

        // list start
        recordConsumer.startField("list", 0);
        recordConsumer.startGroup();

        Type elementType = groupType.getFields().get(0).asGroupType().getFields().get(0);

        if (elementType.isPrimitive()) {
            for (Object elementValue : json) {
                writePrimitiveType(elementType, elementValue, 0);
            }
        } else {
            for (Object elementValue : json) {
                writeGroupType(elementType, (JSON) elementValue, 0);
            }
        }

        recordConsumer.endGroup();
        recordConsumer.endField("list", 0);
        // list end
        recordConsumer.endGroup();

        recordConsumer.endField(groupType.getName(), index);
    }

    private void writeStruct(GroupType groupType, JSON value, int index) {
        if (value == null) {
            return;
        }
        JSONObject json = (JSONObject) value;
        if (json.isEmpty()) {
            return;
        }

        recordConsumer.startField(groupType.getName(), index);
        // struct start
        recordConsumer.startGroup();

        writeFields(groupType.getFields(), json);
        recordConsumer.endGroup();
        // struct end
        recordConsumer.endField(groupType.getName(), index);
    }

    private void writePrimitiveType(Type type, Object value, int index) {
        if (value == null) {
            return;
        }

        recordConsumer.startField(type.getName(), index);
        PrimitiveType primitiveType = type.asPrimitiveType();

        switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN:
                recordConsumer.addBoolean((Boolean) value);
                break;
            case FLOAT:
                if (value instanceof Float) {
                    recordConsumer.addFloat(((Float) value).floatValue());
                } else if (value instanceof Double) {
                    recordConsumer.addFloat(((Double) value).floatValue());
                } else if (value instanceof Long) {
                    recordConsumer.addFloat(((Long) value).floatValue());
                } else if (value instanceof Integer) {
                    recordConsumer.addFloat(((Integer) value).floatValue());
                }
                break;
            case DOUBLE:
                if (value instanceof Float) {
                    recordConsumer.addDouble(((Float) value).doubleValue());
                } else if (value instanceof Double) {
                    recordConsumer.addDouble(((Double) value).doubleValue());
                } else if (value instanceof Long) {
                    recordConsumer.addDouble(((Long) value).doubleValue());
                } else if (value instanceof Integer) {
                    recordConsumer.addDouble(((Integer) value).doubleValue());
                }
                break;
            case INT32:
                if (value instanceof Integer) {
                    recordConsumer.addInteger((Integer) value);
                } else if (value instanceof Long) {
                    recordConsumer.addInteger(((Long) value).intValue());
                } else {
                    // 之前代码写的有问题，导致这里丢列了没抛异常，先收集，后续看看有没有任务命中在决定怎么改
                    LimitLogger.limit("dirtyDataHiveWriterParquet", TimeUnit.MINUTES.toMillis(1), () -> LOGGER.warn("dirtyDataHiveWriterParquet {}", String.format("Invalid value: %s(clazz: %s) for field: %s", value, value.getClass(), type.getName())));
                }
                break;
            case INT64:
                if (value instanceof Integer) {
                    recordConsumer.addLong(((Integer) value).longValue());
                } else if (value instanceof Long) {
                    recordConsumer.addInteger(((Long) value).intValue());
                } else {
                    // 之前代码写的有问题，导致这里丢列了没抛异常，先收集，后续看看有没有任务命中在决定怎么改
                    LimitLogger.limit("dirtyDataHiveWriterParquet", TimeUnit.MINUTES.toMillis(1), () -> LOGGER.warn("dirtyDataHiveWriterParquet {}", String.format("Invalid value: %s(clazz: %s) for field: %s", value, value.getClass(), type.getName())));
                }
                break;
            case INT96:
                if (value instanceof Integer) {
                    recordConsumer.addBinary(timestampColToBinary(new LongColumn((Integer) value)));
                } else if (value instanceof Long) {
                    recordConsumer.addBinary(timestampColToBinary(new LongColumn((Long) value)));
                } else if (value instanceof Timestamp) {
                    recordConsumer.addBinary(timestampColToBinary(new DateColumn((Timestamp) value)));
                } else if (value instanceof Date) {
                    recordConsumer.addBinary(timestampColToBinary(new DateColumn((Date) value)));
                } else {
                    recordConsumer.addBinary(timestampColToBinary(new StringColumn(value.toString())));
                }
                break;
            case FIXED_LEN_BYTE_ARRAY:
                if (primitiveType.getDecimalMetadata() != null) {
                    // decimal
                    Column column;
                    if (value instanceof Integer) {
                        column = new LongColumn((Integer) value);
                    } else if (value instanceof Long) {
                        column = new LongColumn((Long) value);
                    } else if (value instanceof Double) {
                        column = new DoubleColumn((Double) value);
                    } else if (value instanceof BigDecimal) {
                        column = new DoubleColumn((BigDecimal) value);
                    } else {
                        column = new StringColumn(value.toString());
                    }
                    recordConsumer.addBinary(decimalToBinary(column, primitiveType.getDecimalMetadata().getPrecision(), primitiveType.getDecimalMetadata().getScale()));
                    break;
                }
                /* fall through */
            case BINARY:
            default:
                recordConsumer.addBinary(Binary.fromString((String) value));
                break;
        }
        recordConsumer.endField(type.getName(), index);
    }

    private void writePrimitiveType(Type type, Column value, int index) {
        if (value == null || value.getRawData() == null) {
            return;
        }

        recordConsumer.startField(type.getName(), index);
        PrimitiveType primitiveType = type.asPrimitiveType();
        switch (primitiveType.getPrimitiveTypeName()) {
            case BOOLEAN:
                recordConsumer.addBoolean(value.asBoolean());
                break;
            case FLOAT:
                recordConsumer.addFloat(value.asDouble().floatValue());
                break;
            case DOUBLE:
                recordConsumer.addDouble(value.asDouble());
                break;
            case INT32:
                OriginalType originalType = type.getOriginalType();
                if (OriginalType.DATE.equals(originalType)) {
                    int realVal = (int) (new java.sql.Date(value.asLong()).toLocalDate().toEpochDay());
                    recordConsumer.addInteger(realVal);
                } else {
                    recordConsumer.addInteger(value.asLong().intValue());
                }
                break;
            case INT64:
                recordConsumer.addLong(value.asLong());
                break;
            case INT96:
                recordConsumer.addBinary(timestampColToBinary(value));
                break;
            case BINARY:
                String valueAsString2Write = null;
                if (Column.Type.DATE == value.getType() && null != this.dateParse) {
                    valueAsString2Write = dateParse.format(value.asDate());
                } else {
                    valueAsString2Write = value.asString();
                }
                recordConsumer.addBinary(Binary.fromString(valueAsString2Write));
                break;
            case FIXED_LEN_BYTE_ARRAY:
                if (primitiveType.getDecimalMetadata() != null) {
                    // decimal
                    recordConsumer.addBinary(decimalToBinary(value, primitiveType.getDecimalMetadata().getPrecision(), primitiveType.getDecimalMetadata().getScale()));
                    break;
                }
                /* fall through */
            default:
                recordConsumer.addBinary(Binary.fromString(value.asString()));
                break;
        }
        recordConsumer.endField(type.getName(), index);
    }
}
