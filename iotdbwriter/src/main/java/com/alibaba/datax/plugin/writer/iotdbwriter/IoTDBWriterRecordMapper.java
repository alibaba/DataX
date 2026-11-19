package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

final class IoTDBWriterRecordMapper {
    private IoTDBWriterRecordMapper() {
    }

    static Tablet newTablet(IoTDBWriterConfig config, int maxRows) {
        if ("table".equals(config.model)) {
            return new Tablet(config.table, config.columnNames, config.columnTypes,
                    config.columnCategories, maxRows);
        }
        List<IMeasurementSchema> schemas = new ArrayList<>(config.columnNames.size());
        for (int i = 0; i < config.columnNames.size(); i++) {
            schemas.add(new MeasurementSchema(config.columnNames.get(i), config.columnTypes.get(i)));
        }
        return new Tablet(config.device, schemas, maxRows);
    }

    static void append(Record record, Tablet tablet, IoTDBWriterConfig config, int rowIndex) {
        int expectedColumns = config.columnNames.size() + 1;
        if (record == null || record.getColumnNumber() != expectedColumns) {
            throw new IllegalArgumentException("Expected " + expectedColumns
                    + " input columns including time, but received "
                    + (record == null ? "null" : record.getColumnNumber()) + ".");
        }

        Column timeColumn = record.getColumn(config.timeColumnPosition);
        if (timeColumn == null || timeColumn.getRawData() == null) {
            throw new IllegalArgumentException("The time column must not be null.");
        }
        long timestamp = timeColumn.asLong();
        Object[] values = new Object[config.columnNames.size()];
        int inputIndex = 0;
        for (int valueIndex = 0; valueIndex < values.length; valueIndex++) {
            if (inputIndex == config.timeColumnPosition) {
                inputIndex++;
            }
            values[valueIndex] = convert(record.getColumn(inputIndex), config.columnTypes.get(valueIndex));
            inputIndex++;
        }

        tablet.addTimestamp(rowIndex, timestamp);
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (config.columnTypes.get(i) == TSDataType.BLOB && value != null) {
                value = new Binary((byte[]) value);
            }
            tablet.addValue(config.columnNames.get(i), rowIndex, value);
        }
    }

    private static Object convert(Column column, TSDataType type) {
        if (column == null || column.getRawData() == null) {
            return null;
        }
        switch (type) {
            case BOOLEAN:
                return column.asBoolean();
            case INT32:
                return decimalValue(column).intValueExact();
            case INT64:
            case TIMESTAMP:
                if (column.getType() == Column.Type.DATE) {
                    return column.asLong();
                }
                return decimalValue(column).longValueExact();
            case FLOAT:
                float floatValue = decimalValue(column).floatValue();
                if (Float.isInfinite(floatValue)) {
                    throw new IllegalArgumentException("Value is outside the IoTDB FLOAT range.");
                }
                return floatValue;
            case DOUBLE:
                return column.asDouble();
            case STRING:
            case TEXT:
                return column.asString();
            case BLOB:
                if (column.getType() == Column.Type.BYTES) {
                    return column.asBytes();
                }
                return column.asString().getBytes(StandardCharsets.UTF_8);
            case DATE:
                if (column.getType() == Column.Type.DATE) {
                    return new Date(column.asLong()).toLocalDate();
                }
                return LocalDate.parse(column.asString());
            default:
                throw new IllegalArgumentException("Unsupported IoTDB type: " + type);
        }
    }

    private static BigDecimal decimalValue(Column column) {
        BigDecimal value = column.asBigDecimal();
        if (value == null) {
            throw new IllegalArgumentException("A non-null numeric value is required.");
        }
        return value;
    }
}
