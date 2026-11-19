package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;

import java.sql.Date;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class IoTDBReaderRecordMapper {
    private IoTDBReaderRecordMapper() {
    }

    static void append(RowRecord row, Record record, List<String> columnNames,
                       List<String> columnTypes, int timeColumnPosition) {
        List<Field> fields = row.getFields();
        List<Integer> metadataIndexes = metadataIndexes(columnNames, fields.size());
        int timeFieldIndex = timeFieldIndex(columnNames, fields.size());
        int valueCount = fields.size() - (timeFieldIndex >= 0 ? 1 : 0);
        if (timeColumnPosition > valueCount) {
            throw new IllegalArgumentException("timeColumnPosition exceeds the query result width.");
        }

        long timestamp = row.getTimestamp();
        if (timeFieldIndex >= 0) {
            int metadataIndex = metadataIndexes.get(timeFieldIndex);
            TSDataType type = fieldType(fields.get(timeFieldIndex), columnTypes, metadataIndex);
            timestamp = timestampValue(fields.get(timeFieldIndex), type, timestamp);
        }

        int outputIndex = 0;
        for (int fieldIndex = 0; fieldIndex <= fields.size(); fieldIndex++) {
            if (outputIndex == timeColumnPosition) {
                record.addColumn(new DateColumn(timestamp));
                outputIndex++;
            }
            if (fieldIndex < fields.size() && fieldIndex != timeFieldIndex) {
                int metadataIndex = metadataIndexes.get(fieldIndex);
                TSDataType fallbackType = fieldType(fields.get(fieldIndex), columnTypes, metadataIndex);
                record.addColumn(toColumn(fields.get(fieldIndex), fallbackType));
                outputIndex++;
            }
        }
    }

    private static List<Integer> metadataIndexes(List<String> columnNames, int fieldCount) {
        List<Integer> indexes = new ArrayList<>(fieldCount);
        if (columnNames == null || columnNames.isEmpty()) {
            for (int i = 0; i < fieldCount; i++) {
                indexes.add(i);
            }
            return indexes;
        }
        int timeIndex = findTimeColumn(columnNames);
        if (columnNames.size() == fieldCount) {
            for (int i = 0; i < fieldCount; i++) {
                indexes.add(i);
            }
        } else if (timeIndex >= 0 && columnNames.size() - 1 == fieldCount) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (i != timeIndex) {
                    indexes.add(i);
                }
            }
        } else if (timeIndex == 0 && columnNames.size() > fieldCount) {
            for (int i = 0; i < fieldCount; i++) {
                indexes.add(i + 1);
            }
        } else {
            for (int i = 0; i < fieldCount; i++) {
                indexes.add(i);
            }
        }
        return indexes;
    }

    private static int timeFieldIndex(List<String> columnNames, int fieldCount) {
        if (columnNames == null || columnNames.size() != fieldCount) {
            return -1;
        }
        return findTimeColumn(columnNames);
    }

    private static int findTimeColumn(List<String> columnNames) {
        if (columnNames == null) {
            return -1;
        }
        for (int i = 0; i < columnNames.size(); i++) {
            if ("time".equalsIgnoreCase(columnNames.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static TSDataType fieldType(Field field, List<String> columnTypes, int metadataIndex) {
        if (field != null && field.getDataType() != null) {
            return field.getDataType();
        }
        if (columnTypes != null && metadataIndex >= 0 && metadataIndex < columnTypes.size()) {
            try {
                return TSDataType.valueOf(columnTypes.get(metadataIndex).trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        return null;
    }

    private static long timestampValue(Field field, TSDataType type, long fallback) {
        if (field == null || type == null) {
            return fallback;
        }
        if (type == TSDataType.DATE) {
            return Date.valueOf(field.getDateV()).toLocalDate().atStartOfDay()
                    .toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        switch (type) {
            case INT32:
                return field.getIntV();
            case INT64:
            case TIMESTAMP:
                return field.getLongV();
            default:
                throw new IllegalArgumentException("The selected time column is not an integer timestamp.");
        }
    }

    static Column toColumn(Field field, TSDataType fallbackType) {
        TSDataType type = field == null ? null : field.getDataType();
        if (type == null) {
            type = fallbackType;
        }
        if (type == null) {
            throw new IllegalArgumentException("Cannot determine the IoTDB type of a null result value.");
        }
        if (field == null || field.getDataType() == null) {
            return nullColumn(type);
        }

        switch (type) {
            case BOOLEAN:
                return new BoolColumn(field.getBoolV());
            case INT32:
                return new LongColumn((long) field.getIntV());
            case INT64:
            case TIMESTAMP:
                return new LongColumn(field.getLongV());
            case FLOAT:
                return new DoubleColumn((double) field.getFloatV());
            case DOUBLE:
                return new DoubleColumn(field.getDoubleV());
            case STRING:
            case TEXT:
                return new StringColumn(field.getStringValue());
            case DATE:
                return new DateColumn(Date.valueOf(field.getDateV()));
            case BLOB:
                Binary binary = field.getBinaryV();
                return new BytesColumn(binary == null ? null : binary.getValues());
            default:
                throw new IllegalArgumentException("Unsupported IoTDB result type: " + type);
        }
    }

    private static Column nullColumn(TSDataType type) {
        switch (type) {
            case BOOLEAN:
                return new BoolColumn((Boolean) null);
            case INT32:
            case INT64:
            case TIMESTAMP:
                return new LongColumn((Long) null);
            case FLOAT:
            case DOUBLE:
                return new DoubleColumn((Double) null);
            case STRING:
            case TEXT:
                return new StringColumn((String) null);
            case DATE:
                return new DateColumn((Long) null);
            case BLOB:
                return new BytesColumn((byte[]) null);
            default:
                throw new IllegalArgumentException("Unsupported null IoTDB result type: " + type);
        }
    }
}
