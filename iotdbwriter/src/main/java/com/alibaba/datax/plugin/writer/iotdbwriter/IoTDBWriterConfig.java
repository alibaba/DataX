package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.isession.util.Version;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class IoTDBWriterConfig {
    final String model;
    final String device;
    final String table;
    final List<String> columnNames;
    final List<TSDataType> columnTypes;
    final List<ColumnCategory> columnCategories;
    final int timeColumnPosition;
    final int batchSize;

    private IoTDBWriterConfig(String model, String device, String table,
                              List<String> columnNames, List<TSDataType> columnTypes,
                              List<ColumnCategory> columnCategories,
                              int timeColumnPosition, int batchSize) {
        this.model = model;
        this.device = device;
        this.table = table;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnCategories = columnCategories;
        this.timeColumnPosition = timeColumnPosition;
        this.batchSize = batchSize;
    }

    static IoTDBWriterConfig validateAndSetDefaults(Configuration config) {
        String model = config.getString(Key.MODEL, "tree").trim().toLowerCase(Locale.ROOT);
        if (!"tree".equals(model) && !"table".equals(model)) {
            throw invalid("model must be either 'tree' or 'table'.");
        }
        config.set(Key.MODEL, model);
        require(config.getString(Key.HOST), Key.HOST);
        require(config.getString(Key.USERNAME), Key.USERNAME);
        require(config.getString(Key.PASSWORD), Key.PASSWORD);

        int port = config.getInt(Key.PORT, -1);
        if (port < 1 || port > 65535) {
            throw invalid("port must be between 1 and 65535.");
        }
        int batchSize = config.getInt(Key.BATCH_SIZE, 1000);
        if (batchSize < 1 || batchSize > 100000) {
            throw invalid("batchSize must be between 1 and 100000.");
        }
        int timePosition = config.getInt(Key.TIME_COLUMN_POSITION, 0);
        if (timePosition < 0) {
            throw invalid("timeColumnPosition cannot be negative.");
        }
        long timeout = config.getLong(Key.QUERY_TIMEOUT_MS, 60000L);
        if (timeout < 1) {
            throw invalid("queryTimeoutMs must be greater than zero.");
        }
        config.set(Key.BATCH_SIZE, batchSize);
        config.set(Key.TIME_COLUMN_POSITION, timePosition);
        config.set(Key.QUERY_TIMEOUT_MS, timeout);

        if ("tree".equals(model)) {
            String version = config.getString(Key.VERSION, Version.V_1_0.name());
            try {
                Version.valueOf(version);
            } catch (IllegalArgumentException e) {
                throw invalid("version must be a supported IoTDB session protocol version.");
            }
            config.set(Key.VERSION, version);
        }

        List<String> names;
        List<String> rawTypes;
        List<TSDataType> types = new ArrayList<>();
        List<ColumnCategory> categories = new ArrayList<>();
        String device = null;
        String table = null;
        if ("tree".equals(model)) {
            device = config.getString(Key.DEVICE);
            require(device, Key.DEVICE);
            names = config.getList(Key.MEASUREMENTS, String.class);
            if (names == null || names.isEmpty()) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE,
                        "measurements is required for model=tree.");
            }
        } else {
            table = config.getString(Key.TABLE);
            require(table, Key.TABLE);
            names = config.getList(Key.COLUMN_NAMES, String.class);
            if (names == null || names.isEmpty()) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE,
                        "columnNames is required for model=table.");
            }
        }
        validateNames(names);

        rawTypes = config.getList(Key.DATA_TYPES, String.class);
        if (rawTypes == null || rawTypes.size() != names.size()) {
            throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE,
                    "dataTypes must contain one IoTDB type for each measurement or table column.");
        }
        for (String type : rawTypes) {
            TSDataType parsed;
            try {
                parsed = TSDataType.valueOf(type.trim().toUpperCase(Locale.ROOT));
            } catch (RuntimeException e) {
                throw invalid("Unsupported IoTDB data type: " + type);
            }
            if (!isSupported(parsed)) {
                throw invalid("Unsupported IoTDB data type: " + type);
            }
            types.add(parsed);
        }

        if ("table".equals(model)) {
            List<String> rawCategories = config.getList(Key.COLUMN_CATEGORIES, String.class);
            if (rawCategories == null || rawCategories.isEmpty()) {
                for (int i = 0; i < names.size(); i++) {
                    categories.add(ColumnCategory.FIELD);
                }
            } else {
                if (rawCategories.size() != names.size()) {
                    throw invalid("columnCategories must have the same length as columnNames.");
                }
                boolean nonTagSeen = false;
                for (String category : rawCategories) {
                    ColumnCategory parsed;
                    try {
                        parsed = ColumnCategory.valueOf(category.trim().toUpperCase(Locale.ROOT));
                    } catch (RuntimeException e) {
                        throw invalid("columnCategories supports TAG, ATTRIBUTE, and FIELD only.");
                    }
                    if (parsed == ColumnCategory.TIME) {
                        throw invalid("Do not include TIME in columnCategories; timeColumnPosition identifies time.");
                    }
                    if (parsed != ColumnCategory.TAG) {
                        nonTagSeen = true;
                    } else if (nonTagSeen) {
                        throw invalid("TAG columns must precede ATTRIBUTE and FIELD columns.");
                    }
                    categories.add(parsed);
                }
            }
        }

        if (timePosition > names.size()) {
            throw invalid("timeColumnPosition must be within the input record width.");
        }
        return new IoTDBWriterConfig(model, device, table, new ArrayList<>(names), types,
                categories, timePosition, batchSize);
    }

    private static boolean isSupported(TSDataType type) {
        switch (type) {
            case BOOLEAN:
            case INT32:
            case INT64:
            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TEXT:
            case BLOB:
            case DATE:
                return true;
            default:
                return false;
        }
    }

    private static void validateNames(List<String> names) {
        Set<String> uniqueNames = new HashSet<>();
        for (String name : names) {
            if (name == null || name.trim().isEmpty()) {
                throw invalid("measurement and column names must not be empty.");
            }
            if (!uniqueNames.add(name)) {
                throw invalid("measurement and column names must be unique: " + name);
            }
        }
    }

    private static void require(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE,
                    "The parameter [" + key + "] is required.");
        }
    }

    private static DataXException invalid(String message) {
        return DataXException.asDataXException(IoTDBWriterErrorCode.ILLEGAL_VALUE, message);
    }
}
