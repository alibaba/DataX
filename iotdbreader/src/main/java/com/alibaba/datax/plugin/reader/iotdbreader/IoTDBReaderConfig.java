package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.isession.util.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class IoTDBReaderConfig {
    private IoTDBReaderConfig() {
    }

    static void validateAndSetDefaults(Configuration config) {
        String model = config.getString(Key.MODEL, "tree").trim().toLowerCase(Locale.ROOT);
        if (!"tree".equals(model) && !"table".equals(model)) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.ILLEGAL_VALUE,
                    "model must be either 'tree' or 'table'.");
        }
        config.set(Key.MODEL, model);

        require(config.getString(Key.HOST), Key.HOST);
        require(config.getString(Key.USERNAME), Key.USERNAME);
        require(config.getString(Key.PASSWORD), Key.PASSWORD);

        int port = config.getInt(Key.PORT, -1);
        if (port < 1 || port > 65535) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.ILLEGAL_VALUE,
                    "port must be between 1 and 65535.");
        }
        int fetchSize = config.getInt(Key.FETCH_SIZE, 1000);
        if (fetchSize < 1) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.ILLEGAL_VALUE,
                    "fetchSize must be greater than zero.");
        }
        int timePosition = config.getInt(Key.TIME_COLUMN_POSITION, 0);
        if (timePosition < 0) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.ILLEGAL_VALUE,
                    "timeColumnPosition cannot be negative.");
        }
        long queryTimeout = config.getLong(Key.QUERY_TIMEOUT_MS, 60000L);
        if (queryTimeout < 1) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.ILLEGAL_VALUE,
                    "queryTimeoutMs must be greater than zero.");
        }
        config.set(Key.FETCH_SIZE, fetchSize);
        config.set(Key.TIME_COLUMN_POSITION, timePosition);
        config.set(Key.QUERY_TIMEOUT_MS, queryTimeout);

        if ("tree".equals(model)) {
            String version = config.getString(Key.VERSION, Version.V_1_0.name());
            try {
                Version.valueOf(version);
            } catch (IllegalArgumentException e) {
                throw DataXException.asDataXException(IoTDBReaderErrorCode.ILLEGAL_VALUE,
                        "version must be a supported IoTDB session protocol version.");
            }
            config.set(Key.VERSION, version);
        } else {
            config.set(Key.DATABASE, config.getString(Key.DATABASE, ""));
        }

        queries(config);
    }

    static List<String> queries(Configuration config) {
        List<String> configured = config.getList(Key.QUERY_SQLS, String.class);
        List<String> result = new ArrayList<>();
        if (configured != null && !configured.isEmpty()) {
            for (String query : configured) {
                if (query == null || query.trim().isEmpty()) {
                    throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE,
                            "querySqls cannot contain an empty statement.");
                }
                result.add(query.trim());
            }
            return result;
        }

        String query = config.getString(Key.QUERY_SQL);
        if (query != null && !query.trim().isEmpty()) {
            result.add(query.trim());
            return result;
        }

        if ("table".equals(config.getString(Key.MODEL, "tree"))) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE,
                    "model=table requires querySql or querySqls.");
        }

        String device = config.getString(Key.DEVICE);
        List<String> measurements = config.getList(Key.MEASUREMENTS, String.class);
        require(device, Key.DEVICE);
        if (measurements == null || measurements.isEmpty()) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE,
                    "measurements is required when querySqls and querySql are empty.");
        }
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(String.join(",", measurements))
                .append(" FROM ").append(device);
        String where = config.getString(Key.WHERE);
        if (where != null && !where.trim().isEmpty()) {
            sql.append(" WHERE ").append(where.trim());
        }
        result.add(sql.toString());
        return result;
    }

    private static void require(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE,
                    "The parameter [" + key + "] is required.");
        }
    }
}
