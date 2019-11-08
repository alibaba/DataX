package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.tsdbreader.Constant;
import com.alibaba.datax.plugin.reader.tsdbreader.util.HttpUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Dump
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
final class TSDBDump {

    private static final Logger LOG = LoggerFactory.getLogger(TSDBDump.class);

    private static final String QUERY = "/api/query";
    private static final String QUERY_MULTI_FIELD = "/api/mquery";

    static {
        JSON.DEFAULT_PARSER_FEATURE &= ~Feature.UseBigDecimal.getMask();
    }

    private TSDBDump() {
    }

    static void dump4TSDB(TSDBConnection conn, String metric, Map<String, String> tags,
                          Long start, Long end, RecordSender sender) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4SingleField(conn, metric, tags, start, end);
        List<String> dps = getDps4TSDB(metric, res);
        if (dps == null || dps.isEmpty()) {
            return;
        }
        sendTSDBDps(sender, dps);
    }

    static void dump4TSDB(TSDBConnection conn, String metric, List<String> fields, Map<String, String> tags,
                          Long start, Long end, RecordSender sender) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4MultiFields(conn, metric, fields, tags, start, end);
        List<String> dps = getDps4TSDB(metric, fields, res);
        if (dps == null || dps.isEmpty()) {
            return;
        }
        sendTSDBDps(sender, dps);
    }

    static void dump4RDB(TSDBConnection conn, String metric, Map<String, String> tags,
                         Long start, Long end, List<String> columns4RDB, RecordSender sender) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4SingleField(conn, metric, tags, start, end);
        List<DataPoint4TSDB> dps = getDps4RDB(metric, res);
        if (dps == null || dps.isEmpty()) {
            return;
        }
        for (DataPoint4TSDB dp : dps) {
            final Record record = sender.createRecord();
            final Map<String, Object> tagKV = dp.getTags();

            for (String column : columns4RDB) {
                if (Constant.METRIC_SPECIFY_KEY.equals(column)) {
                    record.addColumn(new StringColumn(dp.getMetric()));
                } else if (Constant.TS_SPECIFY_KEY.equals(column)) {
                    record.addColumn(new LongColumn(dp.getTimestamp()));
                } else if (Constant.VALUE_SPECIFY_KEY.equals(column)) {
                    record.addColumn(getColumn(dp.getValue()));
                } else {
                    final Object tagk = tagKV.get(column);
                    if (tagk == null) {
                        continue;
                    }
                    record.addColumn(getColumn(tagk));
                }
            }
            sender.sendToWriter(record);
        }
    }

    static void dump4RDB(TSDBConnection conn, String metric, List<String> fields,
                         Map<String, String> tags, Long start, Long end,
                         List<String> columns4RDB, RecordSender sender) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4MultiFields(conn, metric, fields, tags, start, end);
        List<DataPoint4TSDB> dps = getDps4RDB(metric, fields, res);
        if (dps == null || dps.isEmpty()) {
            return;
        }
        for (DataPoint4TSDB dp : dps) {
            final Record record = sender.createRecord();
            final Map<String, Object> tagKV = dp.getTags();

            for (String column : columns4RDB) {
                if (Constant.METRIC_SPECIFY_KEY.equals(column)) {
                    record.addColumn(new StringColumn(dp.getMetric()));
                } else if (Constant.TS_SPECIFY_KEY.equals(column)) {
                    record.addColumn(new LongColumn(dp.getTimestamp()));
                } else {
                    final Object tagvOrField = tagKV.get(column);
                    if (tagvOrField == null) {
                        continue;
                    }
                    record.addColumn(getColumn(tagvOrField));
                }
            }
            sender.sendToWriter(record);
        }
    }

    private static Column getColumn(Object value) throws Exception {
        Column valueColumn;
        if (value instanceof Double) {
            valueColumn = new DoubleColumn((Double) value);
        } else if (value instanceof Long) {
            valueColumn = new LongColumn((Long) value);
        } else if (value instanceof String) {
            valueColumn = new StringColumn((String) value);
        } else {
            throw new Exception(String.format("value 不支持类型: [%s]", value.getClass().getSimpleName()));
        }
        return valueColumn;
    }

    private static String queryRange4SingleField(TSDBConnection conn, String metric, Map<String, String> tags,
                                                 Long start, Long end) throws Exception {
        String tagKV = getFilterByTags(tags);
        String body = "{\n" +
                "  \"start\": " + start + ",\n" +
                "  \"end\": " + end + ",\n" +
                "  \"queries\": [\n" +
                "    {\n" +
                "      \"aggregator\": \"none\",\n" +
                "      \"metric\": \"" + metric + "\"\n" +
                (tagKV == null ? "" : tagKV) +
                "    }\n" +
                "  ]\n" +
                "}";
        return HttpUtils.post(conn.address() + QUERY, body);
    }

    private static String queryRange4MultiFields(TSDBConnection conn, String metric, List<String> fields,
                                                 Map<String, String> tags, Long start, Long end) throws Exception {
        // fields
        StringBuilder fieldBuilder = new StringBuilder();
        fieldBuilder.append("\"fields\":[");
        for (int i = 0; i < fields.size(); i++) {
            fieldBuilder.append("{\"field\": \"").append(fields.get(i)).append("\",\"aggregator\": \"none\"}");
            if (i != fields.size() - 1) {
                fieldBuilder.append(",");
            }
        }
        fieldBuilder.append("]");
        // tagkv
        String tagKV = getFilterByTags(tags);
        String body = "{\n" +
                "  \"start\": " + start + ",\n" +
                "  \"end\": " + end + ",\n" +
                "  \"queries\": [\n" +
                "    {\n" +
                "      \"aggregator\": \"none\",\n" +
                "      \"metric\": \"" + metric + "\",\n" +
                fieldBuilder.toString() +
                (tagKV == null ? "" : tagKV) +
                "    }\n" +
                "  ]\n" +
                "}";
        return HttpUtils.post(conn.address() + QUERY_MULTI_FIELD, body);
    }

    private static String getFilterByTags(Map<String, String> tags) {
        if (tags != null && !tags.isEmpty()) {
            // tagKV = ",\"tags:\":" + JSON.toJSONString(tags);
            StringBuilder tagBuilder = new StringBuilder();
            tagBuilder.append(",\"filters\":[");
            int count = 1;
            final int size = tags.size();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                final String tagK = entry.getKey();
                final String tagV = entry.getValue();
                tagBuilder.append("{\"type\":\"literal_or\",\"tagk\":\"").append(tagK)
                        .append("\",\"filter\":\"").append(tagV).append("\",\"groupBy\":false}");
                if (count != size) {
                    tagBuilder.append(",");
                }
                count++;
            }
            tagBuilder.append("]");
            return tagBuilder.toString();
        }
        return null;
    }

    private static List<String> getDps4TSDB(String metric, String dps) {
        final List<QueryResult> jsonArray = JSON.parseArray(dps, QueryResult.class);
        if (jsonArray.size() == 0) {
            return null;
        }
        List<String> dpsArr = new LinkedList<>();
        for (QueryResult queryResult : jsonArray) {
            final Map<String, Object> tags = queryResult.getTags();
            final Map<String, Object> points = queryResult.getDps();
            for (Map.Entry<String, Object> entry : points.entrySet()) {
                final String ts = entry.getKey();
                final Object value = entry.getValue();

                DataPoint4TSDB dp = new DataPoint4TSDB();
                dp.setMetric(metric);
                dp.setTags(tags);
                dp.setTimestamp(Long.parseLong(ts));
                dp.setValue(value);
                dpsArr.add(dp.toString());
            }
        }
        return dpsArr;
    }

    private static List<String> getDps4TSDB(String metric, List<String> fields, String dps) {
        final List<MultiFieldQueryResult> jsonArray = JSON.parseArray(dps, MultiFieldQueryResult.class);
        if (jsonArray.size() == 0) {
            return null;
        }
        List<String> dpsArr = new LinkedList<>();
        for (MultiFieldQueryResult queryResult : jsonArray) {
            final Map<String, Object> tags = queryResult.getTags();
            final List<List<Object>> values = queryResult.getValues();
            for (List<Object> value : values) {
                final String ts = value.get(0).toString();
                Map<String, Object> fieldsAndValues = new HashMap<>();
                for (int i = 0; i < fields.size(); i++) {
                    fieldsAndValues.put(fields.get(i), value.get(i + 1));
                }

                final DataPoint4MultiFieldsTSDB dp = new DataPoint4MultiFieldsTSDB();
                dp.setMetric(metric);
                dp.setTimestamp(Long.parseLong(ts));
                dp.setTags(tags);
                dp.setFields(fieldsAndValues);
                dpsArr.add(dp.toString());
            }
        }
        return dpsArr;
    }

    private static List<DataPoint4TSDB> getDps4RDB(String metric, String dps) {
        final List<QueryResult> jsonArray = JSON.parseArray(dps, QueryResult.class);
        if (jsonArray.size() == 0) {
            return null;
        }
        List<DataPoint4TSDB> dpsArr = new LinkedList<>();
        for (QueryResult queryResult : jsonArray) {
            final Map<String, Object> tags = queryResult.getTags();
            final Map<String, Object> points = queryResult.getDps();
            for (Map.Entry<String, Object> entry : points.entrySet()) {
                final String ts = entry.getKey();
                final Object value = entry.getValue();

                final DataPoint4TSDB dp = new DataPoint4TSDB();
                dp.setMetric(metric);
                dp.setTags(tags);
                dp.setTimestamp(Long.parseLong(ts));
                dp.setValue(value);
                dpsArr.add(dp);
            }
        }
        return dpsArr;
    }

    private static List<DataPoint4TSDB> getDps4RDB(String metric, List<String> fields, String dps) {
        final List<MultiFieldQueryResult> jsonArray = JSON.parseArray(dps, MultiFieldQueryResult.class);
        if (jsonArray.size() == 0) {
            return null;
        }
        List<DataPoint4TSDB> dpsArr = new LinkedList<>();
        for (MultiFieldQueryResult queryResult : jsonArray) {
            final Map<String, Object> tags = queryResult.getTags();
            final List<List<Object>> values = queryResult.getValues();
            for (List<Object> value : values) {
                final String ts = value.get(0).toString();
                Map<String, Object> tagsTmp = new HashMap<>(tags);
                for (int i = 0; i < fields.size(); i++) {
                    tagsTmp.put(fields.get(i), value.get(i + 1));
                }

                final DataPoint4TSDB dp = new DataPoint4TSDB();
                dp.setMetric(metric);
                dp.setTimestamp(Long.parseLong(ts));
                dp.setTags(tagsTmp);
                dpsArr.add(dp);
            }
        }
        return dpsArr;
    }

    private static void sendTSDBDps(RecordSender sender, List<String> dps) {
        for (String dp : dps) {
            StringColumn tsdbColumn = new StringColumn(dp);
            Record record = sender.createRecord();
            record.addColumn(tsdbColumn);
            sender.sendToWriter(record);
        }
    }
}
