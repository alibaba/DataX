package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.tsdbreader.Constant;
import com.alibaba.datax.plugin.reader.tsdbreader.util.HttpUtils;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONReader.Feature;
import com.alibaba.fastjson2.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.alibaba.datax.plugin.reader.tsdbreader.Constant.METRIC_SPECIFY_KEY_PREFIX_LENGTH;

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
        JSON.config(Feature.UseBigDecimalForDoubles);
    }

    private TSDBDump() {
    }

    static void dump4TSDB(TSDBConnection conn, String metric, Map<String, String> tags,
                          Long start, Long end, RecordSender sender, Map<String, Object> hint) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4SingleField(conn, metric, tags, start, end, hint);
        List<String> dps = getDps4TSDB(metric, res);
        if (dps == null || dps.isEmpty()) {
            return;
        }
        sendTSDBDps(sender, dps);
    }

    static void dump4TSDB(TSDBConnection conn, String metric, List<String> fields, Map<String, String> tags,
                          Long start, Long end, RecordSender sender, Map<String, Object> hint) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4MultiFields(conn, metric, fields, tags, start, end, hint);
        List<String> dps = getDps4TSDB(metric, fields, res);
        if (dps == null || dps.isEmpty()) {
            return;
        }
        sendTSDBDps(sender, dps);
    }

    static void dump4RDB(TSDBConnection conn, String metric, Map<String, String> tags,
                         Long start, Long end, List<String> columns4RDB, RecordSender sender, Map<String, Object> hint) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4SingleField(conn, metric, tags, start, end, hint);
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

    public static void dump4RDB(TSDBConnection conn, List<String> metrics, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender sender, Map<String, Object> hint) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metrics, start, end);

        List<DataPoint4TSDB> dps = new LinkedList<>();
        for (String metric : metrics) {
            String res = queryRange4SingleField(conn, metric, tags, start, end, hint);
            final List<DataPoint4TSDB> dpList = getDps4RDB(metric, res);
            if (dpList == null || dpList.isEmpty()) {
                continue;
            }
            dps.addAll(dpList);
        }
        if (dps.isEmpty()) {
            return;
        }
        Map<Long, Map<String, DataPoint4TSDB>> dpsCombinedByTs = new LinkedHashMap<>();
        for (DataPoint4TSDB dp : dps) {
            final long ts = dp.getTimestamp();
            final Map<String, DataPoint4TSDB> dpsWithSameTs = dpsCombinedByTs.computeIfAbsent(ts, k -> new LinkedHashMap<>());
            dpsWithSameTs.put(dp.getMetric(), dp);
        }

        for (Map.Entry<Long, Map<String, DataPoint4TSDB>> entry : dpsCombinedByTs.entrySet()) {
            final Long ts = entry.getKey();
            final Map<String, DataPoint4TSDB> metricAndDps = entry.getValue();
            final Record record = sender.createRecord();
            DataPoint4TSDB tmpDp = null;

            for (final String column : columns4RDB) {
                if (column.startsWith(Constant.METRIC_SPECIFY_KEY)) {
                    final String m = column.substring(METRIC_SPECIFY_KEY_PREFIX_LENGTH);
                    tmpDp = metricAndDps.get(m);
                    if (tmpDp == null) {
                        continue;
                    }
                    record.addColumn(getColumn(tmpDp.getValue()));
                } else if (Constant.TS_SPECIFY_KEY.equals(column)) {
                    record.addColumn(new LongColumn(ts));
                } else if (Constant.VALUE_SPECIFY_KEY.equals(column)) {
                    // combine 模式下，不应该定义 __value__ 字段，因为 __metric__.xxx 字段会输出对应的 value 值
                    throw new RuntimeException("The " + Constant.VALUE_SPECIFY_KEY +
                            " column should not be specified in combine mode!");
                } else {
                    // combine 模式下，应该确保 __metric__.xxx 字段的定义，放在 column 数组的最前面，以保证获取到 metric
                    if (tmpDp == null) {
                        throw new RuntimeException("These " + Constant.METRIC_SPECIFY_KEY_PREFIX +
                                " column should be placed first in the column array in combine mode!");
                    }
                    final Object tagv = tmpDp.getTags().get(column);
                    if (tagv == null) {
                        continue;
                    }
                    record.addColumn(getColumn(tagv));
                }
            }
            sender.sendToWriter(record);
        }
    }

    static void dump4RDB(TSDBConnection conn, String metric, List<String> fields,
                         Map<String, String> tags, Long start, Long end,
                         List<String> columns4RDB, RecordSender sender, Map<String, Object> hint) throws Exception {
        LOG.info("conn address: {}, metric: {}, start: {}, end: {}", conn.address(), metric, start, end);

        String res = queryRange4MultiFields(conn, metric, fields, tags, start, end, hint);
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
        } else if (value instanceof Integer) {
            valueColumn = new LongColumn(((Integer)value).longValue());
        } else {
            throw new Exception(String.format("value not supported type: [%s]", value.getClass().getSimpleName()));
        }
        return valueColumn;
    }

    private static String queryRange4SingleField(TSDBConnection conn, String metric, Map<String, String> tags,
                                                 Long start, Long end, Map<String, Object> hint) throws Exception {
        String tagKV = getFilterByTags(tags);
        String body = "{\n" +
                "  \"start\": " + start + ",\n" +
                "  \"end\": " + end + ",\n" +
                "  \"queries\": [\n" +
                "    {\n" +
                "      \"aggregator\": \"none\",\n" +
                "      \"metric\": \"" + metric + "\"\n" +
                (tagKV == null ? "" : tagKV) +
                (hint == null ? "" : (", \"hint\": " + JSON.toJSONString(hint))) +
                "    }\n" +
                "  ]\n" +
                "}";
        return HttpUtils.post(conn.address() + QUERY, conn.username(), conn.password(), body);
    }

    private static String queryRange4MultiFields(TSDBConnection conn, String metric, List<String> fields,
                                                 Map<String, String> tags, Long start, Long end, Map<String, Object> hint) throws Exception {
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
                (hint == null ? "" : (", \"hint\": " + JSON.toJSONString(hint))) +
                "    }\n" +
                "  ]\n" +
                "}";
        return HttpUtils.post(conn.address() + QUERY_MULTI_FIELD, conn.username(), conn.password(), body);
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
