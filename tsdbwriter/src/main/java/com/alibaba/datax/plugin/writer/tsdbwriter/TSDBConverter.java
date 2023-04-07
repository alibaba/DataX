package com.alibaba.datax.plugin.writer.tsdbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSON;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TSDBConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TSDBConverter.class);

    private List<String> columnName;
    private List<String> columnType;

    TSDBConverter(List<String> columnName, List<String> columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        LOG.info("columnName: {}, columnType: {}", JSON.toJSONString(columnName), JSON.toJSONString(columnType));
    }

    List<Point> transRecord2Point(List<Record> records) {
        List<Point> dps = new ArrayList<Point>();
        for (Record record : records) {
            List<Point.MetricBuilder> metricBuilders = new ArrayList<Point.MetricBuilder>();
            Map<String, String> tags = new HashMap<String, String>();
            Long time = 0L;

            for (int i = 0; i < columnType.size(); i++) {
                String type = columnType.get(i);
                String name = columnName.get(i);
                Column column = record.getColumn(i);
                if (TSDBModel.TSDB_TAG.equals(type)) {
                    tags.put(name, column.asString());
                } else if (TSDBModel.TSDB_FIELD_DOUBLE.equals(type)) {
                    metricBuilders.add(new Point.MetricBuilder(name).value(column.asDouble()));
                } else if (TSDBModel.TSDB_FIELD_STRING.equals(type)) {
                    metricBuilders.add(new Point.MetricBuilder(name).value(column.asString()));
                } else if (TSDBModel.TSDB_FIELD_BOOL.equals(type)) {
                    metricBuilders.add(new Point.MetricBuilder(name).value(column.asBoolean()));
                } else if (TSDBModel.TSDB_TIMESTAMP.equals(type)) {
                    time = column.asLong();
                } else if (TSDBModel.TSDB_METRIC_NUM.equals(type)) {
                    // compatible with previous usage of TSDB_METRIC_NUM
                    metricBuilders.add(new Point.MetricBuilder(name).value(column.asDouble()));
                } else if (TSDBModel.TSDB_METRIC_STRING.equals(type)) {
                    // compatible with previous usage of TSDB_METRIC_STRING
                    metricBuilders.add(new Point.MetricBuilder(name).value(column.asString()));
                }
            }
            for (Point.MetricBuilder metricBuilder : metricBuilders) {
                dps.add(metricBuilder.tag(tags).timestamp(time).build(false));
            }
        }
        return dps;
    }

    List<MultiFieldPoint> transRecord2MultiFieldPoint(List<Record> records, String tableName) {
        List<MultiFieldPoint> dps = new ArrayList<MultiFieldPoint>();
        for (Record record : records) {
            MultiFieldPoint.MetricBuilder builder = MultiFieldPoint.metric(tableName);
            for (int i = 0; i < columnType.size(); i++) {
                String type = columnType.get(i);
                String name = columnName.get(i);
                Column column = record.getColumn(i);
                if (TSDBModel.TSDB_TAG.equals(type)) {
                    builder.tag(name, column.asString());
                } else if (TSDBModel.TSDB_FIELD_DOUBLE.equals(type)) {
                    builder.field(name, column.asDouble());
                } else if (TSDBModel.TSDB_FIELD_STRING.equals(type)) {
                    builder.field(name, column.asString());
                } else if (TSDBModel.TSDB_FIELD_BOOL.equals(type)) {
                    builder.field(name, column.asBoolean());
                } else if (TSDBModel.TSDB_TIMESTAMP.equals(type)) {
                    builder.timestamp(column.asLong());
                } else if (TSDBModel.TSDB_METRIC_NUM.equals(type)) {
                    // compatible with previous usage of TSDB_METRIC_NUM
                    builder.field(name, column.asDouble());
                } else if (TSDBModel.TSDB_METRIC_STRING.equals(type)) {
                    // compatible with previous usage of TSDB_METRIC_STRING
                    builder.field(name, column.asString());
                }
            }
            MultiFieldPoint point = builder.build(false);
            dps.add(point);
        }
        return dps;
    }
}
