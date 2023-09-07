package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.*;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesScanSplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OtsReaderSlaveNormalProxy implements IOtsReaderSlaveProxy {
    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveNormalProxy.class);
    private OTSConf conf = null;
    private OTSRange range = null;
    private TableMeta meta = null;
    private SyncClientInterface ots = null;
    private TimeseriesScanSplitInfo splitInfo = null;

    @Override
    public void init(Configuration configuration) {
        conf = GsonParser.jsonToConf((String) configuration.get(Constant.ConfigKey.CONF));
        if (!conf.isTimeseriesTable()) {
            range = GsonParser.jsonToRange((String) configuration.get(Constant.ConfigKey.RANGE));
            meta = GsonParser.jsonToMeta((String) configuration.get(Constant.ConfigKey.META));
        } else {
            splitInfo = GsonParser.stringToTimeseriesScanSplitInfo((String) configuration.get(Constant.ConfigKey.SPLIT_INFO));
            // 时序表 检查tablestore SDK version
            try{
                Common.checkTableStoreSDKVersion();
            }
            catch (Exception e){
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
        }


        this.ots = OtsHelper.getOTSInstance(conf);
    }

    @Override
    public void close() {
        ots.shutdown();
    }

    private void sendToDatax(RecordSender recordSender, Row row) {
        Record line = recordSender.createRecord();

        PrimaryKey pk = row.getPrimaryKey();
        for (OTSColumn column : conf.getColumn()) {
            if (column.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                // 获取指定的列
                PrimaryKeyColumn value = pk.getPrimaryKeyColumn(column.getName());
                if (value != null) {
                    line.addColumn(TranformHelper.otsPrimaryKeyColumnToDataxColumn(value));
                } else {
                    Column c = row.getLatestColumn(column.getName());
                    if (c != null) {
                        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
                    } else {
                        // 这里使用StringColumn的无参构造函数构造对象，而不是用null，下
                        // 游（writer）应该通过获取Column，然后通过Column的数据接口的返回值
                        // 是否是null来判断改Column是否为null
                        // Datax其他插件的也是使用这种方式，约定俗成，并没有使用直接向record中注入null方式代表空
                        line.addColumn(new StringColumn());
                    }
                }
            } else {
                line.addColumn(column.getValue());
            }
        }
        recordSender.sendToWriter(line);
    }

    private void sendToDatax(RecordSender recordSender, TimeseriesRow row) {


        Record line = recordSender.createRecord();
        // 对于配置项中的每一列
        for (int i = 0; i < conf.getColumn().size(); i++) {
            OTSColumn column = conf.getColumn().get(i);
            // 如果不是常数列
            if (column.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                // 如果是tags内字段
                if (conf.getColumn().get(i).getTimeseriesTag()) {
                    String s = row.getTimeseriesKey().getTags().get(column.getName());
                    line.addColumn(new StringColumn(s));
                }
                // 如果为measurement字段
                else if (column.getName().equals(Constant.ConfigKey.TimeseriesPKColumn.MEASUREMENT_NAME)) {
                    String s = row.getTimeseriesKey().getMeasurementName();
                    line.addColumn(new StringColumn(s));
                }
                // 如果为dataSource字段
                else if (column.getName().equals(Constant.ConfigKey.TimeseriesPKColumn.DATA_SOURCE)) {
                    String s = row.getTimeseriesKey().getDataSource();
                    line.addColumn(new StringColumn(s));
                }
                // 如果为tags字段
                else if (column.getName().equals(Constant.ConfigKey.TimeseriesPKColumn.TAGS)) {
                    line.addColumn(new StringColumn(row.getTimeseriesKey().buildTagsString()));
                }
                else if (column.getName().equals(Constant.ConfigKey.TimeseriesPKColumn.TIME)) {
                    Long l = row.getTimeInUs();
                    line.addColumn(new LongColumn(l));
                }
                // 否则为field内字段
                else {
                    ColumnValue c = row.getFields().get(column.getName());
                    if (c == null) {
                        LOG.warn("Get column {} : type {} failed, use empty string instead", column.getName(), conf.getColumn().get(i).getValueType());
                        line.addColumn(new StringColumn());
                    } else if (c.getType() != conf.getColumn().get(i).getValueType()) {
                        LOG.warn("Get column {} failed, expected type: {}, actual type: {}. Sending actual type to writer.", column.getName(), conf.getColumn().get(i).getValueType(), c.getType());
                        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
                    } else {
                        line.addColumn(TranformHelper.otsColumnToDataxColumn(c));
                    }
                }
            }
            // 如果是常数列
            else {
                line.addColumn(column.getValue());
            }
        }
        recordSender.sendToWriter(line);
    }

    /**
     * 将获取到的数据根据用户配置Column的方式传递给datax
     *
     * @param recordSender
     * @param result
     */
    private void sendToDatax(RecordSender recordSender, GetRangeResponse result) {
        for (Row row : result.getRows()) {
            sendToDatax(recordSender, row);
        }
    }

    private void sendToDatax(RecordSender recordSender, ScanTimeseriesDataResponse result) {
        for (TimeseriesRow row : result.getRows()) {
            sendToDatax(recordSender, row);
        }
    }

    @Override
    public void startRead(RecordSender recordSender) throws Exception {
        if (conf.isTimeseriesTable()) {
            readTimeseriesTable(recordSender);
        } else {
            readNormalTable(recordSender);
        }
    }

    public void readTimeseriesTable(RecordSender recordSender) throws Exception {

        List<String> timeseriesPkName = new ArrayList<>();
        timeseriesPkName.add(Constant.ConfigKey.TimeseriesPKColumn.MEASUREMENT_NAME);
        timeseriesPkName.add(Constant.ConfigKey.TimeseriesPKColumn.DATA_SOURCE);
        timeseriesPkName.add(Constant.ConfigKey.TimeseriesPKColumn.TAGS);
        timeseriesPkName.add(Constant.ConfigKey.TimeseriesPKColumn.TIME);

        ScanTimeseriesDataRequest scanTimeseriesDataRequest = new ScanTimeseriesDataRequest(conf.getTableName());
        List<Pair<String, ColumnType>> fieldsToGet = new ArrayList<>();
        for (int i = 0; i < conf.getColumn().size(); i++) {
            /**
             * 如果所配置列
             * 1. 不是常量列（即列名不为null）
             * 2. 列名不在["measurementName","dataSource","tags"]中
             * 3. 不是tags内的字段
             * 则为需要获取的field字段。
             */
            String fieldName = conf.getColumn().get(i).getName();
            if (fieldName != null && !timeseriesPkName.contains(fieldName) && !conf.getColumn().get(i).getTimeseriesTag()) {
                Pair<String, ColumnType> pair = new Pair<>(fieldName, conf.getColumn().get(i).getValueType());
                fieldsToGet.add(pair);
            }
        }
        scanTimeseriesDataRequest.setFieldsToGet(fieldsToGet);
        scanTimeseriesDataRequest.setSplitInfo(splitInfo);

        while (true) {
            ScanTimeseriesDataResponse response = OtsHelper.scanTimeseriesData(
                    ots,
                    scanTimeseriesDataRequest,
                    conf.getRetry(),
                    conf.getRetryPauseInMillisecond());
            sendToDatax(recordSender, response);
            if (response.getNextToken() == null) {
                break;
            }
            scanTimeseriesDataRequest.setNextToken(response.getNextToken());
        }
    }

    public void readNormalTable(RecordSender recordSender) throws Exception {
        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(range.getBegin());
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(range.getEnd());
        PrimaryKey next = inclusiveStartPrimaryKey;

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(conf.getTableName());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setDirection(Common.getDirection(range.getBegin(), range.getEnd()));
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.addColumnsToGet(Common.toColumnToGet(conf.getColumn(), meta));

        do {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(next);
            GetRangeResponse result = OtsHelper.getRange(
                    ots,
                    rangeRowQueryCriteria,
                    conf.getRetry(),
                    conf.getRetryPauseInMillisecond());
            sendToDatax(recordSender, result);
            next = result.getNextStartPrimaryKey();
        } while (next != null);
    }


    public void setConf(OTSConf conf) {
        this.conf = conf;
    }


    public void setRange(OTSRange range) {
        this.range = range;
    }


    public void setMeta(TableMeta meta) {
        this.meta = meta;
    }


    public void setOts(SyncClientInterface ots) {
        this.ots = ots;
    }
}
