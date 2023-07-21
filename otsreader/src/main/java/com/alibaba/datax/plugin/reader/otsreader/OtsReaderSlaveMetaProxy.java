package com.alibaba.datax.plugin.reader.otsreader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.Constant;
import com.alibaba.datax.plugin.reader.otsreader.utils.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamCheckerOld;
import com.alibaba.datax.plugin.reader.otsreader.utils.ReaderModelParser;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.utils.DefaultNoRetry;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.fastjson.JSON;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.ListTableResult;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.ReservedThroughputDetails;
import com.aliyun.openservices.ots.model.TableMeta;

public class OtsReaderSlaveMetaProxy implements IOtsReaderSlaveProxy {

    private OTSClient ots = null;
    private OTSConf conf = null;
    private OTSRange range = null;
    private com.alicloud.openservices.tablestore.model.TableMeta meta = null;
    private Configuration configuration = null;
    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveMetaProxy.class);


    @Override
    public void init(Configuration configuration) {
        OTSServiceConfiguration configure = new OTSServiceConfiguration();
        configure.setRetryStrategy(new DefaultNoRetry());

        this.configuration = configuration;
        conf = GsonParser.jsonToConf((String) configuration.get(Constant.ConfigKey.CONF));
        range = GsonParser.jsonToRange((String) configuration.get(Constant.ConfigKey.RANGE));
        meta = GsonParser.jsonToMeta((String) configuration.get(Constant.ConfigKey.META));

        String endpoint = conf.getEndpoint();
        String accessId = conf.getAccessId();
        String accessKey = conf.getAccessKey();
        String instanceName = conf.getInstanceName();

        ots = new OTSClient(endpoint, accessId, accessKey, instanceName, null, configure, null);
    }

    @Override
    public void close() {
        ots.shutdown();
    }

    @Override
    public void startRead(RecordSender recordSender) throws Exception {
        List<OTSColumn> columns = ReaderModelParser
                .parseOTSColumnList(ParamCheckerOld.checkListAndGet(configuration, Key.COLUMN, true));
        String metaMode = conf.getMetaMode(); // column


        ListTableResult listTableResult = null;
        try {
            listTableResult = ots.listTable();
            LOG.info(String.format("ots listTable requestId:%s, traceId:%s", listTableResult.getRequestID(),
                    listTableResult.getTraceId()));
            List<String> allTables = listTableResult.getTableNames();
            for (String eachTable : allTables) {
                DescribeTableRequest describeTableRequest = new DescribeTableRequest();
                describeTableRequest.setTableName(eachTable);
                DescribeTableResult describeTableResult = ots.describeTable(describeTableRequest);
                LOG.info(String.format("ots describeTable requestId:%s, traceId:%s", describeTableResult.getRequestID(),
                        describeTableResult.getTraceId()));

                TableMeta tableMeta = describeTableResult.getTableMeta();
                // table_name: first_table
                // table primary key: type, data type: STRING
                // table primary key: db_name, data type: STRING
                // table primary key: table_name, data type: STRING
                // Reserved throughput: read(0), write(0)
                // last increase time: 1502881295
                // last decrease time: None
                // number of decreases today: 0

                String tableName = tableMeta.getTableName();
                Map<String, PrimaryKeyType> primaryKey = tableMeta.getPrimaryKey();
                ReservedThroughputDetails reservedThroughputDetails = describeTableResult
                        .getReservedThroughputDetails();
                int reservedThroughputRead = reservedThroughputDetails.getCapacityUnit().getReadCapacityUnit();
                int reservedThroughputWrite = reservedThroughputDetails.getCapacityUnit().getWriteCapacityUnit();
                long lastIncreaseTime = reservedThroughputDetails.getLastIncreaseTime();
                long lastDecreaseTime = reservedThroughputDetails.getLastDecreaseTime();
                int numberOfDecreasesToday = reservedThroughputDetails.getNumberOfDecreasesToday();

                Map<String, String> allData = new HashMap<String, String>();
                allData.put("endpoint", conf.getEndpoint());
                allData.put("instanceName", conf.getInstanceName());
                allData.put("table", tableName);
                // allData.put("primaryKey", JSON.toJSONString(primaryKey));
                allData.put("reservedThroughputRead", reservedThroughputRead + "");
                allData.put("reservedThroughputWrite", reservedThroughputWrite + "");
                allData.put("lastIncreaseTime", lastIncreaseTime + "");
                allData.put("lastDecreaseTime", lastDecreaseTime + "");
                allData.put("numberOfDecreasesToday", numberOfDecreasesToday + "");

                // 可扩展的可配置的形式
                if ("column".equalsIgnoreCase(metaMode)) {
                    // 如果是列元数据模式并且column中配置的name是primaryKey，映射成多行DataX Record
                    List<Record> primaryKeyRecords = new ArrayList<Record>();
                    for (Entry<String, PrimaryKeyType> eachPk : primaryKey.entrySet()) {
                        Record line = recordSender.createRecord();
                        for (OTSColumn col : columns) {
                            if (col.getColumnType() == OTSColumn.OTSColumnType.CONST) {
                                line.addColumn(col.getValue());
                            } else if ("primaryKey.name".equalsIgnoreCase(col.getName())) {
                                line.addColumn(new StringColumn(eachPk.getKey()));
                            } else if ("primaryKey.type".equalsIgnoreCase(col.getName())) {
                                line.addColumn(new StringColumn(eachPk.getValue().name()));
                            } else {
                                String v = allData.get(col.getName());
                                line.addColumn(new StringColumn(v));
                            }
                        }
                        LOG.debug("Reader send record : {}", line.toString());
                        recordSender.sendToWriter(line);
                        primaryKeyRecords.add(line);
                    }
                } else {
                    Record line = recordSender.createRecord();
                    for (OTSColumn col : columns) {
                        if (col.getColumnType() == OTSColumn.OTSColumnType.CONST) {
                            line.addColumn(col.getValue());
                        } else {
                            String v = allData.get(col.getName());
                            line.addColumn(new StringColumn(v));
                        }
                    }
                    LOG.debug("Reader send record : {}", line.toString());
                    recordSender.sendToWriter(line);
                }
            }
        } catch (Exception e) {
            LOG.warn(JSON.toJSONString(listTableResult), e);
        }

    }
}
