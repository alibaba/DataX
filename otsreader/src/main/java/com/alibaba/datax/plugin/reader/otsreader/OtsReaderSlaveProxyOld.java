package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.utils.*;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetRangeCallableOld;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSServiceConfiguration;

public class OtsReaderSlaveProxyOld implements IOtsReaderSlaveProxy {


    private OTSClientAsync ots = null;
    private OTSConf conf = null;
    private OTSRange range = null;

    class RequestItem {
        private RangeRowQueryCriteria criteria;
        private OTSFuture<GetRangeResult> future;

        RequestItem(RangeRowQueryCriteria criteria, OTSFuture<GetRangeResult> future) {
            this.criteria = criteria;
            this.future = future;
        }

        public RangeRowQueryCriteria getCriteria() {
            return criteria;
        }

        public OTSFuture<GetRangeResult> getFuture() {
            return future;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderSlaveProxyOld.class);

    private void rowsToSender(List<Row> rows, RecordSender sender, List<OTSColumn> columns) {
        for (Row row : rows) {
            Record line = sender.createRecord();
            line = CommonOld.parseRowToLine(row, columns, line);

            LOG.debug("Reader send record : {}", line.toString());

            sender.sendToWriter(line);
        }
    }

    private RangeRowQueryCriteria generateRangeRowQueryCriteria(String tableName, RowPrimaryKey begin, RowPrimaryKey end, Direction direction, List<String> columns) {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setDirection(direction);
        criteria.setColumnsToGet(columns);
        criteria.setLimit(-1);
        criteria.setExclusiveEndPrimaryKey(end);
        return criteria;
    }

    private RequestItem generateRequestItem(
            OTSClientAsync ots,
            OTSConf conf,
            RowPrimaryKey begin,
            RowPrimaryKey end,
            Direction direction,
            List<String> columns) throws Exception {
        RangeRowQueryCriteria criteria = generateRangeRowQueryCriteria(conf.getTableName(), begin, end, direction, columns);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        OTSFuture<GetRangeResult> future =  ots.getRange(request);

        return new RequestItem(criteria, future);
    }

    @Override
    public void init(Configuration configuration) {
        conf = GsonParser.jsonToConf(configuration.getString(Constant.ConfigKey.CONF));
        range = GsonParser.jsonToRange(configuration.getString(Constant.ConfigKey.RANGE));

        OTSServiceConfiguration configure = new OTSServiceConfiguration();
        configure.setRetryStrategy(new DefaultNoRetry());

        ots = new OTSClientAsync(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                null,
                configure,
                null);
    }

    @Override
    public void close() {
        ots.shutdown();
    }

    @Override
    public void startRead(RecordSender recordSender) throws Exception {
        RowPrimaryKey token = pKColumnList2RowPrimaryKey(range.getBegin());

        List<String> columns = CommonOld.getNormalColumnNameList(conf.getColumn());
        Direction direction = null;
        switch (Common.getDirection(range.getBegin(), range.getEnd())){
            case FORWARD:
                direction = Direction.FORWARD;
                break;
            case BACKWARD:
            default:
                direction = Direction.BACKWARD;
        }
        RequestItem request = null;

        do {
            LOG.debug("Next token : {}", GsonParser.rowPrimaryKeyToJson(token));
            if (request == null) {
                request = generateRequestItem(ots, conf, token, pKColumnList2RowPrimaryKey(range.getEnd()), direction, columns);
            } else {
                RequestItem req = request;

                GetRangeResult result = RetryHelperOld.executeWithRetry(
                        new GetRangeCallableOld(ots, req.getCriteria(), req.getFuture()),
                        conf.getRetry(),
                        // TODO
                        100
                );
                if ((token = result.getNextStartPrimaryKey()) != null) {
                    request = generateRequestItem(ots, conf, token, pKColumnList2RowPrimaryKey(range.getEnd()), direction, columns);
                }

                rowsToSender(result.getRows(), recordSender, conf.getColumn());
            }
        } while (token != null);
    }

    /**
     * 将 {@link com.alicloud.openservices.tablestore.model.PrimaryKeyColumn}的列表转为{@link com.aliyun.openservices.ots.model.RowPrimaryKey}
     * @param list
     * @return
     */
    public RowPrimaryKey pKColumnList2RowPrimaryKey(List<PrimaryKeyColumn> list){
        RowPrimaryKey rowPrimaryKey = new RowPrimaryKey();
        for(PrimaryKeyColumn pk : list){
            PrimaryKeyValue v = null;
            if(pk.getValue() == com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MAX){
                v = PrimaryKeyValue.INF_MAX;
            } else if (pk.getValue() == com.alicloud.openservices.tablestore.model.PrimaryKeyValue.INF_MIN) {
                v = PrimaryKeyValue.INF_MIN;
            }
            // 非INF_MAX 或 INF_MIN
            else{
                switch (pk.getValue().getType()){
                    case STRING:
                        v = PrimaryKeyValue.fromString(pk.getValue().asString());
                        break;
                    case INTEGER:
                        v = PrimaryKeyValue.fromLong(pk.getValue().asLong());
                        break;
                    case BINARY:
                        v = PrimaryKeyValue.fromBinary(pk.getValue().asBinary());
                        break;
                    default:
                        throw new IllegalArgumentException("the pKColumnList to RowPrimaryKey conversion failed");
                }
            }

            rowPrimaryKey.addPrimaryKeyColumn(pk.getName(),v);
        }
        return rowPrimaryKey;
    }
}
