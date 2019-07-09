package com.alibaba.datax.plugin.reader.tablestorereader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreColumn;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConf;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConst;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreRange;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.Common;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.RetryHelper;
import com.alibaba.fastjson.JSON;
import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableStoreReaderSlaveProxy {

//    class RequestItem {
//        private RangeRowQueryCriteria criteria;
//        private OTSFuture<GetRangeResult> future;
//
//        RequestItem(RangeRowQueryCriteria criteria, OTSFuture<GetRangeResult> future) {
//            this.criteria = criteria;
//            this.future = future;
//        }
//
//        public RangeRowQueryCriteria getCriteria() {
//            return criteria;
//        }
//
//        public OTSFuture<GetRangeResult> getFuture() {
//            return future;
//        }
//    }

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreReaderSlaveProxy.class);

    private void rowsToSender(List<Row> rows, RecordSender sender, List<String> columns) {
        for (Row row : rows) {
            Record line = sender.createRecord();
            line = Common.parseRowToLine(row, columns, line);

            LOG.debug("Reader send record : {}", line.toString());

            sender.sendToWriter(line);
        }
    }

//    private RangeRowQueryCriteria generateRangeRowQueryCriteria(String tableName, RowPrimaryKey begin, RowPrimaryKey end, Direction direction, List<String> columns) {
//        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
//        criteria.setInclusiveStartPrimaryKey(begin);
//        criteria.setDirection(direction);
//        criteria.setColumnsToGet(columns);
//        criteria.setLimit(-1);
//        criteria.setExclusiveEndPrimaryKey(end);
//        return criteria;
//    }

//    private RequestItem generateRequestItem(
//            OTSClientAsync ots,
//            TableStoreConf conf,
//            RowPrimaryKey begin,
//            RowPrimaryKey end,
//            Direction direction,
//            List<String> columns) throws Exception {
//        RangeRowQueryCriteria criteria = generateRangeRowQueryCriteria(conf.getTableName(), begin, end, direction, columns);
//
//        GetRangeRequest request = new GetRangeRequest();
//        request.setRangeRowQueryCriteria(criteria);
//        OTSFuture<GetRangeResult> future = ots.getRange(request);
//
//        return new RequestItem(criteria, future);
//    }

//    public void read(RecordSender sender, Configuration configuration) throws Exception {
//        LOG.info("read begin.");
//
//        TableStoreConf conf = GsonParser.jsonToConf(configuration.getString(TableStoreConst.OTS_CONF));
//        TableStoreRange range = GsonParser.jsonToRange(configuration.getString(TableStoreConst.OTS_RANGE));
//        Direction direction = GsonParser.jsonToDirection(configuration.getString(TableStoreConst.OTS_DIRECTION));
//
//        //OTSServiceConfiguration configure = new OTSServiceConfiguration();
//        ClientConfiguration configure1 = new ClientConfiguration();
//        //configure.setRetryStrategy(new DefaultNoRetry());
//
//        OTSClientAsync ots = new OTSClientAsync(
//                conf.getEndpoint(),
//                conf.getAccessId(),
//                conf.getAccesskey(),
//                conf.getInstanceName(),
//                null,
//                configure,
//                null);
//
//        SyncClient syncClient = new SyncClient(
//                conf.getEndpoint(),
//                conf.getAccessId(),
//                conf.getAccesskey(),
//                conf.getInstanceName(),
//                configure1,
//                null,
//                null
//        );
//        RowPrimaryKey begin = range.getBegin();
//
//
////        RowPrimaryKey token = range.getBegin();
//
//
//        List<String> columns = Common.getNormalColumnNameList(conf.getColumns());
//
//        RequestItem request = null;
//
//        do {
//            LOG.debug("Next token : {}", GsonParser.rowPrimaryKeyToJson(token));
//            if (request == null) {
//                request = generateRequestItem(ots, conf, token, range.getEnd(), direction, columns);
//            } else {
//                RequestItem req = request;
//
//                GetRangeResult result = RetryHelper.executeWithRetry(
//                        new GetRangeCallable(ots, req.getCriteria(), req.getFuture()),
//                        conf.getRetry(),
//                        conf.getSleepInMilliSecond()
//                );
//                if ((token = result.getNextStartPrimaryKey()) != null) {
//                    request = generateRequestItem(ots, conf, token, range.getEnd(), direction, columns);
//                }
//
//                rowsToSender(result.getRows(), sender, conf.getColumns());
//            }
//        } while (token != null);
//
//        ots.shutdown();
//
//        LOG.info("read end.");
//
//        SearchQuery searchQuery = new SearchQuery();
//        searchQuery.setQuery(new MatchAllQuery());
//        searchQuery.setGetTotalCount(true);
//        SearchRequest searchRequest = new SearchRequest("", "", searchQuery);
//
//        SearchResponse resp = syncClient.search(searchRequest);
//
//        if (!resp.isAllSuccess()) {
//            throw new RuntimeException("not all success");
//        }
//        List<Row> rows = resp.getRows();
//        while (resp.getNextToken()!=null) { //读到NextToken为null为止，即读出全部数据
//            //把Token设置到下一次请求中
//            searchRequest.getSearchQuery().setToken(resp.getNextToken());
//            resp = syncClient.search(searchRequest);
//
//            if (!resp.isAllSuccess()) {
//                throw new RuntimeException("not all success");
//            }
//
//            rows.addAll(resp.getRows());
//        }
//
//        syncClient.shutdown();
//
//        LOG.info("RowSize: " + rows.size());
//        LOG.info("TotalCount: " + resp.getTotalCount());
//    }

    public void read(RecordSender sender, Configuration configuration) throws Exception {

        TableStoreConf conf = GsonParser.jsonToConf(configuration.getString(TableStoreConst.OTS_CONF));
        ClientConfiguration configure1 = new ClientConfiguration();

        SyncClient syncClient = new SyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccesskey(),
                conf.getInstanceName(),
                configure1,
                null,
                null
        );

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(conf.getTableName(), conf.getIndexName(), searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(conf.getColumnNames());
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = syncClient.search(searchRequest);

        if (!resp.isAllSuccess()) {
            throw new RuntimeException("not all success");
        }

        List<Row> rows = resp.getRows();

        LOG.info("read begin.");

        //读到NextToken为null为止，即读出全部数据
        while (resp.getNextToken() != null) {
            //把Token设置到下一次请求中
            searchRequest.getSearchQuery().setToken(resp.getNextToken());
            resp = syncClient.search(searchRequest);

            if (!resp.isAllSuccess()) {
                throw new RuntimeException("not all success");
            }

            rowsToSender(rows, sender, conf.getColumnNames());
        }

        syncClient.shutdown();

        LOG.info("read end.");
    }
}
