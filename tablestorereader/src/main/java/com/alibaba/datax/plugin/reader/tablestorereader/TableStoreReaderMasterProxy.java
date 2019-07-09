package com.alibaba.datax.plugin.reader.tablestorereader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tablestorereader.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConf;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConst;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreRange;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.*;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.SyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableStoreReaderMasterProxy {

    private TableStoreConf conf = new TableStoreConf();

    private TableStoreRange range = null;

    private SyncClient tableStoreClient = null;

    private TableMeta meta = null;

    private Direction direction = null;

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreReaderMasterProxy.class);

    /**
     * 1.检查参数是否为
     * null，endpoint,accessid,accesskey,instance-name,table,column,range-begin,range-end,range-split
     * 2.检查参数是否为空字符串
     * endpoint,accessid,accesskey,instance-name,table
     * 3.检查是否为空数组
     * column
     * 4.检查Range的类型个个数是否和PrimaryKey匹配
     * column,range-begin,range-end
     * 5.检查Range Split 顺序和类型是否Range一致，类型是否于PartitionKey一致
     * column-split
     *
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        // 默认参数
        // 每次重试的时间都是上一次的一倍，当sleep时间大于30秒时，Sleep重试时间不在增长。18次能覆盖OTS的Failover时间5分钟
        conf.setRetry(param.getInt(TableStoreConst.RETRY, 18));
        conf.setSleepInMilliSecond(param.getInt(TableStoreConst.SLEEP_IN_MILLI_SECOND, 100));

        // 必选参数
        conf.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT));
        conf.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID));
        conf.setAccesskey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY));
        conf.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME));
        conf.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME));

        tableStoreClient = new SyncClient(
                this.conf.getEndpoint(),
                this.conf.getAccessId(),
                this.conf.getAccesskey(),
                this.conf.getInstanceName());


        meta = getTableMeta(tableStoreClient, conf.getTableName());
        LOG.info("Table Meta : {}", GsonParser.metaToJson(meta));

        conf.setColumns(ReaderModelParser.parseOTSColumnList(ParamChecker.checkListAndGet(param, Key.COLUMN, true)));

//        Map<String, Object> rangeMap = ParamChecker.checkMapAndGet(param, Key.RANGE, true);
//        conf.setRangeBegin(ReaderodelParser.parsePrimaryKey(ParamChecker.checkListAndGet(rangeMap, Key.RANGE_BEGIN, false)));
//        conf.setRangeEnd(ReaderModelParser.parsePrimaryKey(ParamChecker.checkListAndGet(rangeMap, Key.RANGE_END, false)));

//        range = ParamChecker.checkRangeAndGet(meta, this.conf.getRangeBegin(), this.conf.getRangeEnd());

//        direction = ParamChecker.checkDirectionAndEnd(meta, range.getBegin(), range.getEnd());

        LOG.info("Direction : {}", direction);

//        List<PrimaryKeyValue> points = ReaderModelParser.parsePrimaryKey(ParamChecker.checkListAndGet(rangeMap, Key.RANGE_SPLIT));
//        ParamChecker.checkInputSplitPoints(meta, range, direction, points);
//        conf.setRangeSplit(points);
    }

    public List<Configuration> split(int num) {
        LOG.info("Expect split num : " + num);

        List<Configuration> configurations = new ArrayList<Configuration>();

        List<TableStoreRange> ranges = null;

        ranges.add(range);

        // 解决大量的Split Point序列化消耗内存的问题
        // 因为slave中不会使用这个配置，所以置为空
        this.conf.setRangeSplit(null);

        for (TableStoreRange item : ranges) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(TableStoreConst.OTS_CONF, GsonParser.confToJson(this.conf));
            configuration.set(TableStoreConst.OTS_RANGE, GsonParser.rangeToJson(item));
            configuration.set(TableStoreConst.OTS_DIRECTION, GsonParser.directionToJson(direction));
            configurations.add(configuration);
        }

        LOG.info("Configuration list count : " + configurations.size());

        return configurations;
    }

    public TableStoreConf getConf() {
        return conf;
    }

    public void close() {
        tableStoreClient.shutdown();
    }

    // private function

    private TableMeta getTableMeta(SyncClient ots, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                conf.getRetry(),
                conf.getSleepInMilliSecond()
        );
    }
//
//    private RowPrimaryKey getPKOfFirstRow(
//            TableStoreRange range, Direction direction) throws Exception {
//
//        RangeRowQueryCriteria cur = new RangeRowQueryCriteria(this.conf.getTableName());
//        cur.setInclusiveStartPrimaryKey(range.getBegin());
//        cur.setExclusiveEndPrimaryKey(range.getEnd());
//        cur.setLimit(1);
//        cur.setColumnsToGet(Common.getPrimaryKeyNameList(meta));
//        cur.setDirection(direction);
//
//        return RetryHelper.executeWithRetry(
//                new GetFirstRowPrimaryKeyCallable(ots, meta, cur),
//                conf.getRetry(),
//                conf.getSleepInMilliSecond()
//        );
//    }

//    private List<TableStoreRange> defaultRangeSplit(OTSClient ots, TableMeta meta, TableStoreRange range, int num) throws Exception {
//        if (num == 1) {
//            List<TableStoreRange> ranges = new ArrayList<TableStoreRange>();
//            ranges.add(range);
//            return ranges;
//        }
//
//        TableStoreRange reverseRange = new TableStoreRange();
//        reverseRange.setBegin(range.getEnd());
//        reverseRange.setEnd(range.getBegin());
//
//        Direction reverseDirection = (direction == Direction.FORWARD ? Direction.BACKWARD : Direction.FORWARD);
//
//        RowPrimaryKey realBegin = getPKOfFirstRow(range, direction);
//        RowPrimaryKey realEnd = getPKOfFirstRow(reverseRange, reverseDirection);
//
//        // 因为如果其中一行为空，表示这个范围内至多有一行数据
//        // 所以不再细分，直接使用用户定义的范围
//        if (realBegin == null || realEnd == null) {
//            List<TableStoreRange> ranges = new ArrayList<TableStoreRange>();
//            ranges.add(range);
//            return ranges;
//        }
//
//        // 如果出现realBegin，realEnd的方向和direction不一致的情况，直接返回range
//        int cmp = Common.compareRangeBeginAndEnd(meta, realBegin, realEnd);
//        Direction realDirection = cmp > 0 ? Direction.BACKWARD : Direction.FORWARD;
//        if (realDirection != direction) {
//            LOG.warn("Expect '" + direction + "', but direction of realBegin and readlEnd is '" + realDirection + "'");
//            List<TableStoreRange> ranges = new ArrayList<TableStoreRange>();
//            ranges.add(range);
//            return ranges;
//        }
//
//        List<TableStoreRange> ranges = RangeSplit.rangeSplitByCount(meta, realBegin, realEnd, num);
//
//        if (ranges.isEmpty()) { // 当PartitionKey相等时，工具内部不会切分Range
//            ranges.add(range);
//        } else {
//            // replace first and last
//            TableStoreRange first = ranges.get(0);
//            TableStoreRange last = ranges.get(ranges.size() - 1);
//
//            first.setBegin(range.getBegin());
//            last.setEnd(range.getEnd());
//        }
//
//        return ranges;
//    }

//    private List<TableStoreRange> userDefinedRangeSplit(TableMeta meta, TableStoreRange range, List<PrimaryKeyValue> points) {
//        List<TableStoreRange> ranges = RangeSplit.rangeSplitByPoint(meta, range.getBegin(), range.getEnd(), points);
//        if (ranges.isEmpty()) { // 当PartitionKey相等时，工具内部不会切分Range
//            ranges.add(range);
//        }
//        return ranges;
//    }
}
