package com.alibaba.datax.plugin.reader.otsreader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetFirstRowPrimaryKeyCallable;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsreader.utils.ReaderModelParser;
import com.alibaba.datax.plugin.reader.otsreader.utils.RangeSplit;
import com.alibaba.datax.plugin.reader.otsreader.utils.RetryHelper;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.Direction;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

public class OtsReaderMasterProxy {

    private OTSConf conf = new OTSConf();

    private OTSRange range = null;

    private OTSClient ots = null;

    private TableMeta meta = null;

    private Direction direction = null;

    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderMasterProxy.class);

    /**
     * 1.检查参数是否为
     *     null，endpoint,accessid,accesskey,instance-name,table,column,range-begin,range-end,range-split
     * 2.检查参数是否为空字符串 
     *     endpoint,accessid,accesskey,instance-name,table
     * 3.检查是否为空数组
     *     column
     * 4.检查Range的类型个个数是否和PrimaryKey匹配
     *     column,range-begin,range-end
     * 5.检查Range Split 顺序和类型是否Range一致，类型是否于PartitionKey一致
     *     column-split
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {        
        // 默认参数
        // 每次重试的时间都是上一次的一倍，当sleep时间大于30秒时，Sleep重试时间不在增长。18次能覆盖OTS的Failover时间5分钟
        conf.setRetry(param.getInt(OTSConst.RETRY, 18));
        conf.setSleepInMilliSecond(param.getInt(OTSConst.SLEEP_IN_MILLI_SECOND, 100));
        
        // 必选参数
        conf.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT)); 
        conf.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID)); 
        conf.setAccesskey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY)); 
        conf.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME)); 
        conf.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME)); 
        
        ots = new OTSClient(
                this.conf.getEndpoint(),
                this.conf.getAccessId(),
                this.conf.getAccesskey(),
                this.conf.getInstanceName());

        meta = getTableMeta(ots, conf.getTableName());
        LOG.info("Table Meta : {}", GsonParser.metaToJson(meta));
        
        conf.setColumns(ReaderModelParser.parseOTSColumnList(ParamChecker.checkListAndGet(param, Key.COLUMN, true)));
        
        Map<String, Object> rangeMap = ParamChecker.checkMapAndGet(param, Key.RANGE, true);
        conf.setRangeBegin(ReaderModelParser.parsePrimaryKey(ParamChecker.checkListAndGet(rangeMap, Key.RANGE_BEGIN, false)));
        conf.setRangeEnd(ReaderModelParser.parsePrimaryKey(ParamChecker.checkListAndGet(rangeMap, Key.RANGE_END, false)));
        
        range = ParamChecker.checkRangeAndGet(meta, this.conf.getRangeBegin(), this.conf.getRangeEnd());
        
        direction = ParamChecker.checkDirectionAndEnd(meta, range.getBegin(), range.getEnd());
        LOG.info("Direction : {}", direction);

        List<PrimaryKeyValue> points = ReaderModelParser.parsePrimaryKey(ParamChecker.checkListAndGet(rangeMap, Key.RANGE_SPLIT));
        ParamChecker.checkInputSplitPoints(meta, range, direction, points);
        conf.setRangeSplit(points);
    }

    public List<Configuration> split(int num) throws Exception {
        LOG.info("Expect split num : " + num);
        
        List<Configuration> configurations = new ArrayList<Configuration>();

        List<OTSRange> ranges = null;

        if (this.conf.getRangeSplit() != null) { // 用户显示指定了拆分范围
            LOG.info("Begin userDefinedRangeSplit");
            ranges = userDefinedRangeSplit(meta, range, this.conf.getRangeSplit());
            LOG.info("End userDefinedRangeSplit");
        } else { // 采用默认的切分算法 
            LOG.info("Begin defaultRangeSplit");
            ranges = defaultRangeSplit(ots, meta, range, num);
            LOG.info("End defaultRangeSplit");
        }

        // 解决大量的Split Point序列化消耗内存的问题
        // 因为slave中不会使用这个配置，所以置为空
        this.conf.setRangeSplit(null);
        
        for (OTSRange item : ranges) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(this.conf));
            configuration.set(OTSConst.OTS_RANGE, GsonParser.rangeToJson(item));
            configuration.set(OTSConst.OTS_DIRECTION, GsonParser.directionToJson(direction));
            configurations.add(configuration);
        }
        
        LOG.info("Configuration list count : " + configurations.size());

        return configurations;
    }

    public OTSConf getConf() {
        return conf;
    }

    public void close() {
        ots.shutdown();
    }

    // private function

    private TableMeta getTableMeta(OTSClient ots, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                conf.getRetry(),
                conf.getSleepInMilliSecond()
                );
    }

    private RowPrimaryKey getPKOfFirstRow(
            OTSRange range , Direction direction) throws Exception {

        RangeRowQueryCriteria cur = new RangeRowQueryCriteria(this.conf.getTableName());
        cur.setInclusiveStartPrimaryKey(range.getBegin());
        cur.setExclusiveEndPrimaryKey(range.getEnd());
        cur.setLimit(1);
        cur.setColumnsToGet(Common.getPrimaryKeyNameList(meta));
        cur.setDirection(direction);

        return RetryHelper.executeWithRetry(
                new GetFirstRowPrimaryKeyCallable(ots, meta, cur),
                conf.getRetry(),
                conf.getSleepInMilliSecond()
                );
    }

    private List<OTSRange> defaultRangeSplit(OTSClient ots, TableMeta meta, OTSRange range, int num) throws Exception {
        if (num == 1) {
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            ranges.add(range);
            return ranges;
        }
        
        OTSRange reverseRange = new OTSRange();
        reverseRange.setBegin(range.getEnd());
        reverseRange.setEnd(range.getBegin());

        Direction reverseDirection = (direction == Direction.FORWARD ? Direction.BACKWARD : Direction.FORWARD);

        RowPrimaryKey realBegin = getPKOfFirstRow(range, direction);
        RowPrimaryKey realEnd   = getPKOfFirstRow(reverseRange, reverseDirection);
        
        // 因为如果其中一行为空，表示这个范围内至多有一行数据
        // 所以不再细分，直接使用用户定义的范围
        if (realBegin == null || realEnd == null) {
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            ranges.add(range);
            return ranges;
        }
        
        // 如果出现realBegin，realEnd的方向和direction不一致的情况，直接返回range
        int cmp = Common.compareRangeBeginAndEnd(meta, realBegin, realEnd);
        Direction realDirection = cmp > 0 ? Direction.BACKWARD : Direction.FORWARD;
        if (realDirection != direction) {
            LOG.warn("Expect '" + direction + "', but direction of realBegin and readlEnd is '" + realDirection + "'");
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            ranges.add(range);
            return ranges;
        }

        List<OTSRange> ranges = RangeSplit.rangeSplitByCount(meta, realBegin, realEnd, num);

        if (ranges.isEmpty()) { // 当PartitionKey相等时，工具内部不会切分Range
            ranges.add(range);
        } else {
            // replace first and last
            OTSRange first = ranges.get(0);
            OTSRange last = ranges.get(ranges.size() - 1);

            first.setBegin(range.getBegin());
            last.setEnd(range.getEnd());
        }
        
        return ranges;
    }

    private List<OTSRange> userDefinedRangeSplit(TableMeta meta, OTSRange range, List<PrimaryKeyValue> points) {
        List<OTSRange> ranges = RangeSplit.rangeSplitByPoint(meta, range.getBegin(), range.getEnd(), points);
        if (ranges.isEmpty()) { // 当PartitionKey相等时，工具内部不会切分Range
            ranges.add(range);
        }
        return ranges;
    }
}
