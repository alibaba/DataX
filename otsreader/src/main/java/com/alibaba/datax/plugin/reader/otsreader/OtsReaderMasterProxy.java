package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetFirstRowPrimaryKeyCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.*;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesScanSplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class OtsReaderMasterProxy implements IOtsReaderMasterProxy {

    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderMasterProxy.class);
    private OTSConf conf = null;
    private TableMeta meta = null;
    private SyncClientInterface ots = null;
    private Direction direction = null;


    public OTSConf getConf() {
        return conf;
    }

    public TableMeta getMeta() {
        return meta;
    }

    public SyncClientInterface getOts() {
        return ots;
    }

    public void setOts(SyncClientInterface ots) {
        this.ots = ots;
    }

    /**
     * 基于配置传入的配置文件，解析为对应的参数
     *
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        // 基于预定义的Json格式,检查传入参数是否符合Conf定义规范
        conf = OTSConf.load(param);

        // Init ots
        ots = OtsHelper.getOTSInstance(conf);

        // 宽行表init
        if (!conf.isTimeseriesTable()) {
            // 获取TableMeta
            meta = OtsHelper.getTableMeta(
                    ots,
                    conf.getTableName(),
                    conf.getRetry(),
                    conf.getRetryPauseInMillisecond());

            // 基于Meta检查Conf是否正确
            ParamChecker.checkAndSetOTSConf(conf, meta);
            direction = ParamChecker.checkDirectionAndEnd(meta, conf.getRange().getBegin(), conf.getRange().getEnd());
        }
        // 时序表 检查tablestore SDK version
        if (conf.isTimeseriesTable()){
            Common.checkTableStoreSDKVersion();
        }


    }
    
    public List<Configuration> split(int mandatoryNumber) throws Exception {
        LOG.info("Expect split num : " + mandatoryNumber);

        List<Configuration> configurations = new ArrayList<Configuration>();

        if (conf.isTimeseriesTable()) {{    // 时序表全部采用默认切分策略
            LOG.info("Begin timeseries table defaultRangeSplit");
            configurations = getTimeseriesConfigurationBySplit(mandatoryNumber);
            LOG.info("End timeseries table defaultRangeSplit");
        }}
        else if (this.conf.getRange().getSplit().size() != 0) { // 用户显示指定了拆分范围
            LOG.info("Begin userDefinedRangeSplit");
            configurations = getNormalConfigurationBySplit();
            LOG.info("End userDefinedRangeSplit");
        } else { // 采用默认的切分算法
            LOG.info("Begin defaultRangeSplit");
            configurations = getDefaultConfiguration(mandatoryNumber);
            LOG.info("End defaultRangeSplit");
        }

        LOG.info("Expect split num: "+ mandatoryNumber +", and final configuration list count : " + configurations.size());
        return configurations;
    }

    public void close() {
        ots.shutdown();
    }

    /**
     * timeseries split信息，根据切分数配置多个Task
     */
    private List<Configuration> getTimeseriesConfigurationBySplit(int mandatoryNumber) throws Exception {
        List<TimeseriesScanSplitInfo> timeseriesScanSplitInfoList = OtsHelper.splitTimeseriesScan(
                ots,
                conf.getTableName(),
                conf.getMeasurementName(),
                mandatoryNumber,
                conf.getRetry(),
                conf.getRetryPauseInMillisecond());
        List<Configuration> configurations = new ArrayList<>();

        for (int i = 0; i < timeseriesScanSplitInfoList.size(); i++) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(Constant.ConfigKey.CONF, GsonParser.confToJson(conf));
            configuration.set(Constant.ConfigKey.SPLIT_INFO, GsonParser.timeseriesScanSplitInfoToString(timeseriesScanSplitInfoList.get(i)));
            configurations.add(configuration);
        }
        return configurations;
    }

    /**
     * 根据用户配置的split信息，将配置文件基于Range范围转换为多个Task的配置
     */
    private List<Configuration> getNormalConfigurationBySplit() {
        List<List<PrimaryKeyColumn>> primaryKeys = new ArrayList<List<PrimaryKeyColumn>>();
        primaryKeys.add(conf.getRange().getBegin());
        for (PrimaryKeyColumn column : conf.getRange().getSplit()) {
            List<PrimaryKeyColumn> point = new ArrayList<PrimaryKeyColumn>();
            point.add(column);
            ParamChecker.fillPrimaryKey(this.meta.getPrimaryKeyList(), point, PrimaryKeyValue.INF_MIN);
            primaryKeys.add(point);
        }
        primaryKeys.add(conf.getRange().getEnd());

        List<Configuration> configurations = new ArrayList<Configuration>(primaryKeys.size() - 1);

        for (int i = 0; i < primaryKeys.size() - 1; i++) {
            OTSRange range = new OTSRange();
            range.setBegin(primaryKeys.get(i));
            range.setEnd(primaryKeys.get(i + 1));

            Configuration configuration = Configuration.newDefault();
            configuration.set(Constant.ConfigKey.CONF, GsonParser.confToJson(conf));
            configuration.set(Constant.ConfigKey.RANGE, GsonParser.rangeToJson(range));
            configuration.set(Constant.ConfigKey.META, GsonParser.metaToJson(meta));
            configurations.add(configuration);
        }
        return configurations;
    }

    private List<Configuration> getDefaultConfiguration(int num) throws Exception {
        if (num == 1) {
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            OTSRange range = new OTSRange();
            range.setBegin(conf.getRange().getBegin());
            range.setEnd(conf.getRange().getEnd());
            ranges.add(range);

            return getConfigurationsFromRanges(ranges);
        }

        OTSRange reverseRange = new OTSRange();
        reverseRange.setBegin(conf.getRange().getEnd());
        reverseRange.setEnd(conf.getRange().getBegin());

        Direction reverseDirection = (direction == Direction.FORWARD ? Direction.BACKWARD : Direction.FORWARD);

        List<PrimaryKeyColumn> realBegin = getPKOfFirstRow(conf.getRange(), direction);
        List<PrimaryKeyColumn> realEnd   = getPKOfFirstRow(reverseRange, reverseDirection);

        // 因为如果其中一行为空，表示这个范围内至多有一行数据
        // 所以不再细分，直接使用用户定义的范围
        if (realBegin == null || realEnd == null) {
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            ranges.add(conf.getRange());
            return getConfigurationsFromRanges(ranges);
        }

        // 如果出现realBegin，realEnd的方向和direction不一致的情况，直接返回range
        int cmp = Common.compareRangeBeginAndEnd(meta, realBegin, realEnd);
        Direction realDirection = cmp > 0 ? Direction.BACKWARD : Direction.FORWARD;
        if (realDirection != direction) {
            LOG.warn("Expect '" + direction + "', but direction of realBegin and readlEnd is '" + realDirection + "'");
            List<OTSRange> ranges = new ArrayList<OTSRange>();
            ranges.add(conf.getRange());
            return getConfigurationsFromRanges(ranges);
        }

        List<OTSRange> ranges = RangeSplit.rangeSplitByCount(meta, realBegin, realEnd, num);

        if (ranges.isEmpty()) { // 当PartitionKey相等时，工具内部不会切分Range
            ranges.add(conf.getRange());
        } else {
            // replace first and last
            OTSRange first = ranges.get(0);
            OTSRange last = ranges.get(ranges.size() - 1);

            first.setBegin(conf.getRange().getBegin());
            last.setEnd(conf.getRange().getEnd());
        }

        return getConfigurationsFromRanges(ranges);
    }

    private List<Configuration> getConfigurationsFromRanges(List<OTSRange> ranges){
        List<Configuration> configurationList = new ArrayList<>();
        for (OTSRange range:ranges
             ) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(Constant.ConfigKey.CONF, GsonParser.confToJson(conf));
            configuration.set(Constant.ConfigKey.RANGE, GsonParser.rangeToJson(range));
            configuration.set(Constant.ConfigKey.META, GsonParser.metaToJson(meta));
            configurationList.add(configuration);
        }
        return configurationList;
    }

    private List<PrimaryKeyColumn> getPKOfFirstRow(
            OTSRange range , Direction direction) throws Exception {

        RangeRowQueryCriteria cur = new RangeRowQueryCriteria(this.conf.getTableName());
        cur.setInclusiveStartPrimaryKey(new PrimaryKey(range.getBegin()));
        cur.setExclusiveEndPrimaryKey(new PrimaryKey(range.getEnd()));
        cur.setLimit(1);
        cur.addColumnsToGet(Common.getPrimaryKeyNameList(meta));
        cur.setDirection(direction);
        cur.setMaxVersions(1);

        return RetryHelper.executeWithRetry(
                new GetFirstRowPrimaryKeyCallable(ots, meta, cur),
                conf.getRetry(),
                conf.getRetryPauseInMillisecond()
        );
    }

}
