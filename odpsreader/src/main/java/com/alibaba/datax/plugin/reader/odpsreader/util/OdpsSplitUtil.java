package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.alibaba.datax.plugin.reader.odpsreader.Key;
import com.alibaba.datax.plugin.reader.odpsreader.OdpsReaderErrorCode;
import com.aliyun.odps.Odps;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class OdpsSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsSplitUtil.class);

    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OdpsSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalConfig, Odps odps,
                                              int adviceNum) {
        boolean isPartitionedTable = originalConfig.getBool(Constant.IS_PARTITIONED_TABLE);
        if (isPartitionedTable) {
            // 分区表
            return splitPartitionedTable(odps, originalConfig, adviceNum);
        } else {
            // 非分区表
            return splitForNonPartitionedTable(odps, adviceNum, originalConfig);
        }

    }

    private static List<Configuration> splitPartitionedTable(Odps odps, Configuration originalConfig,
                                                             int adviceNum) {
        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        List<String> partitions = originalConfig.getList(Key.PARTITION,
                String.class);

        if ((null == partitions || partitions.isEmpty()) && originalConfig.getBool(Key.SUCCESS_ON_NO_PATITION, false)) {
            Configuration tempConfig = originalConfig.clone();
            tempConfig.set(Key.PARTITION, null);
            splittedConfigs.add(tempConfig);
            LOG.warn(MESSAGE_SOURCE.message("odpssplitutil.4"));
            return splittedConfigs;
        }

        if (null == partitions || partitions.isEmpty()) {
            throw DataXException.asDataXException(OdpsReaderErrorCode.ILLEGAL_VALUE,
                    MESSAGE_SOURCE.message("odpssplitutil.1"));
        }

        //splitMode 默认为 record
        String splitMode = originalConfig.getString(Key.SPLIT_MODE);
        Configuration tempConfig = null;
        if (partitions.size() > adviceNum || Constant.PARTITION_SPLIT_MODE.equals(splitMode)) {
            // 此时不管 splitMode 是什么，都不需要再进行切分了
            // 注意：此处没有把 sessionId 设置到 config 中去，所以后续在 task 中获取 sessionId 时，需要针对这种情况重新创建 sessionId
            for (String onePartition : partitions) {
                tempConfig = originalConfig.clone();
                tempConfig.set(Key.PARTITION, onePartition);
                splittedConfigs.add(tempConfig);
            }

            return splittedConfigs;
        } else {
            // 还需要计算对每个分区，切分份数等信息
            int eachPartitionShouldSplittedNumber = calculateEachPartitionShouldSplittedNumber(
                    adviceNum, partitions.size());

            for (String onePartition : partitions) {
                List<Configuration> configs = splitOnePartition(odps,
                        onePartition, eachPartitionShouldSplittedNumber,
                        originalConfig);
                splittedConfigs.addAll(configs);
            }

            return splittedConfigs;
        }
    }

    private static int calculateEachPartitionShouldSplittedNumber(
            int adviceNumber, int partitionNumber) {
        double tempNum = 1.0 * adviceNumber / partitionNumber;

        return (int) Math.ceil(tempNum);
    }

    private static List<Configuration> splitForNonPartitionedTable(Odps odps,
                                                                   int adviceNum, Configuration sliceConfig) {
        List<Configuration> params = new ArrayList<Configuration>();

        String tunnelServer = sliceConfig.getString(Key.TUNNEL_SERVER);
        String tableName = sliceConfig.getString(Key.TABLE);

        String projectName = sliceConfig.getString(Key.PROJECT);

        DownloadSession session = OdpsUtil.createMasterSessionForNonPartitionedTable(odps,
                tunnelServer, projectName, tableName);

        String id = session.getId();
        long count = session.getRecordCount();

        List<Pair<Long, Long>> splitResult = splitRecordCount(count, adviceNum);

        for (Pair<Long, Long> pair : splitResult) {
            Configuration iParam = sliceConfig.clone();
            iParam.set(Constant.SESSION_ID, id);
            iParam.set(Constant.START_INDEX, pair.getLeft().longValue());
            iParam.set(Constant.STEP_COUNT, pair.getRight().longValue());

            params.add(iParam);
        }

        return params;
    }

    private static List<Configuration> splitOnePartition(Odps odps,
                                                         String onePartition, int adviceNum, Configuration sliceConfig) {
        List<Configuration> params = new ArrayList<Configuration>();

        String tunnelServer = sliceConfig.getString(Key.TUNNEL_SERVER);
        String tableName = sliceConfig.getString(Key.TABLE);

        String  projectName = sliceConfig.getString(Key.PROJECT);

        DownloadSession session = OdpsUtil.createMasterSessionForPartitionedTable(odps,
                tunnelServer, projectName, tableName, onePartition);

        String id = session.getId();
        long count = session.getRecordCount();

        List<Pair<Long, Long>> splitResult = splitRecordCount(count, adviceNum);

        for (Pair<Long, Long> pair : splitResult) {
            Configuration iParam = sliceConfig.clone();
            iParam.set(Key.PARTITION, onePartition);
            iParam.set(Constant.SESSION_ID, id);
            iParam.set(Constant.START_INDEX, pair.getLeft().longValue());
            iParam.set(Constant.STEP_COUNT, pair.getRight().longValue());

            params.add(iParam);
        }

        return params;
    }

    /**
     * Pair left: startIndex, right: stepCount
     */
    private static List<Pair<Long, Long>> splitRecordCount(long recordCount, int adviceNum) {
        if(recordCount<0){
            throw new IllegalArgumentException(MESSAGE_SOURCE.message("odpssplitutil.2", recordCount));
        }

        if(adviceNum<1){
            throw new IllegalArgumentException(MESSAGE_SOURCE.message("odpssplitutil.3", adviceNum));
        }

        List<Pair<Long, Long>> result = new ArrayList<Pair<Long, Long>>();
        // 为了适配 RangeSplitUtil 的处理逻辑，起始值从0开始计算
        if (recordCount == 0) {
            result.add(ImmutablePair.of(0L, 0L));
            return result;
        }

        long[] tempResult = RangeSplitUtil.doLongSplit(0L, recordCount - 1, adviceNum);

        tempResult[tempResult.length - 1]++;

        for (int i = 0; i < tempResult.length - 1; i++) {
            result.add(ImmutablePair.of(tempResult[i], (tempResult[i + 1] - tempResult[i])));
        }
        return result;
    }

}
