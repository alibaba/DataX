package com.alibaba.datax.plugin.reader.obhbasereader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.obhbasereader.Key;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class HbaseSplitUtil {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseSplitUtil.class);

    public static List<Configuration> split(Configuration configuration) {
        final List<Configuration> ranges = configuration.getListConfiguration(Key.RANGE);
        if (CollectionUtils.isEmpty(ranges)) {
            return Lists.newArrayList(configuration);
        }

        //TODO(yuez) 后续hbase api具备查询region的功能后，这里需要添加查询table region的逻辑，并且取table region和用户指定的range的交集
        List<Configuration> sliceConfs = new ArrayList<>(ranges.size());
        for (Configuration range : ranges) {
            byte[] startRowKey = convertUserRowkey(range, true);
            byte[] endRowKey = convertUserRowkey(range, false);
            if (startRowKey.length != 0 && endRowKey.length != 0 && Bytes.compareTo(startRowKey, endRowKey) > 0) {
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "The startRowkey in obhbasereader must not be greater than the endRowkey.");
            }
            Configuration sliceConf = configuration.clone();
            sliceConf.remove(Key.RANGE);
            String startKeyStr = Bytes.toStringBinary(startRowKey);
            String endRowKeyStr = Bytes.toStringBinary(endRowKey);
            sliceConf.set(Key.START_ROWKEY, startKeyStr);
            sliceConf.set(Key.END_ROWKEY, endRowKeyStr);
            sliceConfs.add(sliceConf);
        }
        return sliceConfs;
    }

    public static byte[] convertUserRowkey(Configuration configuration, boolean isStart) {
        String keyName = isStart ? Key.START_ROWKEY : Key.END_ROWKEY;
        String startRowkey = configuration.getString(keyName);
        if (StringUtils.isBlank(startRowkey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            boolean isBinaryRowkey = configuration.getBool(Key.IS_BINARY_ROWKEY, false);
            return stringToBytes(startRowkey, isBinaryRowkey);
        }
    }

    private static byte[] stringToBytes(String rowkey, boolean isBinaryRowkey) {
        if (isBinaryRowkey) {
            return Bytes.toBytesBinary(rowkey);
        } else {
            return Bytes.toBytes(rowkey);
        }
    }

    /**
     * 后续hbase api具备查询region的功能后才用得到此方法
     *
     * @param config
     * @param startRowkeyByte
     * @param endRowkeyByte
     * @param regionRanges
     * @return
     */
    private static List<Configuration> doSplit(Configuration config, byte[] startRowkeyByte, byte[] endRowkeyByte, Pair<byte[][], byte[][]> regionRanges) {

        List<Configuration> configurations = new ArrayList<Configuration>();

        for (int i = 0; i < regionRanges.getFirst().length; i++) {

            byte[] regionStartKey = regionRanges.getFirst()[i];
            byte[] regionEndKey = regionRanges.getSecond()[i];

            // 当前的region为最后一个region
            // 如果最后一个region的start Key大于用户指定的userEndKey,则最后一个region，应该不包含在内
            // 注意如果用户指定userEndKey为"",则此判断应该不成立。userEndKey为""表示取得最大的region
            if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0 && (endRowkeyByte.length != 0 && (Bytes.compareTo(regionStartKey, endRowkeyByte) > 0))) {
                continue;
            }

            // 如果当前的region不是最后一个region，
            // 用户配置的userStartKey大于等于region的endkey,则这个region不应该含在内
            if ((Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) != 0) && (Bytes.compareTo(startRowkeyByte, regionEndKey) >= 0)) {
                continue;
            }

            // 如果用户配置的userEndKey小于等于 region的startkey,则这个region不应该含在内
            // 注意如果用户指定的userEndKey为"",则次判断应该不成立。userEndKey为""表示取得最大的region
            if (endRowkeyByte.length != 0 && (Bytes.compareTo(endRowkeyByte, regionStartKey) <= 0)) {
                continue;
            }

            String thisStartKey = getStartKey(startRowkeyByte, regionStartKey);
            String thisEndKey = getEndKey(endRowkeyByte, regionEndKey);
            Configuration p = config.clone();
            p.set(Key.START_ROWKEY, thisStartKey);
            p.set(Key.END_ROWKEY, thisEndKey);
            LOG.debug("startRowkey:[{}], endRowkey:[{}] .", thisStartKey, thisEndKey);
            configurations.add(p);
        }

        return configurations;
    }

    private static String getEndKey(byte[] endRowkeyByte, byte[] regionEndKey) {
        if (endRowkeyByte == null) { // 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException("userEndKey should not be null!");
        }

        byte[] tempEndRowkeyByte;

        if (endRowkeyByte.length == 0) {
            tempEndRowkeyByte = regionEndKey;
        } else if (Bytes.compareTo(regionEndKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
            // 为最后一个region
            tempEndRowkeyByte = endRowkeyByte;
        } else {
            if (Bytes.compareTo(endRowkeyByte, regionEndKey) > 0) {
                tempEndRowkeyByte = regionEndKey;
            } else {
                tempEndRowkeyByte = endRowkeyByte;
            }
        }

        return Bytes.toStringBinary(tempEndRowkeyByte);
    }

    private static String getStartKey(byte[] startRowkeyByte, byte[] regionStarKey) {
        if (startRowkeyByte == null) { // 由于之前处理过，所以传入的userStartKey不可能为null
            throw new IllegalArgumentException("userStartKey should not be null!");
        }

        byte[] tempStartRowkeyByte;

        if (Bytes.compareTo(startRowkeyByte, regionStarKey) < 0) {
            tempStartRowkeyByte = regionStarKey;
        } else {
            tempStartRowkeyByte = startRowkeyByte;
        }

        return Bytes.toStringBinary(tempStartRowkeyByte);
    }
}
