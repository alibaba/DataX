package com.alibaba.datax.plugin.unstructuredstorage.reader.split;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: guxuan
 * @Date 2022-05-17 15:49
 */
public abstract class UnstructuredSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(UnstructuredSplitUtil.class);
    private boolean needInnerSplit;
    // 对每个文件进行切分的块大小是 64MB
    // warn: 这个最好弄成可配置的， 用户配置channel为2但是有10个文件，不一定需要文件内部切分；
    // 弄成可配置的有些情况下可以避免文件内部切分切分的task太碎
    private static final Long BLOCK_BYTE_CAPACITY = 64 * FileUtils.ONE_MB;

    public UnstructuredSplitUtil(boolean needInnerSplit) {
        this.needInnerSplit = needInnerSplit;
    }

    public List<Configuration> getSplitConfiguration(Configuration originConfiguration, List<String> sourceObjectList,
                                                     int adviceNumber) {

        List<Configuration> splitConfiguration = new ArrayList<Configuration>();
        List<StartEndPair> regulateSplitStartEndPairList = new ArrayList<StartEndPair>();

        for (String object : sourceObjectList) {
            boolean realNeedInnerSplit = false;
            Long contentTotalLength = -1L;
            if (this.needInnerSplit) {
                // 减少不必要的oss接口调用
                contentTotalLength = this.getFileTotalLength(object);
                if (isNeedSplit(contentTotalLength)) {
                    realNeedInnerSplit = true;
                }
            }
            // warn: 数据读模式允许文件内部切分，并且文件大小满足
            if (realNeedInnerSplit) {
                List<StartEndPair> startEndPairList = getSplitStartEndPairList(contentTotalLength, object);
                List<Triple<Long, Long, InputStream>> startEndInputStreamTripleList = new ArrayList<Triple<Long, Long, InputStream>>();
                for (int i = 0; i < startEndPairList.size(); i++) {
                    StartEndPair startEndPair = startEndPairList.get(i);
                    InputStream inputStream = this.getFileInputStream(startEndPair);
                    Triple<Long, Long, InputStream> startEndInputStreamTriple = new ImmutableTriple<Long, Long, InputStream>(
                            startEndPair.getStart(), startEndPair.getEnd(), inputStream);
                    startEndInputStreamTripleList.add(startEndInputStreamTriple);
                }
                regulateSplitStartEndPairList.addAll(regulateSplitStartEndPair(startEndInputStreamTripleList, object));
            } else {
                // 如果指定的Range无效(比如开始位置、结束位置为负数，大于文件大小)，则会下载整个文件；
                StartEndPair startEndPair = new StartEndPair(0L, -1L, object);
                regulateSplitStartEndPairList.add(startEndPair);
            }
        }

        // merge task 将多个文件merge到一个task中执行
        List<List<StartEndPair>> splitResult = RangeSplitUtil.doListSplit(regulateSplitStartEndPairList, adviceNumber);
        // at here this.objects is not null and not empty
        for (List<StartEndPair> eachSlice : splitResult) {
            Configuration splitedConfig = originConfiguration.clone();
            splitedConfig.set(Key.SPLIT_SLICE_CONFIG, eachSlice);
            splitConfiguration.add(splitedConfig);
            LOG.info(String.format("File to be read:%s", JSON.toJSONString(eachSlice)));
        }
        return splitConfiguration;
    }

    /**
     * 对原始的切分点位进行调节校准, 将点位落在每一行数据的换行符处
     *
     * @param startEndInputStreamTripleList
     *            原始的切分点位及inputstream (start, end, inputStream)
     * @return
     */
    private List<StartEndPair> regulateSplitStartEndPair(
            List<Triple<Long, Long, InputStream>> startEndInputStreamTripleList, String filePath) {
        List<StartEndPair> regulatedStartEndPairList = new ArrayList<StartEndPair>();

        for (int i = 0; i < startEndInputStreamTripleList.size(); i++) {
            if (i == 0) {
                Triple<Long, Long, InputStream> firstBlock = startEndInputStreamTripleList.get(i);
                StartEndPair startEndPair = new StartEndPair(firstBlock.getLeft(), null, filePath);
                regulatedStartEndPairList.add(startEndPair);
                continue;
            }
            Triple<Long, Long, InputStream> block = startEndInputStreamTripleList.get(i);
            long start = block.getLeft();
            long offset = 0;
            // 对切分点位进行调节,将切分起始点移动到行尾(即'\n'上)
            if (i < startEndInputStreamTripleList.size()) {
                offset = getLFIndex(block.getRight());
            }
            // 调节正确的切分点位
            long regulatedPoint = start + offset;
            // 将上一个block的末尾点位调节成行尾
            regulatedStartEndPairList.get(i - 1).setEnd(regulatedPoint);
            if (i < startEndInputStreamTripleList.size() - 1) {
                // 将本block起始点位进行调节, 结束点位暂不调节
                regulatedStartEndPairList.add(new StartEndPair(regulatedPoint + 1, null, filePath));
            } else {
                // 调节最后一个block, 调节起始点位, 结束点位就用文件的字节总长度
                regulatedStartEndPairList.add(new StartEndPair(regulatedPoint + 1, block.getMiddle(), filePath));
            }
        }
        return regulatedStartEndPairList;
    }

    /**
     * 获取到输入流开始的第一个'\n'偏移量, 如果向后偏移了ByteCapacity个字节,还是没有找到'\n'的话,则抛出异常 注:
     * 对文件切分的最后一个分块不会调用该方法
     *
     * @param inputStream
     *            输入流
     * @return
     */
    private Long getLFIndex(InputStream inputStream) {
        Long hasReadByteIndex = -1L;
        int ch = 0;
        while (ch != -1) {
            try {
                ch = inputStream.read();
            } catch (IOException e) {
                throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR,
                        String.format("inputstream read Byte has exception: %s", e.getMessage()), e);
            }
            hasReadByteIndex++;
            if (ch == '\n') {
                return hasReadByteIndex;
            }
        }
        return hasReadByteIndex;
    }

    /**
     * 得到一个文件最多能拆分成的份数
     *
     * @param fileTotalLength
     * @return
     */
    private List<StartEndPair> getSplitStartEndPairList(Long fileTotalLength, String filePath) {
        long splitNum = (long) Math.ceil(fileTotalLength * 1.0 / BLOCK_BYTE_CAPACITY);
        List<StartEndPair> startEndPairList = new ArrayList<StartEndPair>();
        long start, end;
        for (int i = 1; i <= splitNum; i++) {
            if (i == 1) {
                start = (i - 1) * BLOCK_BYTE_CAPACITY;
                end = i * BLOCK_BYTE_CAPACITY;
            } else if (i < splitNum) {
                start = (i - 1) * BLOCK_BYTE_CAPACITY + 1;
                end = i * BLOCK_BYTE_CAPACITY;
            } else {
                start = (i - 1) * BLOCK_BYTE_CAPACITY + 1;
                end = fileTotalLength - 1;
            }
            StartEndPair startEndPair = new StartEndPair(start, end, filePath);
            startEndPairList.add(startEndPair);
        }
        return startEndPairList;
    }

    /**
     * 判断文件是否需要切分, 切分的条件是必须要大于 transport.channel.byteCapacity
     *
     * @param fileTotalLength:
     *            文件总字节数
     * @return
     */
    private boolean isNeedSplit(Long fileTotalLength) {
        boolean fileSizeCouldSplit = fileTotalLength > BLOCK_BYTE_CAPACITY ? true : false;
        return fileSizeCouldSplit && this.needInnerSplit;
    }

    public abstract Long getFileTotalLength(String filePath);

    public abstract InputStream getFileInputStream(StartEndPair startEndPair);
}
