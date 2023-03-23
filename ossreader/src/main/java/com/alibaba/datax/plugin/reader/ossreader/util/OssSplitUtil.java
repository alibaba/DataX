package com.alibaba.datax.plugin.reader.ossreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.ossreader.OssInputStream;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.split.StartEndPair;
import com.alibaba.datax.plugin.unstructuredstorage.reader.split.UnstructuredSplitUtil;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author: guxuan
 * @Date 2022-05-17 15:48
 */
public class OssSplitUtil extends UnstructuredSplitUtil {

    private static final Logger LOG = LoggerFactory.getLogger(UnstructuredSplitUtil.class);
    public static final Long SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE = 64 * 1024 * 1024L; // 小于 1MB 的文件不做内部切分
    private OSSClient ossClient;
    private String bucketName;
    private Double balanceThreshold;
    private Long avgLen = -1L;
    private Integer splitGroupNum = -1;

    public OssSplitUtil(OSSClient ossClient, String bucketName) {
        super(false);
        this.ossClient = ossClient;
        this.bucketName = bucketName;
    }

    @Override
    public Long getFileTotalLength(String filePath) {
        // 获取object字节总数
        GetObjectRequest getObjectRequest = new GetObjectRequest(this.bucketName, filePath);
        OSSObject ossObject = this.ossClient.getObject(getObjectRequest);
        return ossObject.getObjectMetadata().getContentLength();
    }

    @Override
    public InputStream getFileInputStream(StartEndPair startEndPair) {
        InputStream inputStream = new OssInputStream(this.ossClient, this.bucketName, startEndPair.getFilePath(),
                startEndPair.getStart(), startEndPair.getEnd());
        return inputStream;
    }

    private Boolean canSplitSingleFile(Configuration jobConfig) {
        Boolean enableInnerSplit = jobConfig.getBool(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENABLE_INNER_SPLIT, true);
        if (!enableInnerSplit) {
            return false;
        }

        // 默认不切分
        String fileFormat = jobConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILE_FORMAT,
                com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_FILE_FORMAT);
        String compressType = jobConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS);

        // 如果不满足"是text格式且非压缩文件"，则直接返回false
        if (! StringUtils.equalsIgnoreCase(fileFormat, com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.FILE_FORMAT_TEXT) ||
                ! StringUtils.isBlank(compressType)) {
            return false;
        }

        // todo: 判断文件是否为软连接文件，如果为软连接文件，则不支持内部切分

        return true;
    }

    private boolean isGroupsBalance(List<Group> groups) {
        assert (groups != null);

        if(groups.size() <= 1) {
            return true;
        }

        double avg = (double) this.avgLen * (1.0 + this.balanceThreshold/100);
        for (Group group : groups) {
            if(group.getFilledLenght() > avg) {
                return false;
            }
        }
        return true;
    }

    /*
     * 把 allObjectKeySizePares 分成 N 组，尽量使得各组中文件 size 之和 近似
     * */
    private List<Group> splitObjectToGroups(List<Pair<String, Long>> allObjKeySizePares, Integer N) {
        List<Group> groups;

        // 若文件数 <= N，则每个文件分一个组
        if(allObjKeySizePares.size() <= N) {
            groups = new ArrayList<>();
            int index = 0;
            for (Pair<String, Long> pair : allObjKeySizePares) {
                // capacity 初始化为avgLen
                Group group = new Group(avgLen);
                FileBlock fileBlock = new FileBlock(pair.getKey(), 0L, pair.getValue() - 1);
                group.fill(fileBlock);
                groups.add(group);
            }

            // 文件不足N，则以空group补全
            for (int i = groups.size(); i < N; i++) {
                groups.add(new Group(avgLen));
            }

            return groups;
        }

        //文件数量 > N
        //对 allObjKeySizePairs 按照 size 从大到小排序
        allObjKeySizePares.sort(new Comparator<Pair<String, Long>>() {
            @Override
            public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
                if (o1.getValue().compareTo(o2.getValue()) < 0) {
                    return 1;
                }
                if (o1.getValue().equals(o2.getValue())) {
                    return 0;
                }
                return -1;
            }
        });

        groups = new ArrayList<>(N);

        for (int i = 0; i < N; i++) {
            Group group = new Group(avgLen);
            groups.add(group);
        }

        for (Pair<String, Long> pair : allObjKeySizePares) {
            FileBlock fileBlock = new FileBlock(pair.getKey(), 0L, pair.getValue() - 1);

            // 对于avgLen < 0 的极端情况，直接将文件按照数量均分到各个group
            if (avgLen > 0 && pair.getValue() >= avgLen) {
                // 若果文件size > avgLen，则独立成组（放在一个空的group中
                for (int index = 0; index < N; index++) {
                    if (groups.get(index).isEmpty()) {
                        groups.get(index).fill(fileBlock);
                        break;
                    }
                }
            } else {
                // 如果文件小于平均长度，则将其放在一个当前能够容纳，且容量最接近的 group 中
                int selectedIndex = 0, index = 0;
                // 先找到第一个能容纳的
                for (; index < N; index++) {
                    if (groups.get(index).getCapacity() >= fileBlock.getSize()) {
                        selectedIndex = index;
                    }
                }
                // 找到能容纳且剩余容量最小的
                for (;index < N; index++) {
                    if (groups.get(index).getCapacity() >= fileBlock.getSize()
                            && groups.get(index).getCapacity() < groups.get(selectedIndex).getCapacity()) {
                        selectedIndex = index;
                    }
                }
                groups.get(selectedIndex).fill(fileBlock);
            }

        }

        return groups;
    }

    private void reBalanceGroup(List<Group> groups) {
        LOG.info("reBalance start");
        assert (groups != null && groups.size() > 0);
        // 对某些group内部的文件进行进一步切分
        /* 1. 选出负载最小和最大的组 */
        Group groupMinLoad = groups.get(0);
        Group groupMaxLoad = groups.get(0);
        for (Group group : groups) {
            if (group.getFilledLenght() > groupMaxLoad.getFilledLenght()) {
                groupMaxLoad = group;
            }

            if (group.getFilledLenght() < groupMinLoad.getFilledLenght()) {
                groupMinLoad = group;
            }
        }

        /* 2. 将 groupMaxLoad 最大文件切分出部分放入 groupMinLoad
         *     大小为 min{grouMaxLoad.length - mean, mean - groupMinLoad.length} */
        Long splitLen = Math.min(groupMinLoad.getCapacity(), groupMaxLoad.getOverloadLength());
        FileBlock splitOutBlock = groupMaxLoad.split(splitLen, this.ossClient, this.bucketName);

        groupMinLoad.fill(splitOutBlock);
        LOG.info("reBalance end");
    }

    private Long getTotoalLenOfObjList(List<Pair<String, Long>> objKeySizePares) {
        Long totalLen = 0L;
        for (Pair<String, Long> pair : objKeySizePares) {
            totalLen += (pair.getValue() < 0 ? 1 : pair.getValue());
        }

        return totalLen;
    }

    public List<Configuration> getSplitedConfigurations(Configuration originConfiguration, List<Pair<String, Long>> objKeySizePares,
                                                        int adviceNumber) {
        List<Configuration> configurationList = new ArrayList<>();

        this.splitGroupNum = adviceNumber;
        this.avgLen = (long)Math.ceil((double)this.getTotoalLenOfObjList(objKeySizePares) / this.splitGroupNum);
        this.balanceThreshold = originConfiguration.getDouble(com.alibaba.datax.plugin.reader.ossreader.Key.BALANCE_THRESHOLD, 10.0);

        List<Group> groups = this.splitObjectToGroups(objKeySizePares, this.splitGroupNum);

        // 划分后，各个组间如果长度确实比较近似，则不需要进一步对单个文件进行内部切分，反之，则需要对单个文件进行内部切分以进行进一步的调整
        if (canSplitSingleFile(originConfiguration)) {
            // 防止文件内部单行过大，对循环加以限制，理论上最多只需要调整 splitGroupNum 次
            Integer i = 0;
            Long timeStart = System.currentTimeMillis();
            while (i++ < splitGroupNum && ! this.isGroupsBalance(groups)) {
                this.reBalanceGroup(groups);
            }
            Long timeEnd = System.currentTimeMillis();
            LOG.info("split groups cost {} ms", timeEnd - timeStart);
        }

        LOG.info("Splited gourps:\n");
        for (Group group : groups) {
            LOG.info(group.toString());
        }

        // 根据Groups划分结果初始化各个分片任务配置
        for (Group group : groups) {
            Configuration configuration = originConfiguration.clone();
            // 根据groups初始化分片
            List<StartEndPair> startEndPairs = new ArrayList<>();
            for (FileBlock fileBlock : group.getFileBLocks()) {
                if (canSplitSingleFile(originConfiguration)) {
                    startEndPairs.add(new StartEndPair(fileBlock.getStartOffset(), fileBlock.getEndOffset(), fileBlock.getObjName()));
                } else {
                    // 如果不支持内部切分，则设置结束位点为-1，直接读取文件全部内容
                    // 对于软连接文件，这是必要的 30190064
                    startEndPairs.add(new StartEndPair(fileBlock.getStartOffset(), -1L, fileBlock.getObjName()));
                }
            }
            configuration.set(Key.SPLIT_SLICE_CONFIG, startEndPairs);
            configurationList.add(configuration);
        }

        return configurationList;
    }
}

class Group {
    /*
     * fileBlockList 表示该Group中对应的文件块列表，单个文件块用一个三元组<objectName, start, end> 表示
     * */
    private List<FileBlock> fileBLockList;
    private Long capacity;
    private Long filledLenght;
    private static final Logger LOG = LoggerFactory.getLogger(Group.class);

    Group (Long capacity) {
        this(new ArrayList<>(), capacity);
    }

    Group (List<FileBlock> fileBLockList, Long capacity) {
        this.capacity = capacity;
        this.fileBLockList = fileBLockList;
        this.filledLenght = 0L;
        for (FileBlock fileBlock : fileBLockList) {
            this.filledLenght += fileBlock.getSize();
            this.capacity -= fileBlock.getSize();
        }
    }

    void fill(FileBlock fileBlock) {
        if (null == fileBlock) {
            return;
        }
        this.fileBLockList.add(fileBlock);
        this.capacity -= fileBlock.getSize();
        this.filledLenght += fileBlock.getSize();
    }

    void take(FileBlock fileBlock) {
        this.capacity += fileBlock.getSize();
        this.filledLenght -= fileBlock.getSize();
        this.fileBLockList.remove(fileBlock);
    }

    Long getCapacity() {
        return this.capacity;
    }

    void setCapacity(Long capacity) {
        this.capacity = capacity;
    }

    Long getFilledLenght() {
        return this.filledLenght;
    }

    public boolean isEmpty() {
        return this.fileBLockList.isEmpty();
    }

    public boolean isFull() {
        return this.capacity <= 0;
    }

    List<FileBlock> getFileBLocks() {
        return this.fileBLockList;
    }

    private Integer getBiggestFileBlock() {
        Integer index = 0;
        Long maxSize = -1L;
        for (int i = 0; i < this.fileBLockList.size(); i++) {
            if (this.fileBLockList.get(index).getSize() > maxSize) {
                index = i;
            }
        }
        return index;
    }

    /*
     * 对Group进行切分，切分逻辑为：对最大block进行切分，前splitLen个字节作为一个新块
     * */
    FileBlock split(Long splitLen, OSSClient ossClient, String ossBucketName) {
        Integer bigBlockIndex = this.getBiggestFileBlock();
        FileBlock bigBlock = this.fileBLockList.get(bigBlockIndex);
        // 如果最大块的不足 10MB，则不进行内部切分直接返回
        if (bigBlock.getSize() <= OssSplitUtil.SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE) {
            return null;
        }

        FileBlock outBlock;
        FileBlock remainBlock;

        this.take(bigBlock);

        // 如果splitLen 大于 最大块的长度, 则直接把最大块切分出去
        if (splitLen >= bigBlock.getSize()) {
            outBlock = new FileBlock(bigBlock);
        } else {
            Long originalEnd = bigBlock.getEndOffset();
            outBlock = new FileBlock(bigBlock.getObjName(), bigBlock.getStartOffset(), bigBlock.getStartOffset() + splitLen - 1);

            // 校准第一个block的结束位点，即往后推到第一个换行符
            InputStream inputStream = new OssInputStream(ossClient, ossBucketName, outBlock.getObjName(), outBlock.getEndOffset(), originalEnd);
            Long endForward = this.getLFIndex(inputStream);
            outBlock.setEndOffset(outBlock.getEndOffset() + endForward);

            // outblock取的是前边部分record，切分除去后，剩余部分可能为空，这时候不生成remainBlock，确保有剩余（outBlock.end > originEnd)时再生成remainBlock.
            if (outBlock.getEndOffset() < originalEnd) {
                remainBlock = new FileBlock(bigBlock.getObjName(), outBlock.getEndOffset() + 1, originalEnd);
                this.fill(remainBlock);
            }
        }

        return outBlock;
    }

    Long getOverloadLength() {
        return Math.max(0, -this.capacity);
    }

    /**
     * 获取到输入流开始的第一个'\n'偏移量
     *
     * @param inputStream
     *            输入流
     * @return
     */
    public Long getLFIndex(InputStream inputStream) {
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
        return -1L;
    }

    public String toString() {
        JSONArray fbList = new JSONArray();
        int index = 0;
        for (FileBlock fb : this.fileBLockList) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(String.format("block[%d]", index++), fb.toString());
            fbList.add(jsonObject);
        }
        return fbList.toString();
    }
}

class FileBlock {
    private String objName;
    private Long startOffset;
    private Long endOffset;
    private Long size;

    FileBlock(String objName, Long startOffset, Long endOffset) {
        assert (StringUtils.isNotBlank(objName) && startOffset >= 0 );
        assert (endOffset == -1 || startOffset <= endOffset);

        this.objName = objName;
        this.startOffset = startOffset;
        // endOffset < 0 的情况下，统一设置为-1，size 设置为0
        this.endOffset = endOffset < 0 ? -1 : endOffset;
        this.size = endOffset < 0 ? 1 : this.endOffset - this.startOffset + 1;
    }

    public FileBlock(String objName) {
        this(objName, 0L, -1L);
    }

    public FileBlock(String objName, Pair<Long, Long> starEndPair) {
        this(objName, starEndPair.getKey(), starEndPair.getValue());
    }

    public FileBlock(FileBlock fileBlock) {
        assert (fileBlock != null);
        this.objName = fileBlock.objName;
        this.startOffset = fileBlock.startOffset;
        this.endOffset = fileBlock.endOffset;
        this.size = fileBlock.size;
    }

    Long getSize() {
        return this.size;
    }

    Long getStartOffset() {
        return this.startOffset;
    }

    void setStartOffset(Long startOffset) {
        Long deltaSize = this.startOffset - startOffset;
        this.startOffset = startOffset;
        this.size += deltaSize;
    }

    Long getEndOffset() {
        return this.endOffset;
    }

    void setEndOffset(Long endOffset) {
        Long deltaSize = endOffset - this.endOffset;
        this.endOffset = endOffset;
        //size随之调整
        this.size += deltaSize;
    }

    String getObjName() {
        return this.objName;
    }

    public String toString() {
        return String.format("<%s,%d,%d>", this.objName, this.startOffset, this.endOffset);
    }
}
