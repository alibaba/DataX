package com.alibaba.datax.plugin.writer.obswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.obs.services.model.PartEtag;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ObsSingleObject {
    private static Logger logger = LoggerFactory.getLogger(ObsSingleObject.class);

    /**
     * 一个uploadId即一个obs对象
     */
    public static String uploadId;

    /**
     * 将最后一个未提交的block全部缓存到lastBlockBuffer中
     */
    private static byte[] lastBlockBuffer;

    /**
     * 当前part number
     */
    public static AtomicInteger currentPartNumber = new AtomicInteger(1);

    /**
     * 所有已经提交的block
     * 注：allPartETags是线程安全的list
     */
    public static List<PartEtag> allPartETags = Collections.synchronizedList(new ArrayList());

    /**
     * 将每个task最后未upload的block加入到lastBlockBuffer，</pr>
     * 如果lastBlockBuffer的大小已经超过blockSizeInByte，则需要upload一次, 防止task过多导致lastBlockBuffer暴增OOM
     *
     * @param lastBlock
     * @param obsWriterProxy
     * @param blockSizeInByte
     * @param object
     */
    public synchronized static void addLastBlockBuffer(byte[] lastBlock,
                                                       ObsWriterProxy obsWriterProxy,
                                                       long blockSizeInByte,
                                                       String object, ObsWriterProxy.HeaderProvider headerProvider) {
        lastBlockBuffer = ArrayUtils.addAll(lastBlockBuffer, lastBlock);
        //lastBlockBuffer大小超过blockSizeInByte则需要upload part
        if (lastBlockBuffer != null && lastBlockBuffer.length >= blockSizeInByte) {
            logger.info("write last block buffer part size [{}] to object [{}], all has uploaded part size:{}, current part number:{}, uploadId:{}",
                    lastBlockBuffer.length, object, allPartETags.size(), currentPartNumber.intValue(), uploadId);
            try {
                obsWriterProxy.uploadOnePartForSingleObject(lastBlockBuffer, uploadId, allPartETags, object, headerProvider);
            } catch (Exception e) {
                logger.error("upload part error: {}", e.getMessage(), e);
                throw DataXException.asDataXException(e.getMessage());
            }
            //currentPartNumber自增
            currentPartNumber.incrementAndGet();
            //清空lastBlockBuffer
            lastBlockBuffer = null;
        }

    }

    public static byte[] getLastBlockBuffer() {
        return lastBlockBuffer;
    }

}
