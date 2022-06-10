package com.alibaba.datax.plugin.unstructuredstorage.reader.binaryFileUtil;

import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: guxuan
 * @Date 2022-05-17 15:59
 */
public class BinaryFileReaderUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BinaryFileReaderUtil.class);

    public static void readFromStream(InputStream inputStream, String filePath, RecordSender recordSender, int blockSizeInByte) {
        try {
            Map<String, String> meta = UnstructuredStorageReaderUtil.buildRecordMeta(filePath);
            byte[] tmp = new byte[blockSizeInByte];
            int len;
            ByteUtils byteUtils = new ByteUtils();
            while ((len = inputStream.read(tmp)) != -1) {
                /**如果len小于blockSizeInByte,说明已经读到了最后一个byte数组
                 * 此时需要将byte数组长度调整为实际读到的字节数,
                 * 否则会导致写入目的文件字节数大于实际文件字节数, 有可能会导致文件损坏(比如pptx, docx等文件)
                 */
                // warn: 这里可以优化掉，没必要做一次数组拷贝，直接复用byte[] tmp即可
                byte[] readBytesArray = Arrays.copyOf(tmp, len);
                byteUtils.append(readBytesArray);
                if (byteUtils.getSize() >= blockSizeInByte) {
                    recordSenderBytesColumn(recordSender, byteUtils.getBuffer(), meta);
                    byteUtils.clear();
                }
            }
            recordSenderBytesColumn(recordSender, byteUtils.getBuffer(), meta);
            LOG.info("End read!!!");
        } catch (IOException e) {
            throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.READ_FILE_IO_ERROR, e);
        }
    }

    private static void recordSenderBytesColumn(RecordSender recordSender, byte[] tmp, Map<String, String> meta){
        Record record = recordSender.createRecord();
        Column column = new BytesColumn(tmp);
        record.addColumn(column);
        record.setMeta(meta);
        recordSender.sendToWriter(record);
    }


}
