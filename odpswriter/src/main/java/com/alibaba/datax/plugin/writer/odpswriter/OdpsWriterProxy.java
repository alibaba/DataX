package com.alibaba.datax.plugin.writer.odpswriter;

import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.odpswriter.util.OdpsUtil;

import com.alibaba.fastjson.JSON;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;

import com.aliyun.odps.data.Record;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OdpsWriterProxy {
    private static final Logger LOG = LoggerFactory
            .getLogger(OdpsWriterProxy.class);

    private volatile boolean printColumnLess;// 是否打印对于源头字段数小于 ODPS 目的表的行的日志

    private TaskPluginCollector taskPluginCollector;

    private TableTunnel.UploadSession slaveUpload;
    private TableSchema schema;
    private int maxBufferSize;
    private ProtobufRecordPack protobufRecordPack;
    private int protobufCapacity;
    private AtomicLong blockId;

    private List<Integer> columnPositions;
    private List<OdpsType> tableOriginalColumnTypeList;
    private boolean emptyAsNull;
    private boolean isCompress;

    public OdpsWriterProxy(TableTunnel.UploadSession slaveUpload, int blockSizeInMB,
                           AtomicLong blockId, List<Integer> columnPositions,
                           TaskPluginCollector taskPluginCollector, boolean emptyAsNull, boolean isCompress)
            throws IOException, TunnelException {
        this.slaveUpload = slaveUpload;
        this.schema = this.slaveUpload.getSchema();
        this.tableOriginalColumnTypeList = OdpsUtil
                .getTableOriginalColumnTypeList(this.schema);

        this.blockId = blockId;
        this.columnPositions = columnPositions;
        this.taskPluginCollector = taskPluginCollector;
        this.emptyAsNull = emptyAsNull;
        this.isCompress = isCompress;

        // 初始化与 buffer 区相关的值
        this.maxBufferSize = (blockSizeInMB - 4) * 1024 * 1024;
        this.protobufCapacity = blockSizeInMB * 1024 * 1024;
        this.protobufRecordPack = new ProtobufRecordPack(this.schema, null, this.protobufCapacity);
        printColumnLess = true;

    }

    public long writeOneRecord(
            com.alibaba.datax.common.element.Record dataXRecord,
            List<Long> blocks) throws Exception {

        Record record = dataxRecordToOdpsRecord(dataXRecord);

        if (null == record) {
            return 0;
        }
        protobufRecordPack.append(record);

        if (protobufRecordPack.getTotalBytes() >= maxBufferSize) {
            long startTimeInNs = System.nanoTime();
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload,
                    protobufRecordPack, blockId.get(), this.isCompress);
            LOG.info("write block {} ok.", blockId.get());
            blocks.add(blockId.get());
            protobufRecordPack.reset();
            this.blockId.incrementAndGet();
            return System.nanoTime() - startTimeInNs;
        }
        return 0;
    }

    public long writeRemainingRecord(List<Long> blocks) throws Exception {
        // complete protobuf stream, then write to http
        if (protobufRecordPack.getTotalBytes() != 0) {
            long startTimeInNs = System.nanoTime();
            OdpsUtil.slaveWriteOneBlock(this.slaveUpload,
                    protobufRecordPack, blockId.get(), this.isCompress);
            LOG.info("write block {} ok.", blockId.get());

            blocks.add(blockId.get());
            // reset the buffer for next block
            protobufRecordPack.reset();
            return System.nanoTime() - startTimeInNs;
        }
        return 0;
    }

    public Record dataxRecordToOdpsRecord(
            com.alibaba.datax.common.element.Record dataXRecord) throws Exception {
        int sourceColumnCount = dataXRecord.getColumnNumber();
        Record odpsRecord = slaveUpload.newRecord();

        int userConfiguredColumnNumber = this.columnPositions.size();
//todo
        if (sourceColumnCount > userConfiguredColumnNumber) {
            throw DataXException
                    .asDataXException(
                            OdpsWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "亲，配置中的源表的列个数和目的端表不一致，源表中您配置的列数是:%s 大于目的端的列数是:%s , 这样会导致源头数据无法正确导入目的端, 请检查您的配置并修改.",
                                    sourceColumnCount,
                                    userConfiguredColumnNumber));
        } else if (sourceColumnCount < userConfiguredColumnNumber) {
            if (printColumnLess) {
                LOG.warn(
                        "源表的列个数小于目的表的列个数，源表列数是:{} 目的表列数是:{} , 数目不匹配. DataX 会把目的端多出的列的值设置为空值. 如果这个默认配置不符合您的期望，请保持源表和目的表配置的列数目保持一致.",
                        sourceColumnCount, userConfiguredColumnNumber);
            }
            printColumnLess = false;
        }

        int currentIndex;
        int sourceIndex = 0;
        try {
            com.alibaba.datax.common.element.Column columnValue;

            for (; sourceIndex < sourceColumnCount; sourceIndex++) {
                currentIndex = columnPositions.get(sourceIndex);
                OdpsType type = this.tableOriginalColumnTypeList
                        .get(currentIndex);
                columnValue = dataXRecord.getColumn(sourceIndex);

                if (columnValue == null) {
                    continue;
                }
                // for compatible dt lib, "" as null
                if(this.emptyAsNull && columnValue instanceof StringColumn && "".equals(columnValue.asString())){
                    continue;
                }

                switch (type) {
                    case STRING:
                        odpsRecord.setString(currentIndex, columnValue.asString());
                        break;
                    case BIGINT:
                        odpsRecord.setBigint(currentIndex, columnValue.asLong());
                        break;
                    case BOOLEAN:
                        odpsRecord.setBoolean(currentIndex, columnValue.asBoolean());
                        break;
                    case DATETIME:
                        odpsRecord.setDatetime(currentIndex, columnValue.asDate());
                        break;
                    case DOUBLE:
                        odpsRecord.setDouble(currentIndex, columnValue.asDouble());
                        break;
                    case DECIMAL:
                        odpsRecord.setDecimal(currentIndex, columnValue.asBigDecimal());
                        String columnStr = columnValue.asString();
                        if(columnStr != null && columnStr.indexOf(".") >= 36) {
                            throw new Exception("Odps decimal 类型的整数位个数不能超过35");
                        }
                    default:
                        break;
                }
            }

            return odpsRecord;
        } catch (Exception e) {
            String message = String.format(
                    "写入 ODPS 目的表时遇到了脏数据: 第[%s]个字段的数据出现错误，请检查该数据并作出修改 或者您可以增大阀值，忽略这条记录.", sourceIndex);
            this.taskPluginCollector.collectDirtyRecord(dataXRecord, e,
                    message);

            return null;
        }

    }
}
