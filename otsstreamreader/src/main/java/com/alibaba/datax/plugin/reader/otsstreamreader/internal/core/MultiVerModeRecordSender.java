package com.alibaba.datax.plugin.reader.otsstreamreader.internal.core;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.ColumnValueTransformHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alicloud.openservices.tablestore.model.*;

/**
 * 输出完整的增量变化信息，每一行为一个cell的变更记录，输出样例如下:
 * | pk1 | pk2 | column_name | timestamp | column_value | op_type | seq_id |
 * | --- | --- | ----------- | --------- | ------------ | ------- | ------ |
 * |  a  |  b  |    col1     | 10928121  |    null      |  DO     |   001  | 删除某一列某个特定版本
 * |  a  |  b  |    col2     |   null    |    null      |  DA     |   002  | 删除某一列所有版本
 * |  a  |  b  |    null     |   null    |    null      |  DR     |   003  | 删除整行
 * |  a  |  b  |    col1     | 1928821   |    abc       |  U      |   004  | 插入一列
 *
 */
public class MultiVerModeRecordSender implements IStreamRecordSender {

    enum OpType {
        U,  // update
        DO, // delete one version
        DA, // delete all version
        DR // delete row
    }

    private final RecordSender dataxRecordSender;
    private String shardId;
    private final boolean isExportSequenceInfo;

    public MultiVerModeRecordSender(RecordSender dataxRecordSender, String shardId, boolean isExportSequenceInfo) {
        this.dataxRecordSender = dataxRecordSender;
        this.shardId = shardId;
        this.isExportSequenceInfo = isExportSequenceInfo;
    }

    @Override
    public void sendToDatax(StreamRecord streamRecord) {
        int colIdx = 0;
        switch (streamRecord.getRecordType()) {
            case PUT:
                sendToDatax(streamRecord.getPrimaryKey(), OpType.DR, null,
                        getSequenceInfo(streamRecord, colIdx++));
                for (RecordColumn recordColumn : streamRecord.getColumns()) {
                    String sequenceInfo = getSequenceInfo(streamRecord, colIdx++);
                    sendToDatax(streamRecord.getPrimaryKey(), recordColumn, sequenceInfo);
                }
                break;
            case UPDATE:
                for (RecordColumn recordColumn : streamRecord.getColumns()) {
                    String sequenceInfo = getSequenceInfo(streamRecord, colIdx++);
                    sendToDatax(streamRecord.getPrimaryKey(), recordColumn, sequenceInfo);
                }
                break;
            case DELETE:
                sendToDatax(streamRecord.getPrimaryKey(), OpType.DR, null,
                        getSequenceInfo(streamRecord, colIdx++));
                break;
            default:
                throw new OTSStreamReaderException("Unknown stream record type: " + streamRecord.getRecordType() + ".");
        }
    }

    private void sendToDatax(PrimaryKey primaryKey, RecordColumn column, String sequenceInfo) {
        switch (column.getColumnType()) {
            case PUT:
                sendToDatax(primaryKey, OpType.U, column.getColumn(), sequenceInfo);
                break;
            case DELETE_ONE_VERSION:
                sendToDatax(primaryKey, OpType.DO, column.getColumn(), sequenceInfo);
                break;
            case DELETE_ALL_VERSION:
                sendToDatax(primaryKey, OpType.DA, column.getColumn(), sequenceInfo);
                break;
            default:
                throw new OTSStreamReaderException("Unknown record column type: " + column.getColumnType() + ".");
        }
    }

    private void sendToDatax(PrimaryKey primaryKey, OpType opType, Column column, String sequenceInfo) {
        Record line = dataxRecordSender.createRecord();

        for (PrimaryKeyColumn pkCol : primaryKey.getPrimaryKeyColumns()) {
            line.addColumn(ColumnValueTransformHelper.otsPrimaryKeyValueToDataxColumn(pkCol.getValue()));
        }

        switch (opType) {
            case U:
                line.addColumn(new StringColumn(column.getName()));
                line.addColumn(new LongColumn(column.getTimestamp()));
                line.addColumn(ColumnValueTransformHelper.otsColumnValueToDataxColumn(column.getValue()));
                line.addColumn(new StringColumn("" + opType));
                if (isExportSequenceInfo) {
                    line.addColumn(new StringColumn(sequenceInfo));
                }
                break;
            case DO:
                line.addColumn(new StringColumn(column.getName()));
                line.addColumn(new LongColumn(column.getTimestamp()));
                line.addColumn(new StringColumn(null));
                line.addColumn(new StringColumn("" + opType));
                if (isExportSequenceInfo) {
                    line.addColumn(new StringColumn(sequenceInfo));
                }
                break;
            case DA:
                line.addColumn(new StringColumn(column.getName()));
                line.addColumn(new StringColumn(null));
                line.addColumn(new StringColumn(null));
                line.addColumn(new StringColumn("" + opType));
                if (isExportSequenceInfo) {
                    line.addColumn(new StringColumn(sequenceInfo));
                }
                break;
            case DR:
                line.addColumn(new StringColumn(null));
                line.addColumn(new StringColumn(null));
                line.addColumn(new StringColumn(null));
                line.addColumn(new StringColumn("" + OpType.DR));
                if (isExportSequenceInfo) {
                    line.addColumn(new StringColumn(sequenceInfo));
                }
                break;
            default:
                throw new OTSStreamReaderException("Unknown operation type: " + opType + ".");
        }
        synchronized (dataxRecordSender) {
            dataxRecordSender.sendToWriter(line);
        }
    }

    private String getSequenceInfo(StreamRecord streamRecord, int colIdx) {
        int epoch = streamRecord.getSequenceInfo().getEpoch();
        long timestamp = streamRecord.getSequenceInfo().getTimestamp();
        int rowIdx = streamRecord.getSequenceInfo().getRowIndex();
        String sequenceId = String.format("%010d_%020d_%010d_%s:%010d", epoch, timestamp, rowIdx, shardId, colIdx);
        return sequenceId;
    }
}
