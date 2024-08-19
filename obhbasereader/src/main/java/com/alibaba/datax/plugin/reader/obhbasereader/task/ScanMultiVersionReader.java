package com.alibaba.datax.plugin.reader.obhbasereader.task;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obhbasereader.Constant;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseReaderErrorCode;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ColumnType;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class ScanMultiVersionReader extends AbstractScanReader {
    private final static Logger LOG = LoggerFactory.getLogger(ScanMultiVersionReader.class);
    private static byte[] COLON_BYTE;
    private List<KeyValue> kvList = new ArrayList<>();
    private int currentReadPosition = 0;

    // rowKey类型
    private ColumnType rowkeyReadoutType = null;

    public ScanMultiVersionReader(Configuration configuration) {
        super(configuration);
        HbaseColumnCell rowKey = hbaseColumnCellMap.get(Constant.ROWKEY_FLAG);
        if (rowKey != null && rowKey.getColumnType() != null) {
            this.rowkeyReadoutType = rowKey.getColumnType();
        } else {
            this.rowkeyReadoutType = ColumnType.BYTES;
        }
        try {
            ScanMultiVersionReader.COLON_BYTE = ":".getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw DataXException.asDataXException(HbaseReaderErrorCode.PREPAR_READ_ERROR, "Failed to get binary of column family and column name colon separator inside the system.", e);
        }
    }

    private void convertKVToLine(KeyValue keyValue, Record record) throws Exception {
        byte[] rawRowkey = keyValue.getRow();
        long timestamp = keyValue.getTimestamp();
        byte[] cfAndQualifierName = Bytes.add(keyValue.getFamily(), ScanMultiVersionReader.COLON_BYTE, keyValue.getQualifier());

        record.addColumn(convertBytesToAssignType(this.rowkeyReadoutType, rawRowkey));

        record.addColumn(convertBytesToAssignType(ColumnType.STRING, cfAndQualifierName));

        // 直接忽略了用户配置的 timestamp 的类型
        record.addColumn(new LongColumn(timestamp));

        String cfAndQualifierNameStr = Bytes.toString(cfAndQualifierName);
        HbaseColumnCell currentCell = hbaseColumnCellMap.get(cfAndQualifierNameStr);
        ColumnType valueReadoutType = currentCell != null ? currentCell.getColumnType() : ColumnType.BYTES;
        String dateFormat = currentCell != null ? currentCell.getDateformat() : null;
        record.addColumn(convertBytesToAssignType(valueReadoutType, keyValue.getValue(), dateFormat));
    }

    private Column convertBytesToAssignType(ColumnType columnType, byte[] byteArray) throws Exception {
        return convertBytesToAssignType(columnType, byteArray, null);
    }

    private Column convertBytesToAssignType(ColumnType columnType, byte[] byteArray, String dateFormat) throws Exception {
        return ObHbaseReaderUtil.buildColumn(byteArray, columnType, encoding, dateFormat, timezone);
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result;
        if (this.kvList.size() == this.currentReadPosition) {
            result = getNextHbaseRow();
            if (result == null) {
                return false;
            }
            this.kvList = result.list();
            if (this.kvList == null) {
                return false;
            }
            this.currentReadPosition = 0;
        }

        try {
            KeyValue keyValue = this.kvList.get(this.currentReadPosition);
            convertKVToLine(keyValue, record);
        } finally {
            this.currentReadPosition++;
        }
        return true;
    }
}
