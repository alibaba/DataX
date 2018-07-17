package com.alibaba.datax.plugin.reader.hbase094xreader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MultiVersionTask extends HbaseAbstractTask {
    private static byte[] COLON_BYTE;

    private int maxVersion;
    private List<KeyValue> kvList = new ArrayList<KeyValue>();

    private int currentReadPosition = 0;
    public List<Map> column;
    private HashMap<String,HashMap<String,String>> familyQualifierMap = null;

    public MultiVersionTask(Configuration configuration) {
        super(configuration);
        this.maxVersion = configuration.getInt(Key.MAX_VERSION);
        this.column = configuration.getList(Key.COLUMN, Map.class);
        this.familyQualifierMap = Hbase094xHelper.parseColumnOfMultiversionMode(this.column);

        try {
            MultiVersionTask.COLON_BYTE = ":".getBytes("utf8");
        } catch (UnsupportedEncodingException e) {
            throw DataXException.asDataXException(Hbase094xReaderErrorCode.PREPAR_READ_ERROR, "系统内部获取 列族与列名冒号分隔符的二进制时失败.", e);
        }
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result;
        if (this.kvList == null || this.kvList.size() == this.currentReadPosition) {
            result = super.getNextHbaseRow();
            if (result == null) {
                return false;
            }
            super.lastResult = result;

            this.kvList = result.list();
            if (this.kvList == null) {
                return false;
            }
            this.currentReadPosition = 0;
        }
        try {
            KeyValue keyValue = this.kvList.get(this.currentReadPosition);

            convertCellToLine(keyValue, record);

        } catch (Exception e) {
            throw e;
        } finally {
            this.currentReadPosition++;
        }
        return true;
    }

    private void convertCellToLine(KeyValue keyValue, Record record) throws Exception {
        byte[] rawRowkey = keyValue.getRow();
        long timestamp = keyValue.getTimestamp();
        byte[] cfAndQualifierName = Bytes.add(keyValue.getFamily(), MultiVersionTask.COLON_BYTE, keyValue.getQualifier());
        byte[] columnValue = keyValue.getValue();

        ColumnType rawRowkeyType = ColumnType.getByTypeName(familyQualifierMap.get(Constant.ROWKEY_FLAG).get(Key.TYPE));
        String familyQualifier =  new String(cfAndQualifierName, Constant.DEFAULT_ENCODING);
        ColumnType columnValueType = ColumnType.getByTypeName(familyQualifierMap.get(familyQualifier).get(Key.TYPE));
        String columnValueFormat = familyQualifierMap.get(familyQualifier).get(Key.FORMAT);
        if(StringUtils.isBlank(columnValueFormat)){
            columnValueFormat = Constant.DEFAULT_DATA_FORMAT;
        }

        record.addColumn(convertBytesToAssignType(rawRowkeyType, rawRowkey, columnValueFormat));
        record.addColumn(convertBytesToAssignType(ColumnType.STRING, cfAndQualifierName, columnValueFormat));
        // 直接忽略了用户配置的 timestamp 的类型
        record.addColumn(new LongColumn(timestamp));
        record.addColumn(convertBytesToAssignType(columnValueType, columnValue, columnValueFormat));
    }

    public void setMaxVersions(Scan scan) {
        if (this.maxVersion == -1 || this.maxVersion == Integer.MAX_VALUE) {
            scan.setMaxVersions();
        } else {
            scan.setMaxVersions(this.maxVersion);
        }
    }

}
