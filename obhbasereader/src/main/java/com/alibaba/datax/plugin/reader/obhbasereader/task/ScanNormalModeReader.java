package com.alibaba.datax.plugin.reader.obhbasereader.task;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ColumnType;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanNormalModeReader extends AbstractScanReader {
    private static Logger LOG = LoggerFactory.getLogger(ScanNormalModeReader.class);

    public ScanNormalModeReader(Configuration configuration) {
        super(configuration);
        this.maxVersion = 1;
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result = getNextHbaseRow();
        if (null == result) {
            return false;
        }
        try {
            byte[] hbaseColumnValue;
            String columnName;
            ColumnType columnType;

            byte[] cf;
            byte[] qualifier;

            for (HbaseColumnCell cell : this.hbaseColumnCellMap.values()) {
                columnType = cell.getColumnType();
                Column column = null;
                if (cell.isConstant()) {
                    // 对常量字段的处理
                    column = constantMap.get(cell.getColumnName());
                } else {
                    // 根据列名称获取值
                    columnName = cell.getColumnName();
                    if (ObHbaseReaderUtil.isRowkeyColumn(columnName)) {
                        hbaseColumnValue = result.getRow();
                    } else {
                        cf = cell.getCf();
                        qualifier = cell.getQualifier();
                        hbaseColumnValue = result.getValue(cf, qualifier);
                    }
                    column = ObHbaseReaderUtil.buildColumn(hbaseColumnValue, columnType, super.encoding, cell.getDateformat(), timezone);
                }
                record.addColumn(column);
            }
        } catch (Exception e) {
            // 注意，这里catch的异常，期望是byte数组转换失败的情况。而实际上，string的byte数组，转成整数类型是不容易报错的。但是转成double类型容易报错。
            record.setColumn(0, new StringColumn(Bytes.toStringBinary(result.getRow())));
            throw e;
        }
        return true;
    }
}
