package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

public class NormalTask extends HbaseAbstractTask {
    private List<Map> column;
    private List<HbaseColumnCell> hbaseColumnCells;

    public NormalTask(Configuration configuration) {
        super(configuration);
        this.column = configuration.getList(Key.COLUMN, Map.class);
        this.hbaseColumnCells = Hbase11xHelper.parseColumnOfNormalMode(this.column);
    }

    /**
     * normal模式下将用户配置的column 设置到scan中
     */
    @Override
    public void initScan(Scan scan) {
        boolean isConstant;
        boolean isRowkeyColumn;
        for (HbaseColumnCell cell : this.hbaseColumnCells) {
            isConstant = cell.isConstant();
            isRowkeyColumn = Hbase11xHelper.isRowkeyColumn(cell.getColumnName());
            if (!isConstant && !isRowkeyColumn) {
                this.scan.addColumn(cell.getColumnFamily(), cell.getQualifier());
            }
        }
    }


    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result = super.getNextHbaseRow();

        if (null == result) {
            return false;
        }
        super.lastResult = result;

        try {
            byte[] hbaseColumnValue;
            String columnName;
            ColumnType columnType;

            byte[] columnFamily;
            byte[] qualifier;

            for (HbaseColumnCell cell : this.hbaseColumnCells) {
                columnType = cell.getColumnType();
                if (cell.isConstant()) {
                    // 对常量字段的处理
                    String constantValue = cell.getColumnValue();

                    Column constantColumn = super.convertValueToAssignType(columnType,constantValue,cell.getDateformat());
                    record.addColumn(constantColumn);
                } else {
                    // 根据列名称获取值
                    columnName = cell.getColumnName();
                    if (Hbase11xHelper.isRowkeyColumn(columnName)) {
                        hbaseColumnValue = result.getRow();
                    } else {
                        columnFamily = cell.getColumnFamily();
                        qualifier = cell.getQualifier();
                        hbaseColumnValue = result.getValue(columnFamily, qualifier);
                    }

                    Column hbaseColumn = super.convertBytesToAssignType(columnType,hbaseColumnValue,cell.getDateformat());
                    record.addColumn(hbaseColumn);
                }
            }
        } catch (Exception e) {
            // 注意，这里catch的异常，期望是byte数组转换失败的情况。而实际上，string的byte数组，转成整数类型是不容易报错的。但是转成double类型容易报错。
            record.setColumn(0, new StringColumn(Bytes.toStringBinary(result.getRow())));
            throw e;
        }
        return true;
    }
}
