package com.alibaba.datax.plugin.reader.hbase11xreader;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;

import com.alibaba.datax.plugin.reader.hbase11xreader.util.Commons;
import com.alibaba.datax.plugin.reader.hbase11xreader.util.MessyCodeCheck;

public class MultiLineTask  extends HbaseAbstractTask{

    private List<Map> column;
    private List<HbaseColumnCell> hbaseColumnCells;

    public MultiLineTask(Configuration configuration) {
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
//        for (HbaseColumnCell cell : this.hbaseColumnCells) {
//            isConstant = cell.isConstant();
//            isRowkeyColumn = Hbase11xHelper.isRowkeyColumn(cell.getColumnName());
//            if (!isConstant && !isRowkeyColumn) {
//                this.scan.addColumn(cell.getColumnFamily(), cell.getQualifier());
//            }
//        }
    }


    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result = super.getNextHbaseRow();

        if (null == result) {
            return false;
        }
        super.lastResult = result;


        List<Cell> cells = result.listCells();

        String rowkey = Bytes.toString(result.getRow());
        if(MessyCodeCheck.isMessyCode(rowkey)) {
            rowkey="";
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("rowkey", rowkey);
        String tmp = "";

        /**
         * 他们都只能解决单时间戳问题,就是一个cell只有一个时间戳 VERSIONS => '1'
         * 一个rowkey是一行的问题
         */
        Boolean flag = false;
        int count = 1;
        for (Cell cell : cells) {
            String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String qKey = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                    cell.getQualifierLength());
            byte[] bytesValue = CellUtil.cloneValue(cell);
            String valueStr= Commons.getStringByBytes(bytesValue);
//			String valueStr=(String)Commons.byteToObject(bytesValue);


            // 去除value里面的特殊符号
            try {
                valueStr=Commons.parseFieldStr(valueStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
            jsonObject.put(StringUtils.isBlank(family) ? qKey : family + ":" + qKey, valueStr);
            if (count == 1) {
                jsonObject.put("timestamp", cell.getTimestamp() + "");
            }
            count++;

        }
        ;

//        String text = Commons.parseLineStr(jsonObject.toString());

        for (HbaseColumnCell cell : this.hbaseColumnCells) {
            cell.getColumnName();
            Object data = jsonObject.get(cell.getColumnName());
            StringColumn stringColumn = new StringColumn(String.valueOf(data));
            record.addColumn(stringColumn);
        }



        return true;
    }
}
