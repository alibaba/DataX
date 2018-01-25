package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.client.Put;

public  class MultiVersionTask extends HbaseAbstractTask {

    public MultiVersionTask(Configuration configuration) {
        super(configuration);
    }

    @Override
    public Put convertRecordToPut(Record record) {
        if (record.getColumnNumber() != 4 ) {
            // multversion 模式下源头读取字段列数为4元组(rowkey,column,timestamp,value),目的端需告诉[]
            throw DataXException
                    .asDataXException(
                            Hbase11xWriterErrorCode.ILLEGAL_VALUE,
                            String.format(
                                    "HbaseWriter multversion模式下列配置信息有错误.源头应该为四元组,实际源头读取字段数:%s,请检查您的配置并作出修改.",
                                    record.getColumnNumber()));
        }
        Put put = null;
        //rowkey
//        ColumnType rowkeyType  = ColumnType.getByTypeName(String.valueOf(columnList.get(0).get(Key.TYPE)));
//        if(record.getColumn(0).getRawData() == null){
//            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "HbaseWriter的rowkey不能为空,请选择合适的rowkey列");
//        }
//        //timestamp
//        if(record.getColumn(2).getRawData()!= null){
//            put = new Put(getColumnByte(rowkeyType,record.getColumn(0)),record.getColumn(2).asLong());
//        }else{
//            put = new Put(getColumnByte(rowkeyType,record.getColumn(0)));
//        }
//        //column family,qualifie
//        Map<String, String> userColumn = columnList.get(1);
//        ColumnType columnType = ColumnType.getByTypeName(userColumn.get(Key.TYPE));
//        String columnName = userColumn.get(Key.NAME);
//        String promptInfo = "Hbasewriter 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + columnName;
//        String[] cfAndQualifier = columnName.split(":");
//        Validate.isTrue(cfAndQualifier != null && cfAndQualifier.length == 2
//                && StringUtils.isNotBlank(cfAndQualifier[0])
//                && StringUtils.isNotBlank(cfAndQualifier[1]), promptInfo);
//
//        if(!columnName.equals(record.getColumn(1).asString())){
//            throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE,
//                    String.format("您的配置中源端和目的端列名不一致,源端为[%s],目的端为[%s],请检查您的配置并作出修改.",record.getColumn(1).asString(),columnName));
//
//        }
//        //value
//        Column column = record.getColumn(3);
//        put.addColumn(Bytes.toBytes(
//                cfAndQualifier[0]),
//                Bytes.toBytes(cfAndQualifier[1]),
//                getColumnByte(columnType,column)
//        );
        return put;
    }

}
