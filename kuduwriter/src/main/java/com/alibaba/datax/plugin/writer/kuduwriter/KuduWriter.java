package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.base.Strings;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KuduWriter extends Writer{

    public static class Job extends Writer.Job {

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
            for(int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);
        private   Configuration       writerSliceConfig;

        private KuduClient client;
        private KuduTable kuduTable;
        private KuduSession session;

        private String kudu_masters;

        private String tableName;
        private Boolean isUpdate;
        private Integer batchSize = 1024;
        private Schema schema;

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.kudu_masters = writerSliceConfig.getString(Key.KUDU_MASTERS);
            this.tableName = writerSliceConfig.getString(Key.TABLE_NAME);
            this.isUpdate = writerSliceConfig.getBool(Key.IS_UPSERT,false);

            this.client = new KuduClient.KuduClientBuilder(kudu_masters).build();
            Integer batchSIZE = writerSliceConfig.getInt(Key.BATCH_SIZE);
            if ((batchSIZE == null)){
                this.batchSize = 1024;
            }else{
                this.batchSize = batchSIZE;
            }
            try {
                this.kuduTable = this.client.openTable(tableName);
            } catch (KuduException e) {
                String message = String.format("连接Kudu时发生异常,请检查您的网络是否正常!KUDU_MASTERS地址：[%s]",
                        kudu_masters);
                logger.error(message);
                throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_ADDRESS, e);
            }
            this.session = this.client.newSession();
            this.session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            this.schema = this.kuduTable.getSchema();
        }

        @Override
        public void destroy() {
            try{
                this.session.close();
            }catch (KuduException e){
                throw DataXException.asDataXException(KuduWriterErrorCode.CLOSE_EXCEPTION, e.getMessage());
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(Strings.isNullOrEmpty(tableName)) {
                throw DataXException.asDataXException(KuduWriterErrorCode.ILLEGAL_VALUE,
                        KuduWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record;
            logger.info("开始导入数据");
            while((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(this.session,writerBuffer,this.kuduTable);
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                doBatchInsert(this.session,writerBuffer,this.kuduTable);
                writerBuffer.clear();
            }
        }

        private void doBatchInsert(KuduSession session, List<Record> writerBuffer, KuduTable kuduTable) {
            //进行类型转换和数据插入
            if(isUpdate){
                for(Record record : writerBuffer) {
                    Upsert upsert = kuduTable.newUpsert();
                    PartialRow row = upsert.getRow();
                    int flag = 0;//类型错误判断是否应该跳出
                    for(int i = 0; i < record.getColumnNumber(); i++) {
                        //空记录处理
                        if (Strings.isNullOrEmpty(record.getColumn(i).asString())) {
                            row.setNull(i);
                            continue;
                        }
                        try{
                            generateColumnData(row, i, record);
                        }catch (Exception e){
                            super.getTaskPluginCollector().collectDirtyRecord(record ,e.getMessage());
                            flag = 1;
                            break;
                        }
                    }
                    //插入失败时将脏数据收集起来
                    if(flag == 1){
                        continue;
                    }
                    try{
                        session.apply(upsert);
                        if (session.countPendingErrors() > 0) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, session.getPendingErrors().getRowErrors()[0].toString());
                        }
                    }catch (KuduException e){
                        super.getTaskPluginCollector().collectDirtyRecord(record ,e.getMessage());
                    }
                }
                try {
                    this.session.flush();
                } catch (KuduException e) {
                    e.printStackTrace();
                }
            }else {
                for (Record record : writerBuffer) {
                    Insert insert = kuduTable.newInsert();
                    PartialRow row = insert.getRow();
                    int flag = 0;//类型错误判断是否应该跳出
                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        //空记录处理
                        if (Strings.isNullOrEmpty(record.getColumn(i).asString())) {
                            row.setNull(i);
                            continue;
                        }
                        try {
                            generateColumnData(row, i, record);
                        } catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e.getMessage());
                            flag = 1;
                            break;
                        }
                    }
                    //插入失败时将脏数据收集起来
                    if (flag == 1) {
                        continue;
                    }
                    try {
                        session.apply(insert);
                        if (session.countPendingErrors() > 0) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, session.getPendingErrors().getRowErrors()[0].toString());
                        }
                    } catch (KuduException e) {
                        super.getTaskPluginCollector().collectDirtyRecord(record, e.getMessage());
                    }
                }
            }
        }

        /**
         * Add random data to the given row for the column at index {@code index}
         * of type {@code type}
         * @param row The row to add the field to
         */
        private void generateColumnData(PartialRow row, Integer index, Record record) {
            Type type = this.schema.getColumnByIndex(index).getType();
            switch (type) {
                //因为DataX内部转换原因，Date类型会转成String类型
                case INT8:
                    row.addByte(index,  record.getColumn(index).asDouble().byteValue());
                    return;
                case INT16:
                    row.addShort(index, record.getColumn(index).asDouble().shortValue());
                    return;
                case INT32:
                    row.addInt(index, record.getColumn(index).asDouble().intValue());
                    return;
                case INT64:
                    row.addInt(index, record.getColumn(index).asDouble().intValue());
                    return;
                case UNIXTIME_MICROS:
                    row.addLong(index, record.getColumn(index).asLong());
                    return;
                case BINARY:
                    row.addBinary(index, record.getColumn(index).asBytes());
                    return;
                case STRING:
                    row.addString(index, record.getColumn(index).asString());
                    return;
                case BOOL:
                    row.addBoolean(index, record.getColumn(index).asBoolean());
                    return;
                case FLOAT:
                    row.addFloat(index, record.getColumn(index).asDouble().floatValue());
                    return;
                case DOUBLE:
                    row.addDouble(index, record.getColumn(index).asDouble());
                    return;
                default:
                    throw DataXException.asDataXException(KuduWriterErrorCode.UNKNOWN_TYPE, record.getColumn(index).getType().toString());
            }
        }
    }


}
