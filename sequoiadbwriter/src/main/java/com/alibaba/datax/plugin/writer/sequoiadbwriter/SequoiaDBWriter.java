package com.alibaba.datax.plugin.writer.sequoiadbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.sequoiadbwriter.util.SequoiaDBUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Strings;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SequoiaDBWriter extends Writer {

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
        public void destroy() {

        }

        @Override
        public void prepare() {
            super.prepare();
        }
    }

    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig;
        private Sequoiadb SDBClient = null;
        private String userName = null;
        private String password = null;

        private String collectionSpace = null;
        private String collection = null;
        private Integer batchSize = null;
        private JSONArray sequoiaDBColumnMeta = null;
        private static int BATCH_SIZE = 1000;

        @Override
        public void startWrite(RecordReceiver lineReceiver) {

            if(Strings.isNullOrEmpty(collectionSpace) || Strings.isNullOrEmpty(collection)
                    || SDBClient == null || sequoiaDBColumnMeta == null || batchSize == null) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.ILLEGAL_VALUE, SequoiaDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }

            DBCollection cl = SDBClient.getCollectionSpace(collectionSpace).getCollection(collection);
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            while((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(cl,writerBuffer,sequoiaDBColumnMeta);
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                doBatchInsert(cl,writerBuffer,sequoiaDBColumnMeta);
                writerBuffer.clear();
            }

        }

        private void doBatchInsert(DBCollection cl, List<Record> writerBuffer, JSONArray columnMeta) {


            List<BSONObject> dataList = new ArrayList<BSONObject>();

            for(Record record : writerBuffer) {

                BSONObject data = new BasicBSONObject();

                for(int i = 0; i < record.getColumnNumber(); i++) {

                    String type = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_TYPE);
                    //空记录处理
                    if (Strings.isNullOrEmpty(record.getColumn(i).asString())) {
                        if (KeyConstant.isArrayType(type.toLowerCase())) {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), new Object[0]);
                        } else {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString());
                        }
                        continue;
                    }
                    if (Column.Type.INT.name().equalsIgnoreCase(type)) {
                        try {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    Integer.parseInt(
                                            String.valueOf(record.getColumn(i).getRawData())));
                        } catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        }
                    } else if(record.getColumn(i) instanceof StringColumn){

                        try {
                            if (KeyConstant.isObjectIdType(type)) {
                                //objectId
                                data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                        new ObjectId(record.getColumn(i).asString()));
                            } else if (KeyConstant.isArrayType(type)) {
                                //数组类型
                                String splitter = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_SPLITTER);
                                if (Strings.isNullOrEmpty(splitter)) {
                                    throw DataXException.asDataXException(SequoiaDBWriterErrorCode.ILLEGAL_VALUE,
                                            SequoiaDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
                                }
                                String itemType = columnMeta.getJSONObject(i).getString(KeyConstant.ITEM_TYPE);
                                if (itemType != null && !itemType.isEmpty()) {
                                    //如果数组指定类型不为空，将其转换为指定类型
                                    String[] item = record.getColumn(i).asString().split(splitter);
                                    if (itemType.equalsIgnoreCase(Column.Type.DOUBLE.name())) {
                                        ArrayList<Double> list = new ArrayList<Double>();
                                        for (String s : item) {
                                            list.add(Double.parseDouble(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list);
                                    } else if (itemType.equalsIgnoreCase(Column.Type.INT.name())) {
                                        ArrayList<Integer> list = new ArrayList<Integer>();
                                        for (String s : item) {
                                            list.add(Integer.parseInt(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list);
                                    } else if (itemType.equalsIgnoreCase(Column.Type.LONG.name())) {
                                        ArrayList<Long> list = new ArrayList<Long>();
                                        for (String s : item) {
                                            list.add(Long.parseLong(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list);
                                    } else if (itemType.equalsIgnoreCase(Column.Type.BOOL.name())) {
                                        ArrayList<Boolean> list = new ArrayList<Boolean>();
                                        for (String s : item) {
                                            list.add(Boolean.parseBoolean(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list);
                                    } else if (itemType.equalsIgnoreCase(Column.Type.BYTES.name())) {
                                        ArrayList<Byte> list = new ArrayList<Byte>();
                                        for (String s : item) {
                                            list.add(Byte.parseByte(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list);
                                    }
                                } else {
                                    String[] item = record.getColumn(i).asString().split(splitter);
                                    ArrayList list = new ArrayList<>();
                                    Collections.addAll(list, item);
                                    data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list);
                                }
                            } else if(KeyConstant.isDocumentType(type)) {
                                //如果是对象类型,将其进行转换
                                Object mode = org.bson.util.JSON.parse(record.getColumn(i).asString());
                                data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),mode);
                            } else {
                                data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString());
                            }
                        } catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        }
                    } else if(record.getColumn(i) instanceof LongColumn) {

                        if (Column.Type.LONG.name().equalsIgnoreCase(type)) {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asLong());
                        } else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }

                    } else if(record.getColumn(i) instanceof DateColumn) {

                        if (Column.Type.DATE.name().equalsIgnoreCase(type)) {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    record.getColumn(i).asDate());
                        } else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }

                    } else if(record.getColumn(i) instanceof DoubleColumn) {

                        if (Column.Type.DOUBLE.name().equalsIgnoreCase(type)) {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    record.getColumn(i).asDouble());
                        } else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }

                    } else if(record.getColumn(i) instanceof BoolColumn) {

                        if (Column.Type.BOOL.name().equalsIgnoreCase(type)) {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    record.getColumn(i).asBoolean());
                        } else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }

                    } else if(record.getColumn(i) instanceof BytesColumn) {

                        if (Column.Type.BYTES.name().equalsIgnoreCase(type)) {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    record.getColumn(i).asBytes());
                        } else {
                            super.getTaskPluginCollector().collectDirtyRecord(record, "record's [" + i + "] column's type should be: " + type);
                        }

                    } else {
                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),record.getColumn(i).asString());
                    }
                }
                dataList.add(data);
            }

            cl.insert(dataList, DBCollection.FLG_INSERT_CONTONDUP);
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.userName = writerSliceConfig.getString(KeyConstant.SDB_USER_NAME);
            this.password = writerSliceConfig.getString(KeyConstant.SDB_USER_PASSWORD);
            this.collectionSpace = writerSliceConfig.getString(KeyConstant.SDB_COLLECTION_SPACE);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                this.SDBClient = SequoiaDBUtil.initAuthenticationSDBClient(this.writerSliceConfig, userName, password);
            } else {
                this.SDBClient = SequoiaDBUtil.initSDBClient(this.writerSliceConfig);
            }
            this.collection = writerSliceConfig.getString(KeyConstant.SDB_COLLECTION);
            this.batchSize = BATCH_SIZE;
            this.sequoiaDBColumnMeta = JSON.parseArray(writerSliceConfig.getString(KeyConstant.SDB_COLUMN));
        }

        @Override
        public void destroy() {
            SDBClient.close();
        }

        @Override
        public void prepare() {
            super.prepare();
        }
    }
}
