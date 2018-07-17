package com.alibaba.datax.plugin.writer.mongodbwriter;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.writer.mongodbwriter.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MongoDBWriter extends Writer{

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

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String database = null;
        private String collection = null;
        private Integer batchSize = null;
        private JSONArray mongodbColumnMeta = null;
        private JSONObject writeMode = null;
        private static int BATCH_SIZE = 1000;

        @Override
        public void prepare() {
            super.prepare();
            //获取presql配置，并执行
            String preSql = writerSliceConfig.getString(Key.PRE_SQL);
            if(Strings.isNullOrEmpty(preSql)) {
                return;
            }
            Configuration conConf = Configuration.from(preSql);
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
                    || mongoClient == null || mongodbColumnMeta == null || batchSize == null) {
                throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                        MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);
            String type = conConf.getString("type");
            if (Strings.isNullOrEmpty(type)){
                return;
            }
            if (type.equals("drop")){
                col.drop();
            } else if (type.equals("remove")){
                String json = conConf.getString("json");
                BasicDBObject query;
                if (Strings.isNullOrEmpty(json)) {
                    query = new BasicDBObject();
                    List<Object> items = conConf.getList("item", Object.class);
                    for (Object con : items) {
                        Configuration _conf = Configuration.from(con.toString());
                        if (Strings.isNullOrEmpty(_conf.getString("condition"))) {
                            query.put(_conf.getString("name"), _conf.get("value"));
                        } else {
                            query.put(_conf.getString("name"),
                                    new BasicDBObject(_conf.getString("condition"), _conf.get("value")));
                        }
                    }
//              and  { "pv" : { "$gt" : 200 , "$lt" : 3000} , "pid" : { "$ne" : "xxx"}}
//              or  { "$or" : [ { "age" : { "$gt" : 27}} , { "age" : { "$lt" : 15}}]}
                } else {
                    query = (BasicDBObject) com.mongodb.util.JSON.parse(json);
                }
                col.deleteMany(query);
            }
            if(logger.isDebugEnabled()) {
                logger.debug("After job prepare(), originalConfig now is:[\n{}\n]", writerSliceConfig.toJSON());
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            if(Strings.isNullOrEmpty(database) || Strings.isNullOrEmpty(collection)
                    || mongoClient == null || mongodbColumnMeta == null || batchSize == null) {
                throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                                                MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection<BasicDBObject> col = db.getCollection(this.collection, BasicDBObject.class);
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            while((record = lineReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if(writerBuffer.size() >= this.batchSize) {
                    doBatchInsert(col,writerBuffer,mongodbColumnMeta);
                    writerBuffer.clear();
                }
            }
            if(!writerBuffer.isEmpty()) {
                doBatchInsert(col,writerBuffer,mongodbColumnMeta);
                writerBuffer.clear();
            }
        }

        private void doBatchInsert(MongoCollection<BasicDBObject> collection, List<Record> writerBuffer, JSONArray columnMeta) {

            List<BasicDBObject> dataList = new ArrayList<BasicDBObject>();

            for(Record record : writerBuffer) {

                BasicDBObject data = new BasicDBObject();

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
                        //int是特殊类型, 其他类型按照保存时Column的类型进行处理
                        try {
                            data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    Integer.parseInt(
                                            String.valueOf(record.getColumn(i).getRawData())));
                        } catch (Exception e) {
                            super.getTaskPluginCollector().collectDirtyRecord(record, e);
                        }
                    } else if(record.getColumn(i) instanceof StringColumn){
                        //处理ObjectId和数组类型
                        try {
                            if (KeyConstant.isObjectIdType(type.toLowerCase())) {
                                data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),
                                    new ObjectId(record.getColumn(i).asString()));
                            } else if (KeyConstant.isArrayType(type.toLowerCase())) {
                                String splitter = columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_SPLITTER);
                                if (Strings.isNullOrEmpty(splitter)) {
                                    throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                                            MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
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
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list.toArray(new Double[0]));
                                    } else if (itemType.equalsIgnoreCase(Column.Type.INT.name())) {
                                        ArrayList<Integer> list = new ArrayList<Integer>();
                                        for (String s : item) {
                                            list.add(Integer.parseInt(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list.toArray(new Integer[0]));
                                    } else if (itemType.equalsIgnoreCase(Column.Type.LONG.name())) {
                                        ArrayList<Long> list = new ArrayList<Long>();
                                        for (String s : item) {
                                            list.add(Long.parseLong(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list.toArray(new Long[0]));
                                    } else if (itemType.equalsIgnoreCase(Column.Type.BOOL.name())) {
                                        ArrayList<Boolean> list = new ArrayList<Boolean>();
                                        for (String s : item) {
                                            list.add(Boolean.parseBoolean(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list.toArray(new Boolean[0]));
                                    } else if (itemType.equalsIgnoreCase(Column.Type.BYTES.name())) {
                                        ArrayList<Byte> list = new ArrayList<Byte>();
                                        for (String s : item) {
                                            list.add(Byte.parseByte(s));
                                        }
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), list.toArray(new Byte[0]));
                                    } else {
                                        data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString().split(splitter));
                                    }
                                } else {
                                    data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME), record.getColumn(i).asString().split(splitter));
                                }
                            } else if(type.toLowerCase().equalsIgnoreCase("json")) {
                                //如果是json类型,将其进行转换
                                Object mode = com.mongodb.util.JSON.parse(record.getColumn(i).asString());
                                data.put(columnMeta.getJSONObject(i).getString(KeyConstant.COLUMN_NAME),JSON.toJSON(mode));
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
            /**
             * 如果存在重复的值覆盖
             */
            if(this.writeMode != null &&
                    this.writeMode.getString(KeyConstant.IS_REPLACE) != null &&
                    KeyConstant.isValueTrue(this.writeMode.getString(KeyConstant.IS_REPLACE))) {
                String uniqueKey = this.writeMode.getString(KeyConstant.UNIQUE_KEY);
                if(!Strings.isNullOrEmpty(uniqueKey)) {
                    List<ReplaceOneModel<BasicDBObject>> replaceOneModelList = new ArrayList<ReplaceOneModel<BasicDBObject>>();
                    for(BasicDBObject data : dataList) {
                        BasicDBObject query = new BasicDBObject();
                        if(uniqueKey != null) {
                            query.put(uniqueKey,data.get(uniqueKey));
                        }
                        ReplaceOneModel<BasicDBObject> replaceOneModel = new ReplaceOneModel<BasicDBObject>(query, data, new UpdateOptions().upsert(true));
                        replaceOneModelList.add(replaceOneModel);
                    }
                    collection.bulkWrite(replaceOneModelList, new BulkWriteOptions().ordered(false));
                } else {
                    throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,
                            MongoDBWriterErrorCode.ILLEGAL_VALUE.getDescription());
                }
            } else {
                collection.insertMany(dataList);
            }
        }


        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.userName = writerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
            this.password = writerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            this.database = writerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(this.writerSliceConfig,userName,password,database);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(this.writerSliceConfig);
            }
            this.collection = writerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.batchSize = BATCH_SIZE;
            this.mongodbColumnMeta = JSON.parseArray(writerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.writeMode = JSON.parseObject(writerSliceConfig.getString(KeyConstant.WRITE_MODE));
        }

        @Override
        public void destroy() {
            mongoClient.close();
        }
    }

}
