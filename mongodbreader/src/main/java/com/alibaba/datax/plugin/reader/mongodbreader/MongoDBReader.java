package com.alibaba.datax.plugin.reader.mongodbreader;

import java.util.*;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb =  originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            String sslMode =  originalConfig.getString(KeyConstant.SSL_MODE);
            String trustStorePath =  originalConfig.getString(KeyConstant.TRUST_STORE_PATH);
            String trustStorePwd =  originalConfig.getString(KeyConstant.TRUST_STORE_PWD);
            String keyStorePath =  originalConfig.getString(KeyConstant.KEY_STORE_PATH);
            String keyStorePwd =  originalConfig.getString(KeyConstant.KEY_STORE_PWD);
            Map<String,Integer> map = new HashMap<String, Integer>();
            map.put("two-way", 1);
            map.put("client-authentication", 2);
            map.put("no-authentication", 3);
            switch (map.get(sslMode)) {
                case 1:
                    this.mongoClient = MongoUtil.initCredentialSSLMongoClient(this.originalConfig,
                      userName,password,authDb,trustStorePath,trustStorePwd,keyStorePath,keyStorePwd);
                    break;
                case 2:
                    this.mongoClient = MongoUtil.initCredentialClientAuthenticationMongoClient(this.originalConfig,
                      userName,password,authDb,keyStorePath,keyStorePwd);
                    break;
                case 3:
                    this.mongoClient = MongoUtil.initCredentialNoAuthenticationMongoClient(this.originalConfig,
                      userName,password,authDb);
                    break;
                default:
                    if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                        this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,authDb);
                    } else {
                        this.mongoClient = MongoUtil.initMongoClient(originalConfig);
                    }
                    break;
            }

        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        @Override
        public void startRead(RecordSender recordSender) {

            if(lowerBound== null || upperBound == null ||
                mongoClient == null || database == null ||
                collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }
            } else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            } else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }
            if(!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }
            dbCursor = col.find(filter).iterator();
            while (dbCursor.hasNext()) {
                Document item = dbCursor.next();
                Record record = recordSender.createRecord();
                Iterator columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject)columnItera.next();
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    if (tempCol == null) {
                        if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String[] name = column.getString(KeyConstant.COLUMN_NAME).split("\\.");
                            if (name.length > 1) {
                                Object obj;
                                Document nestedDocument = item;
                                for (String str : name) {
                                    obj = nestedDocument.get(str);
                                    if (obj instanceof Document) {
                                        nestedDocument = (Document) obj;
                                    }
                                }

                                if (null != nestedDocument) {
                                    Document doc = nestedDocument;
                                    tempCol = doc.get(name[name.length - 1]);
                                }
                            }
                        }
                    }
                    if (tempCol == null) {
                        //continue; 这个不能直接continue会导致record到目的端错位
                        record.addColumn(new StringColumn(null));
                    }else if (tempCol instanceof Double) {
                        //TODO deal with Double.isNaN()
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    }else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    } else {
                        if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if(Strings.isNullOrEmpty(splitter)) {
                                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                            } else {
                                ArrayList array = (ArrayList)tempCol;
                                String tempArrayStr = Joiner.on(splitter).join(array);
                                record.addColumn(new StringColumn(tempArrayStr));
                            }
                        } else {
                            record.addColumn(new StringColumn(tempCol.toString()));
                        }
                    }
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
            this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
            String sslMode =  readerSliceConfig.getString(KeyConstant.SSL_MODE);
            String trustStorePath =  readerSliceConfig.getString(KeyConstant.TRUST_STORE_PATH);
            String trustStorePwd =  readerSliceConfig.getString(KeyConstant.TRUST_STORE_PWD);
            String keyStorePath =  readerSliceConfig.getString(KeyConstant.KEY_STORE_PATH);
            String keyStorePwd =  readerSliceConfig.getString(KeyConstant.KEY_STORE_PWD);
            Map<String,Integer> map = new HashMap<String, Integer>();
            map.put("two-way", 1);
            map.put("client-authentication", 2);
            map.put("no-authentication", 3);
            switch (map.get(sslMode)) {
                case 1:
                    mongoClient = MongoUtil.initCredentialSSLMongoClient(this.readerSliceConfig,
                      userName,password,authDb,trustStorePath,trustStorePwd,keyStorePath,keyStorePwd);
                    break;
                case 2:
                    mongoClient = MongoUtil.initCredentialClientAuthenticationMongoClient(this.readerSliceConfig,
                      userName,password,authDb,keyStorePath,keyStorePwd);
                    break;
                case 3:
                    mongoClient = MongoUtil.initCredentialNoAuthenticationMongoClient(this.readerSliceConfig,
                      userName,password,authDb);
                    break;
                default:
                    if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                        mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,authDb);
                    } else {
                        mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
                    }
                    break;
            }
            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
        }

        @Override
        public void destroy() {

        }

    }
}
