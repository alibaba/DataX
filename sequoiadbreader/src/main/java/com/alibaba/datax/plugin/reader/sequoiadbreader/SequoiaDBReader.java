package com.alibaba.datax.plugin.reader.sequoiadbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.sequoiadbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.sequoiadbreader.util.SequoiaDBUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.DBQuery;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

import java.util.*;

public class SequoiaDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private Sequoiadb sequoiaDBClient = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,sequoiaDBClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String userName = originalConfig.getString(KeyConstant.SDB_USERNAME);
            String password = originalConfig.getString(KeyConstant.SDB_PASSWORD);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                sequoiaDBClient = SequoiaDBUtil.initAuthenticationSDBClient(originalConfig, userName, password);
            } else {
                sequoiaDBClient = SequoiaDBUtil.initSDBClient(originalConfig);
            }
        }

        @Override
        public void destroy() {
            sequoiaDBClient.close();
        }
    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private Sequoiadb sequoiaDBClient = null;
        private String userName = null;
        private String password = null;
        private String collectionSpace = null;
        private String collection = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private JSONArray sequoiadbColumnMeta = null;

        @Override
        public void startRead(RecordSender recordSender) {

            if(lowerBound == null || upperBound == null
                    ||sequoiaDBClient== null || userName == null ||
                    password == null || collectionSpace == null ||
                    collection == null  || sequoiadbColumnMeta == null) {
                throw DataXException.asDataXException(SequoiaDBReaderErrorCode.ILLEGAL_VALUE,
                        SequoiaDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            DBCollection cl = sequoiaDBClient.getCollectionSpace(collectionSpace).getCollection(collection);

            BasicBSONList condition = new BasicBSONList();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    BSONObject upperCondition = new BasicBSONObject();
                    BSONObject upperRange = new BasicBSONObject();
                    upperRange.put("$lt", upperBound);
                    upperCondition.put("_id",upperRange);
                    condition.add(upperCondition);
                }
            } else if (upperBound.equals("max")) {
                BSONObject lowCondition = new BasicBSONObject();
                BSONObject lowRange = new BasicBSONObject();
                lowRange.put("$gte", lowerBound);
                lowCondition.put("_id",lowRange);
                condition.add(lowCondition);
            } else {
                BSONObject lowCondition = new BasicBSONObject();
                BSONObject lowRange = new BasicBSONObject();
                lowRange.put("$gte", lowerBound);
                lowCondition.put("_id",lowRange);
                BSONObject upperCondition = new BasicBSONObject();
                BSONObject upperRange = new BasicBSONObject();
                upperRange.put("$lt", upperBound);
                upperCondition.put("_id",upperRange);
                condition.add(lowCondition);
                condition.add(upperCondition);
            }

            BSONObject filter = new BasicBSONObject();
            filter.put("$and", condition);
            DBQuery query = new DBQuery();
            query.setMatcher(filter);

            DBCursor cursor = cl.query(query);
            while(cursor.hasNext()) {
                BSONObject item = cursor.getNext();
                Record record = recordSender.createRecord();
                for (Object iterator : sequoiadbColumnMeta) {
                    JSONObject column = (JSONObject)iterator;
                    Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    if (tempCol == null) {
                        record.addColumn(new StringColumn(null));
                    } else if (tempCol instanceof Double) {
                        record.addColumn(new DoubleColumn((Double) tempCol));
                    } else if (tempCol instanceof Boolean) {
                        record.addColumn(new BoolColumn((Boolean) tempCol));
                    } else if (tempCol instanceof Date) {
                        record.addColumn(new DateColumn((Date) tempCol));
                    } else if (tempCol instanceof Integer) {
                        record.addColumn(new LongColumn((Integer) tempCol));
                    } else if (tempCol instanceof Long) {
                        record.addColumn(new LongColumn((Long) tempCol));
                    } else {
                        if (KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                            if (Strings.isNullOrEmpty(splitter)) {
                                throw DataXException.asDataXException(SequoiaDBReaderErrorCode.ILLEGAL_VALUE,
                                        SequoiaDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                            } else {
                                ArrayList array = (ArrayList) tempCol;
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
            this.userName = readerSliceConfig.getString(KeyConstant.SDB_USERNAME);
            this.password = readerSliceConfig.getString(KeyConstant.SDB_PASSWORD);
            this.collectionSpace = readerSliceConfig.getString(KeyConstant.SDB_COLLECTION_SPACE_NAME);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                sequoiaDBClient = SequoiaDBUtil.initAuthenticationSDBClient(readerSliceConfig, userName, password);
            } else {
                sequoiaDBClient = SequoiaDBUtil.initSDBClient(readerSliceConfig);
            }
            this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
            this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
            this.collection = readerSliceConfig.getString(KeyConstant.SDB_COLLECTION_NAME);
            this.sequoiadbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.SDB_COLUMN));
        }

        @Override
        public void destroy() {
            sequoiaDBClient.close();
        }
    }
}
