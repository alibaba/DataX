package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 * Modified by liukunyuan on 2020/6/22.
 */
public class CollectionSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(CollectionSplitUtil.class);


    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, MongoClient mongoClient, String query) {
        LOG.info("adviceNumber is :" + adviceNumber);
        List<Configuration> confList = new ArrayList<Configuration>();

        String dbName = originalSliceConfig.getString(KeyConstant.MONGO_DB_NAME, originalSliceConfig.getString(KeyConstant.MONGO_DATABASE));

        String collName = originalSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);

        if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(collName) || mongoClient == null) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                    MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        // empty collection
        if (null == doc) {
            List<Configuration> rangeList = new ArrayList<Configuration>();
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, "min");
            conf.set(KeyConstant.UPPER_BOUND, "max");
            conf.set(KeyConstant.IS_OBJECTID, false);
            rangeList.add(conf);
            return rangeList;
        }

        boolean isObjectId = isPrimaryIdObjectId(mongoClient, dbName, collName);

        List<Range> rangeList = doSplitCollection(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
        for (Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            conf.set(KeyConstant.IS_OBJECTID, isObjectId);
            confList.add(conf);
        }
        return confList;
    }


    private static boolean isPrimaryIdObjectId(MongoClient mongoClient, String dbName, String collName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);
        Document doc = col.find().limit(1).first();
        Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
        if (id instanceof ObjectId) {
            return true;
        }
        return false;
    }

    /**
     * 通过MongodbSampleSplitter、MongodbVectorSplitter和MongodbCommonSplitter等3个类按照优先顺序进行切片
     *
     * @param adviceNumber
     * @param mongoClient
     * @param dbName
     * @param collName
     * @param isObjectId
     * @param query
     * @return
     */
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId, String query) {

        List<Range> rangeList = null;
        long starttime = System.currentTimeMillis();
        long endtime = 0L;
        try {

            rangeList = MongodbSampleSplitter.split(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
            endtime = System.currentTimeMillis();
            LOG.info("成功通过Sample采样进行切片,切片操作耗时:{}毫秒", (endtime - starttime));

        } catch (Exception e) {
            rangeList = null;
            LOG.info(e.getMessage());
        }


        if (null == rangeList) {

            try {
                starttime = System.currentTimeMillis();
                rangeList = MongodbVectorSplitter.split(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
                endtime = System.currentTimeMillis();
                LOG.info("成功通过VectorSplit进行切片,切片操作耗时:{}毫秒", (endtime - starttime));

            } catch (Exception e) {
                rangeList = null;
                LOG.info(e.getMessage());
            }

        }

        if (null == rangeList) {
            starttime = System.currentTimeMillis();
            rangeList = MongodbCommonSplitter.split(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
            endtime = System.currentTimeMillis();
            LOG.info("成功通过CommonSplit进行切片,切片操作耗时:{}毫秒", (endtime - starttime));

        }

        return rangeList;
    }

    public static List<Range> getOneSplit() {
        Range range = new Range();
        range.lowerBound = "min";
        range.upperBound = "max";
        return Arrays.asList(range);
    }


    // bson转json格式设置
    public static JsonWriterSettings getJsonWriterSettings() {
        JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder()
                .dateTimeConverter((value, writer) -> writer.writeString(Long.toString(value)))
                .decimal128Converter((value, writer) -> writer.writeNumber(value.toString()))
                .objectIdConverter((value, writer) -> writer.writeString(value.toString()))
                .int32Converter((value, writer) -> writer.writeNumber(value.toString()))
                .int64Converter((value, writer) -> writer.writeString(Long.toString(value)))
                .build();

        return jsonWriterSettings;
    }


}

class Range {
    Object lowerBound;
    Object upperBound;
}
