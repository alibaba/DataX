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
     * 原始的切片算法 col.find().skip(skipCount).limit(chunkDocCount).first()
     * 在针对数据量非常大的mongodb表时，切片时间难以忍受，基本上无法进行切片
     * 改进后的切片算法，加过滤条件,并且在头部去掉min值，减少Reader.Task过滤的数据范围
     */
    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId, String query) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        if (adviceNumber == 1) {
            return getOneSplit();
        }

        Document filter = new Document();
        if (StringUtils.isNotBlank(query)) {
            filter = Document.parse(query);
        }
        long docCount = database.getCollection(collName).count(filter);

        if (docCount == 0L) {
            return getOneSplit();
        }


        MongoCollection<Document> col = database.getCollection(collName);
        List<Range> rangeList = new ArrayList<Range>();


        long expectChunkDocCount = docCount / adviceNumber;

        ArrayList<Object> splitPoints = new ArrayList<Object>();

        //i=0时，通过first()获取到的objectid并不是最小的，会丢数据，因此跳过
        for (int i = 1; i < adviceNumber; i++) {
            Document doc = col.find(filter).skip((int) (i * expectChunkDocCount)).limit((int) expectChunkDocCount).first();
            Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID).toString();
            splitPoints.add(id);

        }

        Object lastObjectId = splitPoints.get(0).toString();
        Range range = new Range();
        range.lowerBound = "min";
        range.upperBound = lastObjectId;
        rangeList.add(range);

        for(int i=1;i<splitPoints.size();i++){
            String splitPoint = splitPoints.get(i).toString();

            range = new Range();
            range.lowerBound = lastObjectId;
            lastObjectId = splitPoint;
            range.upperBound = lastObjectId;
            rangeList.add(range);
        }

        range = new Range();
        range.lowerBound = lastObjectId;
        range.upperBound = "max";
        rangeList.add(range);




        return rangeList;
    }

    private static List<Range> getOneSplit() {
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
