package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.RdbmsRangeSplitWrap;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
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
     * 在针对几百G到4T的mongodb表时，切片时间难以忍受，基本上无法进行切片
     * 改进后的切片算法，优先根据objectid进行切片，如果objectid不能切片，那么会根据原始的切片算法进行切片，但是会加过滤条件
     * mongodb的标准objectid由时间戳+机器码+随机数构成，而objectid使用时间戳进行排序
     * 因此可以根据objectid里面的时间戳（数字）进行排序然后进行split，切片效率非常高
     * <p>
     * 排序参考: https://github.com/mongodb/mongo/blob/master/src/mongo/bson/oid.h
     * The timestamp and counter are big endian (in contrast to the rest of BSON) because
     * we use memcmp to order OIDs, and we want to ensure an increasing order.
     */
    private static List<Range> doSplitCollection(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId, String query) {

        // split by objectid
        if (isObjectId) {
            LOG.info("根据mongodb表的ObjectId进行切片划分");
            return doSplitCollectionByObjectid(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
        }
        // 原始的切片算法，在切片时，加了过滤条件
        return doSplitCollectionByNotObjectid(adviceNumber, mongoClient, dbName, collName, isObjectId, query);
    }


    // 根据objectid进行切片划分
    private static List<Range> doSplitCollectionByObjectid(int adviceNumber, MongoClient mongoClient,
                                                           String dbName, String collName, boolean isObjectId, String query) {

        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> col = database.getCollection(collName);


        Document filter = new Document();
        if (!Strings.isNullOrEmpty(query)) {
            Document queryFilter = Document.parse(query);
            filter = new Document("$and", Arrays.asList(filter, queryFilter));
        }

        Document firstDoc = col.find().filter(filter).limit(1).sort(Sorts.orderBy(Sorts.ascending(KeyConstant.MONGO_PRIMARY_ID))).filter(filter).limit(1).first();
        LOG.info("firstDoc is :"+firstDoc.toJson());

        Document lastDoc = col.find().filter(filter).limit(1).sort(Sorts.orderBy(Sorts.descending(KeyConstant.MONGO_PRIMARY_ID))).filter(filter).limit(1).first();
        LOG.info("lastDoc is :"+firstDoc.toJson());




     

        if (null == firstDoc || null == lastDoc) {
            return getOneSplit();
        }


        List<Range> rangeList = new ArrayList<Range>();

        // 将最早记录的时间戳-1秒，将最大的一条记录的时间戳值+1秒钟，避免漏掉记录
        long[] tempResult = RangeSplitUtil.doLongSplit(firstDoc.getObjectId(KeyConstant.MONGO_PRIMARY_ID).getTimestamp() - 1, lastDoc.getObjectId(KeyConstant.MONGO_PRIMARY_ID).getTimestamp() + 1, adviceNumber);


        Object lastObjectId = "";
        for (int i = 0; i < tempResult.length; i++) {

            String splitPoint = new ObjectId(new Date(tempResult[i] * 1000)).toHexString();

            if (i == 0) {
                lastObjectId = splitPoint;
                continue;
            }
            Range range = new Range();
            range.lowerBound = lastObjectId;
            range.upperBound = splitPoint;
            lastObjectId = splitPoint;
            rangeList.add(range);
        }

        for (Range thisRange : rangeList) {
            LOG.info("split result is ：" + thisRange.lowerBound + " --> " + thisRange.upperBound);
        }

        return rangeList;


    }


    // split the collection into multiple chunks, each chunk specifies a range
    private static List<Range> doSplitCollectionByNotObjectid(int adviceNumber, MongoClient mongoClient,
                                                              String dbName, String collName, boolean isObjectId, String query) {
        LOG.info("不根据mongodb表的ObjectId进行切片划分");
        MongoDatabase database = mongoClient.getDatabase(dbName);
        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            return getOneSplit();
        }
        long docCount = 0L;
        Document result = database.runCommand(new Document("collStats", collName));

        // Some collections return double value
        Object countObject = result.get("count");

        if (countObject instanceof Integer) {
            docCount = new Long((Integer) countObject);
        } else if (countObject instanceof Double) {
            docCount = ((Double) countObject).longValue();
            if (!new Double(docCount).equals((Double) countObject)) {
                String message = "Double cast to Long is Error:" + (Double) countObject;
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
            }
        }


        if (docCount == 0L) {
            return rangeList;
        }
        int avgObjSize = 1;
        Object avgObjSizeObj = result.get("avgObjSize");
        if (avgObjSizeObj instanceof Integer) {
            avgObjSize = ((Integer) avgObjSizeObj).intValue();
        } else if (avgObjSizeObj instanceof Double) {
            avgObjSize = ((Double) avgObjSizeObj).intValue();
        }
        int splitPointCount = adviceNumber - 1;
        long expectChunkDocCount = docCount / adviceNumber;
        int chunkDocCount=0;
        if((int)expectChunkDocCount != expectChunkDocCount){
            String message = "The split has too many records :"+expectChunkDocCount+". Please let the \"job.setting.speed.channel\" parameter exceed  "+adviceNumber;
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
        }else{
            chunkDocCount = (int)expectChunkDocCount;

        }


        ArrayList<Object> splitPoints = new ArrayList<Object>();

        // test if user has splitVector role(clusterManager)
        boolean supportSplitVector = true;
        try {
            database.runCommand(new Document("splitVector", dbName + "." + collName)
                    .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                    .append("force", true));
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == KeyConstant.MONGO_UNAUTHORIZED_ERR_CODE ||
                    e.getErrorCode() == KeyConstant.MONGO_ILLEGALOP_ERR_CODE) {
                supportSplitVector = false;
            }
        }

        if (supportSplitVector) {
            boolean forceMedianSplit = false;
            long maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
            //int maxChunkSize = (chunkDocCount - 1) * 2 * avgObjSize / (1024 * 1024);
            if (maxChunkSize < 1) {
                forceMedianSplit = true;
            }
            if (!forceMedianSplit) {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("maxChunkSize", maxChunkSize)
                        .append("maxSplitPoints", adviceNumber - 1));
            } else {
                result = database.runCommand(new Document("splitVector", dbName + "." + collName)
                        .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                        .append("force", true));
            }
            ArrayList<Document> splitKeys = result.get("splitKeys", ArrayList.class);

            for (int i = 0; i < splitKeys.size(); i++) {
                Document splitKey = splitKeys.get(i);
                Object id = splitKey.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId) id;
                    splitPoints.add(oid.toHexString());
                } else {
                    splitPoints.add(id);
                }
            }
        } else {
            int skipCount = chunkDocCount;
            MongoCollection<Document> col = database.getCollection(collName);
            Document queryFilter = Document.parse(query);
            for (int i = 0; i < splitPointCount; i++) {
                // add query
                Document doc = col.find(queryFilter).skip(skipCount).limit(chunkDocCount).first();
                Object id = doc.get(KeyConstant.MONGO_PRIMARY_ID);
                if (isObjectId) {
                    ObjectId oid = (ObjectId) id;
                    splitPoints.add(oid.toHexString());
                } else {
                    splitPoints.add(id);
                }
                skipCount += chunkDocCount;
            }
        }

        Object lastObjectId = "min";
        for (Object splitPoint : splitPoints) {
            Range range = new Range();
            range.lowerBound = lastObjectId;
            lastObjectId = splitPoint;
            range.upperBound = lastObjectId;
            rangeList.add(range);
        }
        Range range = new Range();
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


    public static void main(String[] args) {
        ObjectId objectId = new ObjectId();

//        timeStampToObjectId();

        long[] tempResult = RangeSplitUtil.doLongSplit(objectId.getTimestamp(), objectId.getTimestamp() + 1000, 4);
//        for(long a:tempResult){
//            System.out.println(a);
//        }

        byte[] array = objectId.toByteArray();
        for (byte a : array) {
//            System.out.println(a);
        }

//        System.out.println(objectId.toHexString());
        ObjectId objectId1 = new ObjectId(new Date(objectId.getTimestamp()));
        objectId = new ObjectId(new Date(objectId.getTimestamp() + 1000));

        byte i = -1;
        String s = "";


    }


}

class Range {
    Object lowerBound;
    Object upperBound;
}
