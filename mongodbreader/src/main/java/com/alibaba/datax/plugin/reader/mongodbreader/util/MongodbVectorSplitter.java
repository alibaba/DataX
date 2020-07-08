package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

//对支持运行splitVector命令的mongodb表进行切片
public class MongodbVectorSplitter extends MongodbCommonSplitter {

    public static List<Range> split(int adviceNumber, MongoClient mongoClient,
                             String dbName, String collName, boolean isObjectId, String query) {
        MongoDatabase database = mongoClient.getDatabase(dbName);




        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            return CollectionSplitUtil.getOneSplit();
        }

        Document result = database.runCommand(new Document("collStats", collName));
        // Some collections return double value
        Object countObject = result.get("count");
        Long docCount = 0L;

        if (countObject instanceof Integer) {
            docCount = new Long((Integer) countObject);
        } else if (countObject instanceof Double) {
            docCount = ((Double) countObject).longValue();
            if (!new Double(docCount).equals((Double) countObject)) {
                String message = "Double cast to Long is Error:" + (Double) countObject;
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
            }
        }


        if (docCount <= adviceNumber ) {
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
        int chunkDocCount = 0;
        if ((int) expectChunkDocCount != expectChunkDocCount) {
            String message = "The split has too many records :" + expectChunkDocCount + ". Please let the \"job.setting.speed.channel\" parameter exceed  " + adviceNumber;
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
        } else {
            chunkDocCount = (int) expectChunkDocCount;

        }

        ArrayList<Object> splitPoints = new ArrayList<Object>();

        // test if user has splitVector role(clusterManager)
        boolean supportSplitVector = true;
        database.runCommand(new Document("splitVector", dbName + "." + collName)
                .append("keyPattern", new Document(KeyConstant.MONGO_PRIMARY_ID, 1))
                .append("force", true));

        boolean forceMedianSplit = false;
        long maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
//            int maxChunkSize = (docCount / splitPointCount - 1) * 2 * avgObjSize / (1024 * 1024);
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
}
