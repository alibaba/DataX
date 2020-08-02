package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 对支持随机采样功能的mongodb表进行切片
 * mongodb 3.2 version新增sample随机采样功能
 */
public class MongodbSampleSplitter extends MongodbCommonSplitter {
    private static final Logger LOG = LoggerFactory
            .getLogger(MongodbSampleSplitter.class);


    public static List<Range> split(int adviceNumber, MongoClient mongoClient,
                                    String dbName, String collName, boolean isObjectId, String query) {
        MongoDatabase database = mongoClient.getDatabase(dbName);

        if (adviceNumber <= 1) {
            return CollectionSplitUtil.getOneSplit();
        }
        Document filter = new Document();
        if (StringUtils.isNotBlank(query)) {
            filter = Document.parse(query);
        }

        // 随机采样取adviceNumber个元素，可能会有重复值。因此取3 * adviceNumber个元素
        BasicDBObject basicDBObject = new BasicDBObject("$sample", new BasicDBObject("size", 3 * adviceNumber));

        Document idDoc = new Document();
        idDoc.put(KeyConstant.MONGO_PRIMARY_ID, 1);
        BasicDBObject project = new BasicDBObject("$project", idDoc);
        BasicDBObject sort = new BasicDBObject("$sort", idDoc);

        ArrayList<Bson> list = new ArrayList<Bson>();

        Bson match = Aggregates.match(filter
        );


        list.add(match);

        list.add(basicDBObject);
        list.add(project);
        list.add(sort);
        AggregateIterable<Document> aggResult;

        try {
            aggResult = database.getCollection(collName).aggregate(list);

        } catch (MongoException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION, "不支持Sample采样，请确认mongodb版本大于等于3.2", e);
        }


        List<Range> splits = new ArrayList<Range>(adviceNumber);
        MongoCursor<Document> iterator = aggResult.iterator();
        ArrayList<String> sampleObjectIdList = new ArrayList<String>(adviceNumber);
        while (iterator.hasNext()) {
            Document next = iterator.next();
            String objectId = next.get(KeyConstant.MONGO_PRIMARY_ID).toString();
            sampleObjectIdList.add(objectId);
        }


        long[] indexArr = RangeSplitUtil.doLongSplit(0L, new Long(sampleObjectIdList.size() - 1), adviceNumber);





        String lastObjectId = null;
        for (int i = 1; i < indexArr.length - 1; i++) {
            String splitPoint = sampleObjectIdList.get((int) (indexArr[i])).toString();
            if (i == 1) {
                Range lowerRange = new Range();
                lowerRange.setLowerBound("min");
                lowerRange.setUpperBound(splitPoint);
                splits.add(lowerRange);
            }else{
                Range range = new Range();
                range.setLowerBound(lastObjectId);
                range.setUpperBound(splitPoint);
                splits.add(range);
            }

            lastObjectId = splitPoint;


        }

        Range upperRange = new Range();
        upperRange.setUpperBound("max");
        upperRange.setLowerBound(lastObjectId);
        splits.add(upperRange);


        return splits;
    }


}
