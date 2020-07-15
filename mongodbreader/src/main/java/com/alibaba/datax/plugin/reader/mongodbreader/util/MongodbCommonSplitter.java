package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongodbCommonSplitter {


    /**
     * 原始的切片算法 col.find().skip(skipCount).limit(chunkDocCount).first()
     * 在针对数据量非常大的mongodb表时，切片时间太长
     * 改进后的切片算法，加过滤条件
     */
    public static List<Range>  split(int adviceNumber, MongoClient mongoClient,
                                                 String dbName, String collName, boolean isObjectId, String query) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        if (adviceNumber == 1) {
            return CollectionSplitUtil.getOneSplit();
        }

        Document filter = new Document();
        if (StringUtils.isNotBlank(query)) {
            filter = Document.parse(query);
        }
        long docCount = database.getCollection(collName).countDocuments(filter);
        if (docCount == 0L) {
            return CollectionSplitUtil.getOneSplit();
        }


        MongoCollection<Document> col = database.getCollection(collName);
        List<Range> rangeList = new ArrayList<Range>();


        long expectChunkDocCount = docCount / adviceNumber;

        ArrayList<String> splitPoints = new ArrayList<String>();

        //i=0时，通过first()获取到的objectid并不是最小的，会丢数据，因此跳过
        for (int i = 1; i < adviceNumber; i++) {
            Document doc  = col.find(filter).skip((int) (i * expectChunkDocCount)).limit(1).iterator().next();
            Object objectId = doc.get(KeyConstant.MONGO_PRIMARY_ID);

            splitPoints.add(objectId.toString());



        }
        Collections.sort(splitPoints);
        Object lastObjectId = splitPoints.get(0).toString();

        Range range = new Range();
        range.lowerBound = "min";
        range.upperBound = lastObjectId;
        rangeList.add(range);

        for (int i = 1; i < splitPoints.size(); i++) {
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

}
