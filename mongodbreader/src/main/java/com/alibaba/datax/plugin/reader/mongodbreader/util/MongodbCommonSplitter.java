package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MongodbCommonSplitter {

    private static final Logger LOG = LoggerFactory
            .getLogger(MongodbCommonSplitter.class);
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

        // 这个地方需要对long转成int进行判断，是不是丢失精度
        long expectChunkDocCount =(int)(docCount / adviceNumber) ;
        if((int)expectChunkDocCount != expectChunkDocCount){
            String message = "The split has too many records :"+expectChunkDocCount+". Please let the \"job.setting.speed.channel\" parameter exceed  "+adviceNumber;
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE, message);
        }


        LOG.info("每个切片期望的文档个数是:{}",expectChunkDocCount);
        ArrayList<String> splitPoints = new ArrayList<String>();

        Range lastRange=null;
        for (int i = 0; i < adviceNumber; i++) {
            Range range = new Range();
            range.setSkip((int) (i * expectChunkDocCount));
            range.setLimit((int)expectChunkDocCount);
            range.setSampleType(false);
            rangeList.add(range);
            lastRange=range;
        }
        int totalSkipDoc=(lastRange.getSkip()+lastRange.getLimit());
        if(totalSkipDoc<docCount){
            lastRange.setLimit(lastRange.getLimit()+(int)(docCount-totalSkipDoc));
        }

        return rangeList;
    }

}
