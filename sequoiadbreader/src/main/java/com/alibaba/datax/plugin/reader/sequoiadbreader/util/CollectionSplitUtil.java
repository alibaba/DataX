package com.alibaba.datax.plugin.reader.sequoiadbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.sequoiadbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.sequoiadbreader.SequoiaDBReaderErrorCode;
import com.google.common.base.Strings;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

public class CollectionSplitUtil {

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, Sequoiadb sequoiaDBClient) {
        List<Configuration> confList = new ArrayList<Configuration>();

        String collectionSpaceName = originalSliceConfig.getString(KeyConstant.SDB_COLLECTION_SPACE_NAME);
        String collectionName = originalSliceConfig.getString(KeyConstant.SDB_COLLECTION_NAME);

        if(Strings.isNullOrEmpty(collectionSpaceName) || Strings.isNullOrEmpty(collectionName)
                || sequoiaDBClient == null) {
            throw DataXException.asDataXException(SequoiaDBReaderErrorCode.ILLEGAL_VALUE,
                    SequoiaDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
        }

        List<Range> rangeList = doSplitCollection(adviceNumber, sequoiaDBClient, collectionSpaceName, collectionName);
        for(Range range : rangeList) {
            Configuration conf = originalSliceConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, range.lowerBound);
            conf.set(KeyConstant.UPPER_BOUND, range.upperBound);
            confList.add(conf);
        }
        return confList;
    }

    private static List<Range> doSplitCollection(int adviceNumber, Sequoiadb sequoiaDBClient, String collectionSpaceName, String collectionName) {
        List<Range> rangeList = new ArrayList<Range>();
        if (adviceNumber == 1) {
            Range range = new Range();
            range.lowerBound = "min";
            range.upperBound = "max";
            rangeList.add(range);
            return rangeList;
        }

        long docCount = sequoiaDBClient.getCollectionSpace(collectionSpaceName).getCollection(collectionName).getCount();
        if (docCount == 0) {
            return rangeList;
        }
        long splitPointCount = adviceNumber - 1;
        long chunkDocCount = docCount / adviceNumber;
        ArrayList<Object> splitPoints = new ArrayList<Object>();

        long skipCount = chunkDocCount;
        DBCollection cl = sequoiaDBClient.getCollectionSpace(collectionSpaceName).getCollection(collectionName);

        BSONObject orderBy = new BasicBSONObject();
        orderBy.put("_id",1);
        for (int i = 0; i < splitPointCount; i++) {
            DBCursor cursor = cl.query(null,null,orderBy,null,skipCount,1);
            if(cursor.hasNext()) {
                ObjectId id = (ObjectId) cursor.getNext().get("_id");
                splitPoints.add(id);
            }
            skipCount += chunkDocCount;
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

class Range {
    Object lowerBound;
    Object upperBound;
}
