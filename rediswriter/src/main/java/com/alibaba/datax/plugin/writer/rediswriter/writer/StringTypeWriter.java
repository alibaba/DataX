package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;


/**
 * @author lijf@2345.com
 * @date 2020/5/19 16:15
 * @desc string类型写redis
 */
public class StringTypeWriter extends RedisWriteAbstract {


    public StringTypeWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        super.checkAndGetParams();
    }

    @Override
    public void addToPipLine(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            String redisKey;
            String redisValue;
            if (null != keyIndex) {
                String key = record.getColumn(keyIndex).asString();
                redisKey = keyPreffix + key + keySuffix;
                redisValue = record.getColumn(valueIndex).asString();
            } else {
                redisKey = keyPreffix + strKey + keySuffix;
                redisValue = record.getColumn(valueIndex).asString();
            }
            redisValue = valuePreffix + redisValue + valueSuffix;
            pipelined.set(redisKey, redisValue);
            pipelined.expire(redisKey, expire);
            records++;
        }
    }

}
