package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.Constant;
import com.alibaba.datax.plugin.writer.rediswriter.Key;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;
import org.apache.commons.lang3.StringUtils;

/**
 * @author lijf@2345.com
 * @date 2020/5/19 16:18
 * @desc list类型写redis
 */
public class ListTypeWriter extends RedisWriteAbstract {
    String pushType;
    String valueDelimiter;

    public ListTypeWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        super.checkAndGetParams();
        Configuration detailConfig = this.configuration.getConfiguration(Key.CONFIG);
        pushType = detailConfig.getString(Key.LIST_PUSH_TYPE, Constant.LIST_PUSH_TYPE_OVERWRITE);
        valueDelimiter = detailConfig.getString(Key.LIST_VALUE_DELIMITER);
        if (StringUtils.isBlank(valueDelimiter)) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "valueDelimiter不能为空！请检查配置");
        }
        if (!Constant.LIST_PUSH_TYPE_LPUSH.equalsIgnoreCase(pushType) &&
                !Constant.LIST_PUSH_TYPE_RPUSH.equalsIgnoreCase(pushType) &&
                !Constant.LIST_PUSH_TYPE_OVERWRITE.equalsIgnoreCase(pushType)) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "pushType不合法！list类型只支持lpush，rpush，overwrite！请检查配置!pushType:" + pushType);
        }
    }

    @Override
    public void addToPipLine(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            String redisKey;
            String columnValue;
            if (null != keyIndex) {
                String key = record.getColumn(keyIndex).asString();
                redisKey = keyPreffix + key + keySuffix;
            } else {
                redisKey = keyPreffix + strKey + keySuffix;
            }
            columnValue = record.getColumn(valueIndex).asString();
            String[] redisValue = columnValue.split(valueDelimiter);
            switch (pushType) {
                case Constant.LIST_PUSH_TYPE_OVERWRITE:
                    pipelined.del(redisKey);
                    pipelined.rpush(redisKey, redisValue);
                    break;
                case Constant.LIST_PUSH_TYPE_RPUSH:
                    pipelined.rpush(redisKey, redisValue);
                    break;
                case Constant.LIST_PUSH_TYPE_LPUSH:
                    pipelined.lpush(redisKey, redisValue);
                    break;
            }
            pipelined.expire(redisKey, expire);
            records++;
        }
    }
}
