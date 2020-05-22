package com.alibaba.datax.plugin.writer.rediswriter.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.Key;
import com.alibaba.datax.plugin.writer.rediswriter.RedisWriteAbstract;
import org.apache.commons.lang3.StringUtils;

/**
 * @author lijf@2345.com
 * @date 2020/5/20 13:39
 * @desc
 */
public class DeleteWriter extends RedisWriteAbstract {
    // 要删除的hash的域名称
    String hashFileds;
    Integer keyIndex;
    String strKey;

    public DeleteWriter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void checkAndGetParams() {
        Configuration detailConfig = configuration.getConfiguration(Key.CONFIG);
        String colKey = detailConfig.getString(Key.COLKEY, null);
        this.strKey = detailConfig.getString(Key.STRING_KEY, null);
        if ((StringUtils.isBlank(colKey)) && StringUtils.isBlank(strKey)) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "strKey或colKey不能为空！请检查配置");
        }
        if (StringUtils.isNotBlank(colKey)) {
            this.keyIndex = detailConfig.getConfiguration(Key.COLKEY).getInt(Key.COL_INDEX);
        }
        hashFileds = detailConfig.getString(Key.HASH_DELETE_FILEDS, null);

    }

    @Override
    public void addToPipLine(RecordReceiver lineReceiver) {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            String key = record.getColumn(keyIndex).asString();
            String redisKey = keyPreffix + key + keySuffix;
            if (null != hashFileds) {
                String[] fileds = hashFileds.split(",");
                pipelined.hdel(redisKey, fileds);
            } else {
                pipelined.del(redisKey);
            }
            records++;
        }
    }
}
