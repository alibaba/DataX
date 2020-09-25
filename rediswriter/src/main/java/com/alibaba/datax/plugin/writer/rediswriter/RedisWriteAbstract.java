package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.PipelineBase;


/**
 * @author lijf@2345.com
 * @date 2020/5/19 15:56
 * @desc 写redis对象抽象类
 */
public abstract class RedisWriteAbstract {
    protected Configuration configuration;
    protected PipelineBase pipelined;
    Object redisClient;
    protected int records;
    protected String keyPreffix;
    protected String keySuffix;
    protected String valuePreffix;
    protected String valueSuffix;
    protected Integer batchSize;
    protected Integer expire;
    protected String strKey;
    protected Integer keyIndex;
    protected Integer valueIndex;

    public RedisWriteAbstract(Configuration configuration) {
        this.configuration = configuration;
    }


    public PipelineBase getRedisPipelineBase(Configuration configuration) {
        String mode = configuration.getNecessaryValue(Key.REDISMODE, CommonErrorCode.CONFIG_ERROR);
        String addr = configuration.getNecessaryValue(Key.ADDRESS, CommonErrorCode.CONFIG_ERROR);
        String auth = configuration.getString(Key.AUTH);
        if (Constant.CLUSTER.equalsIgnoreCase(mode)) {
            redisClient = RedisWriterHelper.getJedisCluster(addr, auth);
        } else {
            redisClient = RedisWriterHelper.getJedis(addr, auth);
        }
        return RedisWriterHelper.getPipeLine(redisClient);
    }

    /**
     * 初始化公共参数
     */
    public void initCommonParams() {
        Configuration detailConfig = this.configuration.getConfiguration(Key.CONFIG);
        batchSize = detailConfig.getInt(Key.BATCH_SIZE, 1000);
        keyPreffix = detailConfig.getString(Key.KEY_PREFIX, "");
        keySuffix = detailConfig.getString(Key.KEY_SUFFIX, "");
        valuePreffix = detailConfig.getString(Key.VALUE_PREFIX, "");
        valueSuffix = detailConfig.getString(Key.VALUE_SUFFIX, "");
        expire = detailConfig.getInt(Key.EXPIRE, Integer.MAX_VALUE);
        pipelined = getRedisPipelineBase(configuration);
    }

    /**
     * 检查和解析参数
     */
    public void checkAndGetParams() {
        Configuration detailConfig = configuration.getConfiguration(Key.CONFIG);

        String colKey = detailConfig.getString(Key.COLKEY, null);
        String strKey = detailConfig.getString(Key.STRING_KEY, null);

        if ((StringUtils.isBlank(colKey) && StringUtils.isBlank(strKey))) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "strKey或colKey不能为空！请检查配置");
        }
        if ((StringUtils.isNotBlank(colKey) && StringUtils.isNotBlank(strKey))) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "strKey或colKey不能同时存在！请检查配置");
        }

        if (StringUtils.isNotBlank(colKey)) {
            keyIndex = detailConfig.getConfiguration(Key.COLKEY).getInt(Key.COL_INDEX);
        } else {
            this.strKey = strKey;
        }
        String colValue = detailConfig.getString(Key.COLVALUE, null);
        if (StringUtils.isNotBlank(colKey) && StringUtils.isBlank(colValue)) {
            throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "colValue不能为空！请检查配置");
        }
        String writeType = configuration.getString(Key.WRITE_TYPE);
        // hash类型的colValue配置里面有多个column，要考虑排除获取valueIndex，HashTypeWriter子类单独处理
        if (!Constant.WRITE_TYPE_HASH.equalsIgnoreCase(writeType)) {
            valueIndex = detailConfig.getConfiguration(Key.COLVALUE).getInt(Key.COL_INDEX);
        }
    }

    /**
     * 把数据add到pipeline
     *
     * @param lineReceiver PipelineBase
     */
    public abstract void addToPipLine(RecordReceiver lineReceiver);


    /**
     * 正式写入数据到redis
     */
    public void syscData() {
        if (records >= batchSize) {
            RedisWriterHelper.syscData(pipelined);
            records = 0;
        }
    }


    public void syscAllData() {
        RedisWriterHelper.syscData(pipelined);
    }

    /**
     * 关闭资源
     */
    public void close() {
        RedisWriterHelper.syscData(pipelined);
        RedisWriterHelper.close(redisClient);
    }
}
