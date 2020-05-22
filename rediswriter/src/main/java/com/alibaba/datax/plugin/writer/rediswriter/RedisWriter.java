package com.alibaba.datax.plugin.writer.rediswriter;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.rediswriter.writer.DeleteWriter;
import com.alibaba.datax.plugin.writer.rediswriter.writer.HashTypeWriter;
import com.alibaba.datax.plugin.writer.rediswriter.writer.ListTypeWriter;
import com.alibaba.datax.plugin.writer.rediswriter.writer.StringTypeWriter;

import java.util.ArrayList;
import java.util.List;

public class RedisWriter extends Writer {
    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originalConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            RedisWriterHelper.checkConnection(originalConfig);
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        RedisWriteAbstract wirter;

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            wirter.addToPipLine(lineReceiver);
            wirter.syscData();
        }

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            String writeType = taskConfig.getString(Key.WRITE_TYPE);
            String writeMode = taskConfig.getString(Key.WRITE_MODE);
            // 判断是delete还是insert
            if (Constant.WRITE_MODE_DELETE.equalsIgnoreCase(writeMode)) {
                wirter = new DeleteWriter(taskConfig);
            } else {
                // 判断写redis的数据类型，string，list，hash
                switch (writeType) {
                    case Constant.WRITE_TYPE_HASH:
                        wirter = new HashTypeWriter(taskConfig);
                        break;
                    case Constant.WRITE_TYPE_LIST:
                        wirter = new ListTypeWriter(taskConfig);
                        break;
                    case Constant.WRITE_TYPE_STRING:
                        wirter = new StringTypeWriter(taskConfig);
                        break;
                    default:
                        throw DataXException.asDataXException(CommonErrorCode.CONFIG_ERROR, "rediswriter 不支持此数据类型:" + writeType);
                }

            }
            wirter.checkAndGetParams();
            wirter.initCommonParams();
        }

        @Override
        public void destroy() {
            wirter.syscAllData();
            wirter.close();
        }
    }
}
