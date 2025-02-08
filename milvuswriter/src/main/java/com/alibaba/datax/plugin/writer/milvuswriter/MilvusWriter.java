package com.alibaba.datax.plugin.writer.milvuswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MilvusWriter extends Writer {
    public static class Job extends Writer.Job {
        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            originalConfig.getNecessaryValue(KeyConstant.ENDPOINT, MilvusWriterErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(KeyConstant.COLUMN, MilvusWriterErrorCode.REQUIRED_VALUE);
            originalConfig.getNecessaryValue(KeyConstant.COLLECTION, MilvusWriterErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {
            //collection create process
            MilvusClient milvusClient = new MilvusClient(originalConfig);
            try {
                MilvusCreateCollection milvusCreateCollection = new MilvusCreateCollection(originalConfig);
                milvusCreateCollection.createCollectionByMode(milvusClient);
                String collection = originalConfig.getString(KeyConstant.COLLECTION);
                String partition = originalConfig.getString(KeyConstant.PARTITION);
                if (partition != null && !milvusClient.hasPartition(collection, partition)) {
                    log.info("collection[{}] not contain partition[{}],try to create partition", collection, partition);
                    milvusClient.createPartition(collection, partition);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(MilvusWriterErrorCode.MILVUS_COLLECTION, e.getMessage(), e);
            } finally {
                milvusClient.close();
            }
        }

        /**
         * 切分任务。<br>
         *
         * @param mandatoryNumber 为了做到Reader、Writer任务数对等，这里要求Writer插件必须按照源端的切分数进行切分。否则框架报错！
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {

        private MilvusBufferWriter milvusBufferWriter;
        MilvusClient milvusClient;

        @Override
        public void init() {
            log.info("Initializing Milvus writer");
            // get configuration
            Configuration writerSliceConfig = this.getPluginJobConf();
            this.milvusClient = new MilvusClient(writerSliceConfig);
            this.milvusBufferWriter = new MilvusBufferWriter(this.milvusClient, writerSliceConfig);
            log.info("Milvus writer initialized");
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                milvusBufferWriter.add(record, this.getTaskPluginCollector());
                if (milvusBufferWriter.needCommit()) {
                    log.info("begin committing data size[{}]", milvusBufferWriter.getDataCacheSize());
                    milvusBufferWriter.commit();
                }
            }
            if (milvusBufferWriter.getDataCacheSize() > 0) {
                log.info("begin committing data size[{}]", milvusBufferWriter.getDataCacheSize());
                milvusBufferWriter.commit();
            }
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void destroy() {
            if (this.milvusClient != null) {
                this.milvusClient.close();
            }
        }
    }
}