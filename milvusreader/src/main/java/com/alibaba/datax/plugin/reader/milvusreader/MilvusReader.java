package com.alibaba.datax.plugin.reader.milvusreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public class MilvusReader extends Reader {
    public static class Job extends Reader.Job {
        private Configuration originalConfig = null;
        /**
         * 切分任务。<br>
         *
         * @param mandatoryNumber 为了做到Reader、Writer任务数对等，这里要求Writer插件必须按照源端的切分数进行切分。否则框架报错！
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configList = new ArrayList<Configuration>();
            for(int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }
    public static class Task extends Reader.Task {

        private MilvusClientV2 milvusClientV2;

        private MilvusSourceConverter milvusSourceConverter;

        private String collection = null;
        private String partition = null;
        private Integer batchSize;

        private CreateCollectionReq.CollectionSchema collectionSchema;

        @Override
        public void init() {
            log.info("Initializing Milvus writer");
            // get configuration
            Configuration writerSliceConfig = this.getPluginJobConf();
            this.collection = writerSliceConfig.getString(KeyConstant.COLLECTION);
            this.partition = writerSliceConfig.getString(KeyConstant.PARTITION, null);
            this.batchSize = writerSliceConfig.getInt(KeyConstant.BATCH_SIZE, 100);
            log.info("Collection:{}", this.collection);
            // connect to milvus
            ConnectConfig connectConfig = ConnectConfig.builder()
                    .uri(writerSliceConfig.getString(KeyConstant.URI))
                    .token(writerSliceConfig.getString(KeyConstant.TOKEN))
                    .build();
            if(writerSliceConfig.getString(KeyConstant.DATABASE) == null) {
                log.warn("Database is set, using database{}", writerSliceConfig.getString(KeyConstant.DATABASE));
                connectConfig.setDbName(writerSliceConfig.getString(KeyConstant.DATABASE));
            }
            this.milvusClientV2 = new MilvusClientV2(connectConfig);
            this.milvusSourceConverter = new MilvusSourceConverter();
            log.info("Milvus writer initialized");
        }
        @Override
        public void prepare() {
            super.prepare();
            Boolean hasCollection = milvusClientV2.hasCollection(HasCollectionReq.builder().collectionName(collection).build());
            if (!hasCollection) {
                log.error("Collection {} does not exist", collection);
                throw new RuntimeException("Collection does not exist");
            }
            DescribeCollectionReq describeCollectionReq = DescribeCollectionReq.builder()
                            .collectionName(collection)
                                    .build();
            DescribeCollectionResp describeCollectionResp = milvusClientV2.describeCollection(describeCollectionReq);
            this.collectionSchema = describeCollectionResp.getCollectionSchema();
        }

        @Override
        public void destroy() {
            log.info("Closing Milvus reader, closing connection");
            this.milvusClientV2.close();
        }

        @Override
        public void startRead(RecordSender recordSender) {
            QueryIteratorReq queryIteratorReq = QueryIteratorReq.builder()
                            .collectionName(collection)
                            .outputFields(Collections.singletonList("*"))
                            .batchSize(batchSize)
                            .build();
            if(partition != null) {
                queryIteratorReq.setPartitionNames(Collections.singletonList(partition));
            }
            QueryIterator queryIterator = milvusClientV2.queryIterator(queryIteratorReq);
            while (true){
                List<QueryResultsWrapper.RowRecord> rowRecords = queryIterator.next();
                if(rowRecords.isEmpty()){
                    break;
                }
                rowRecords.forEach(rowRecord -> {
                    Record record = recordSender.createRecord();
                    record = milvusSourceConverter.toDataXRecord(record, rowRecord, collectionSchema);
                    recordSender.sendToWriter(record);
                });
            }
        }
    }
}
