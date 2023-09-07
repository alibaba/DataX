package com.alibaba.datax.plugin.writer.otswriter.callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

import java.util.concurrent.Callable;

public class BatchWriteRowCallable implements Callable<BatchWriteRowResponse>{
    
    private SyncClientInterface ots = null;
    private BatchWriteRowRequest batchWriteRowRequest = null;

    public BatchWriteRowCallable(SyncClientInterface ots,  BatchWriteRowRequest batchWriteRowRequest) {
        this.ots = ots;
        this.batchWriteRowRequest = batchWriteRowRequest;

    }
    
    @Override
    public BatchWriteRowResponse call() throws Exception {
        return ots.batchWriteRow(batchWriteRowRequest);
    }

}