package com.alibaba.datax.plugin.writer.otswriter.callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;

import java.util.concurrent.Callable;

public class PutRowChangeCallable implements Callable<PutRowResponse>{
    
    private SyncClientInterface ots = null;
    private PutRowRequest putRowRequest = null;

    public PutRowChangeCallable(SyncClientInterface ots, PutRowRequest putRowRequest) {
        this.ots = ots;
        this.putRowRequest = putRowRequest;
    }
    
    @Override
    public PutRowResponse call() throws Exception {
        return ots.putRow(putRowRequest);
    }

}