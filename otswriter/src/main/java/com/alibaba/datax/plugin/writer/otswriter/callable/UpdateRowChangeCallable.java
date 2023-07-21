package com.alibaba.datax.plugin.writer.otswriter.callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.alicloud.openservices.tablestore.model.UpdateRowResponse;

import java.util.concurrent.Callable;

public class UpdateRowChangeCallable implements Callable<UpdateRowResponse>{
    
    private SyncClientInterface ots = null;
    private UpdateRowRequest updateRowRequest  = null;

    public UpdateRowChangeCallable(SyncClientInterface ots, UpdateRowRequest updateRowRequest ) {
        this.ots = ots;
        this.updateRowRequest = updateRowRequest;
    }
    
    @Override
    public UpdateRowResponse call() throws Exception {
        return ots.updateRow(updateRowRequest);
    }

}
