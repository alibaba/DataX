package com.alibaba.datax.plugin.reader.otsreader.callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;

import java.util.concurrent.Callable;

public class GetRangeCallable implements Callable<GetRangeResponse> {
    
    private SyncClientInterface ots;
    private RangeRowQueryCriteria criteria;
    
    public GetRangeCallable(SyncClientInterface ots, RangeRowQueryCriteria criteria) {
        this.ots = ots;
        this.criteria = criteria;
    }
    
    @Override
    public GetRangeResponse call() throws Exception {
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        return ots.getRange(request);
    }
}