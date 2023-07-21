package com.alibaba.datax.plugin.reader.otsreader.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.OTSFuture;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;

public class GetRangeCallableOld implements Callable<GetRangeResult> {
    
    private OTSClientAsync ots;
    private RangeRowQueryCriteria criteria;
    private OTSFuture<GetRangeResult> future;
    
    public GetRangeCallableOld(OTSClientAsync ots, RangeRowQueryCriteria criteria, OTSFuture<GetRangeResult> future) {
        this.ots = ots;
        this.criteria = criteria;
        this.future = future;
    }
    
    @Override
    public GetRangeResult call() throws Exception {
        try {
            return future.get();
        } catch (Exception e) {
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            future = ots.getRange(request);
            throw e;
        }
    }

}
