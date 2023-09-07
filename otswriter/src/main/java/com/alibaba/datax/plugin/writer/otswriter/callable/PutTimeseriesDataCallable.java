package com.alibaba.datax.plugin.writer.otswriter.callable;

import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;

import java.util.concurrent.Callable;

public class PutTimeseriesDataCallable implements Callable<PutTimeseriesDataResponse> {
    private TimeseriesClient client = null;
    private PutTimeseriesDataRequest putTimeseriesDataRequest = null;

    public PutTimeseriesDataCallable(TimeseriesClient client, PutTimeseriesDataRequest putTimeseriesDataRequest) {
        this.client = client;
        this.putTimeseriesDataRequest = putTimeseriesDataRequest;
    }

    @Override
    public PutTimeseriesDataResponse call() throws Exception {
        return client.putTimeseriesData(putTimeseriesDataRequest);
    }
}
