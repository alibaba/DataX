package com.alibaba.datax.plugin.reader.otsreader.callable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesScanSplitInfo;

import java.util.List;
import java.util.concurrent.Callable;

public class ScanTimeseriesDataCallable implements Callable<ScanTimeseriesDataResponse> {

    private TimeseriesClient client = null;
    private ScanTimeseriesDataRequest request = null;

    public ScanTimeseriesDataCallable(SyncClientInterface ots, ScanTimeseriesDataRequest scanTimeseriesDataRequest){
        this.client = ((SyncClient) ots).asTimeseriesClient();
        this.request = scanTimeseriesDataRequest;
    }

    @Override
    public ScanTimeseriesDataResponse call() throws Exception {
        return client.scanTimeseriesData(request);
    }
}
