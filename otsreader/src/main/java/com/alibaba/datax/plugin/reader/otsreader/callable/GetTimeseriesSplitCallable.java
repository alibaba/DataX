package com.alibaba.datax.plugin.reader.otsreader.callable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.model.timeseries.SplitTimeseriesScanTaskRequest;
import com.alicloud.openservices.tablestore.model.timeseries.SplitTimeseriesScanTaskResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesScanSplitInfo;

import java.util.List;
import java.util.concurrent.Callable;

public class GetTimeseriesSplitCallable implements Callable<List<TimeseriesScanSplitInfo>> {

    private TimeseriesClient client = null;
    private String timeseriesTableName = null;
    private String measurementName = null;
    private int splitCountHint = 1;


    public GetTimeseriesSplitCallable(SyncClientInterface ots, String timeseriesTableName, String measurementName, int splitCountHint) {
        this.client = ((SyncClient) ots).asTimeseriesClient();
        this.timeseriesTableName = timeseriesTableName;
        this.measurementName = measurementName;
        this.splitCountHint = splitCountHint;
    }

    @Override
    public List<TimeseriesScanSplitInfo> call() throws Exception {
        SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(timeseriesTableName, splitCountHint);
        if (measurementName.length() != 0) {
            request.setMeasurementName(measurementName);
        }

        SplitTimeseriesScanTaskResponse response = client.splitTimeseriesScanTask(request);
        return response.getSplitInfos();
    }
}
