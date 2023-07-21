package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alibaba.datax.plugin.reader.otsreader.callable.GetRangeCallable;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.reader.otsreader.callable.GetTimeseriesSplitCallable;
import com.alibaba.datax.plugin.reader.otsreader.callable.ScanTimeseriesDataCallable;
import com.alibaba.datax.plugin.reader.otsreader.model.DefaultNoRetry;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesScanSplitInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OtsHelper {

    public static SyncClientInterface getOTSInstance(OTSConf conf) {
        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(conf.getIoThreadCount());
        clientConfigure.setMaxConnections(conf.getMaxConnectCount());
        clientConfigure.setSocketTimeoutInMillisecond(conf.getSocketTimeoutInMillisecond());
        clientConfigure.setConnectionTimeoutInMillisecond(conf.getConnectTimeoutInMillisecond());
        clientConfigure.setRetryStrategy(new DefaultNoRetry());

        SyncClient ots = new SyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName(),
                clientConfigure);


        Map<String, String> extraHeaders = new HashMap<String, String>();
        extraHeaders.put("x-ots-sdk-type", "public");
        extraHeaders.put("x-ots-request-source", "datax-otsreader");
        ots.setExtraHeaders(extraHeaders);

        return ots;
    }

    public static TableMeta getTableMeta(SyncClientInterface ots, String tableName, int retry, int sleepInMillisecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                retry,
                sleepInMillisecond
        );
    }

        public static GetRangeResponse getRange(SyncClientInterface ots, RangeRowQueryCriteria rangeRowQueryCriteria, int retry, int sleepInMillisecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetRangeCallable(ots, rangeRowQueryCriteria),
                retry,
                sleepInMillisecond
        );
    }

    public static List<TimeseriesScanSplitInfo> splitTimeseriesScan(SyncClientInterface ots, String tableName, String measurementName, int splitCountHint, int retry, int sleepInMillisecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTimeseriesSplitCallable(ots, tableName, measurementName, splitCountHint),
                retry,
                sleepInMillisecond
        );
    }

    public static ScanTimeseriesDataResponse scanTimeseriesData(SyncClientInterface ots, ScanTimeseriesDataRequest scanTimeseriesDataRequest, int retry, int sleepInMillisecond) throws Exception {
        return RetryHelper.executeWithRetry(
                new ScanTimeseriesDataCallable(ots, scanTimeseriesDataRequest),
                retry,
                sleepInMillisecond
        );
    }
}
