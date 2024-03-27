package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.OTSErrorCode;
import com.alibaba.datax.plugin.writer.otswriter.callable.PutTimeseriesDataCallable;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.LineAndError;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OTSTimeseriesRowTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(OTSTimeseriesRowTask.class);
    private TimeseriesClient client = null;
    private OTSConf conf = null;
    private List<OTSLine> otsLines = new ArrayList<OTSLine>();
    private boolean isDone = false;
    private int retryTimes = 0;

    public OTSTimeseriesRowTask(
            final TimeseriesClient client,
            final OTSConf conf,
            final List<OTSLine> lines
    ) {
        this.client = client;
        this.conf = conf;

        this.otsLines.addAll(lines);
    }

    @Override
    public void run() {
        LOG.debug("Begin run");
        sendAll(otsLines);
        LOG.debug("End run");
    }

    public boolean isDone() {
        return this.isDone;
    }

    private boolean isExceptionForSendOneByOne(TableStoreException ee) {
        if (ee.getErrorCode().equals(OTSErrorCode.INVALID_PARAMETER) ||
                ee.getErrorCode().equals(OTSErrorCode.REQUEST_TOO_LARGE)
        ) {
            return true;
        }
        return false;
    }

    private PutTimeseriesDataRequest createRequest(List<OTSLine> lines) {
        PutTimeseriesDataRequest newRequest = new PutTimeseriesDataRequest(conf.getTableName());
        for (OTSLine l : lines) {
            newRequest.addRow(l.getTimeseriesRow());
        }
        return newRequest;
    }

    /**
     * 单行发送数据
     *
     * @param line
     */
    public void sendLine(OTSLine line) {
        try {
            PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(conf.getTableName());
            putTimeseriesDataRequest.addRow(line.getTimeseriesRow());
            PutTimeseriesDataResponse result = RetryHelper.executeWithRetry(
                    new PutTimeseriesDataCallable(client, putTimeseriesDataRequest),
                    conf.getRetry(),
                    conf.getSleepInMillisecond());


            if (!result.isAllSuccess()){
                String errMsg = result.getFailedRows().get(0).getError().getMessage();
                LOG.warn("sendLine fail. " + errMsg);
                CollectorUtil.collect(line.getRecords(), errMsg);

            }else {
                LOG.debug("Request ID : {}", result.getRequestId());
            }

        } catch (Exception e) {
            LOG.warn("sendLine fail. ", e);
            CollectorUtil.collect(line.getRecords(), e.getMessage());
        }
    }

    private void sendAllOneByOne(List<OTSLine> lines) {
        for (OTSLine l : lines) {
            sendLine(l);
        }
    }

    private void sendAll(List<OTSLine> lines) {
        try {
            Thread.sleep(Common.getDelaySendMillinSeconds(retryTimes, conf.getSleepInMillisecond()));
            PutTimeseriesDataRequest putTimeseriesDataRequest = createRequest(lines);
            PutTimeseriesDataResponse result = RetryHelper.executeWithRetry(
                    new PutTimeseriesDataCallable(client, putTimeseriesDataRequest),
                    conf.getRetry(),
                    conf.getSleepInMillisecond());

            LOG.debug("Request ID : {}", result.getRequestId());
            List<LineAndError> errors = getLineAndError(result, lines);
            if (!errors.isEmpty()) {
                if (retryTimes < conf.getRetry()) {
                    retryTimes++;
                    LOG.warn("Retry times : {}", retryTimes);
                    List<OTSLine> newLines = new ArrayList<OTSLine>();
                    for (LineAndError re : errors) {
                        LOG.warn("Because: {}", re.getError().getMessage());
                        if (RetryHelper.canRetry(re.getError().getCode())) {
                            newLines.add(re.getLine());
                        } else {
                            LOG.warn("Can not retry, record row to collector. {}", re.getError().getMessage());
                            CollectorUtil.collect(re.getLine().getRecords(), re.getError().getMessage());
                        }
                    }
                    if (!newLines.isEmpty()) {
                        sendAll(newLines);
                    }
                } else {
                    LOG.warn("Retry times more than limitation. RetryTime : {}", retryTimes);
                    CollectorUtil.collect(errors);
                }
            }
        } catch (TableStoreException e) {
            LOG.warn("Send data fail. {}", e.getMessage());
            if (isExceptionForSendOneByOne(e)) {
                if (lines.size() == 1) {
                    LOG.warn("Can not retry.", e);
                    CollectorUtil.collect(e.getMessage(), lines);
                } else {
                    // 进入单行发送的分支
                    sendAllOneByOne(lines);
                }
            } else {
                LOG.error("Can not send lines to OTS for RuntimeException.", e);
                CollectorUtil.collect(e.getMessage(), lines);
            }
        } catch (Exception e) {
            LOG.error("Can not send lines to OTS for Exception.", e);
            CollectorUtil.collect(e.getMessage(), lines);
        }
    }

    private List<LineAndError> getLineAndError(PutTimeseriesDataResponse result, List<OTSLine> lines) throws OTSCriticalException {
        List<LineAndError> errors = new ArrayList<LineAndError>();

        List<PutTimeseriesDataResponse.FailedRowResult> status = result.getFailedRows();
        for (PutTimeseriesDataResponse.FailedRowResult r : status) {
            errors.add(new LineAndError(lines.get(r.getIndex()), r.getError()));
        }

        return errors;
    }
}
