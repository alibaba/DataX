package com.alibaba.datax.plugin.writer.otswriter.model;

import com.alibaba.datax.plugin.writer.otswriter.OTSCriticalException;
import com.alibaba.datax.plugin.writer.otswriter.OTSErrorCode;
import com.alibaba.datax.plugin.writer.otswriter.callable.BatchWriteRowCallable;
import com.alibaba.datax.plugin.writer.otswriter.callable.PutRowChangeCallable;
import com.alibaba.datax.plugin.writer.otswriter.callable.UpdateRowChangeCallable;
import com.alibaba.datax.plugin.writer.otswriter.utils.CollectorUtil;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.LineAndError;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse.RowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OTSBatchWriterRowTask implements Runnable {
    private SyncClientInterface ots = null;
    private OTSConf conf = null;
    private List<OTSLine> otsLines = new ArrayList<OTSLine>();
    
    private boolean isDone = false;
    private int retryTimes = 0;
    
    private static final Logger LOG = LoggerFactory.getLogger(OTSBatchWriterRowTask.class);
    
    public OTSBatchWriterRowTask(
            final SyncClientInterface ots,
            final OTSConf conf, 
            final List<OTSLine> lines
            ) {
        this.ots = ots;
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
        if (ee.getErrorCode().equals(OTSErrorCode.INVALID_PARAMETER)||
                ee.getErrorCode().equals(OTSErrorCode.REQUEST_TOO_LARGE)
                ) {
            return true;
        }
        return false;
    }
    
    private BatchWriteRowRequest createRequest(List<OTSLine> lines) {
        BatchWriteRowRequest newRequest = new BatchWriteRowRequest();
        switch (conf.getOperation()) {
            case PUT_ROW:
            case UPDATE_ROW:
                for (OTSLine l : lines) {
                    newRequest.addRowChange(l.getRowChange());
                }
                break;
            default:
                throw new RuntimeException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, conf.getOperation()));
        }
        return newRequest;
    }
    
    /**
     * 单行发送数据
     * @param line
     */
    public void sendLine(OTSLine line) {
        try {
            switch (conf.getOperation()) {
                case PUT_ROW:
                    PutRowRequest putRowRequest = new PutRowRequest();
                    putRowRequest.setRowChange((RowPutChange) line.getRowChange());
                    PutRowResponse putResult = RetryHelper.executeWithRetry(
                            new PutRowChangeCallable(ots, putRowRequest),
                            conf.getRetry(), 
                            conf.getSleepInMillisecond());
                    LOG.debug("Requst ID : {}", putResult.getRequestId());
                    break;
                case UPDATE_ROW:
                    UpdateRowRequest updateRowRequest = new UpdateRowRequest();
                    updateRowRequest.setRowChange((RowUpdateChange) line.getRowChange());
                    UpdateRowResponse updateResult = RetryHelper.executeWithRetry(
                            new UpdateRowChangeCallable(ots, updateRowRequest), 
                            conf.getRetry(), 
                            conf.getSleepInMillisecond());
                    LOG.debug("Requst ID : {}", updateResult.getRequestId());
                    break;
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
    
    /**
     * 批量发送数据
     * 如果程序发送失败，BatchWriteRow接口可能整体异常返回或者返回每个子行的操作状态
     * 1.在整体异常的情况下：方法会检查这个异常是否能通过把批量数据拆分成单行发送，如果不行，
     * 将会把这一批数据记录到脏数据回收器中，如果可以，方法会调用sendAllOneByOne进行单行数据发送。
     * 2.如果BatchWriteRow成功执行，方法会检查每行的返回状态，如果子行操作失败，方法会收集所有失
     * 败的行，重新调用sendAll，发送失败的数据。
     * @param lines
     */
    private void sendAll(List<OTSLine> lines) {
        try {
            Thread.sleep(Common.getDelaySendMillinSeconds(retryTimes, conf.getSleepInMillisecond()));
            BatchWriteRowRequest batchWriteRowRequest = createRequest(lines);
            BatchWriteRowResponse result = RetryHelper.executeWithRetry(
                    new BatchWriteRowCallable(ots, batchWriteRowRequest), 
                    conf.getRetry(), 
                    conf.getSleepInMillisecond());
            
            LOG.debug("Requst ID : {}", result.getRequestId());
            List<LineAndError> errors = getLineAndError(result, lines);
            if (!errors.isEmpty()){
                if(retryTimes < conf.getRetry()) {
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
    
    private List<LineAndError> getLineAndError(BatchWriteRowResponse result, List<OTSLine> lines) throws OTSCriticalException {
        List<LineAndError> errors = new ArrayList<LineAndError>();
        
        switch(conf.getOperation()) {
            case PUT_ROW:
            case UPDATE_ROW: {
                List<RowResult> status = result.getFailedRows();
                for (RowResult r : status) {
                    errors.add(new LineAndError(lines.get(r.getIndex()), r.getError()));
                }
            }
            break;
            default:
                LOG.error("Bug branch.");
                throw new OTSCriticalException(String.format(OTSErrorMessage.OPERATION_PARSE_ERROR, conf.getOperation()));
        }
        return errors;
    }
}
