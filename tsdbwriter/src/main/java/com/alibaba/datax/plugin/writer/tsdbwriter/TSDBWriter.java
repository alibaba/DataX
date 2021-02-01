package com.alibaba.datax.plugin.writer.tsdbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.conn.TSDBConnection;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Http Writer
 *
 * @author Benedict Jin
 * @since 2019-04-18
 */
@SuppressWarnings("unused")
public class TSDBWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String address = this.originalConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(address)) {
                throw DataXException.asDataXException(TSDBWriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.ENDPOINT + "] is not set.");
            }

            Integer batchSize = this.originalConfig.getInt(Key.BATCH_SIZE);
            if (batchSize == null || batchSize < 1) {
                originalConfig.set(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
                LOG.info("The parameter [" + Key.BATCH_SIZE +
                        "] will be default value: " + Constant.DEFAULT_BATCH_SIZE);
            }

            Integer retrySize = this.originalConfig.getInt(Key.MAX_RETRY_TIME);
            if (retrySize == null || retrySize < 0) {
                originalConfig.set(Key.MAX_RETRY_TIME, Constant.DEFAULT_TRY_SIZE);
                LOG.info("The parameter [" + Key.MAX_RETRY_TIME +
                        "] will be default value: " + Constant.DEFAULT_TRY_SIZE);
            }

            Boolean ignoreWriteError = this.originalConfig.getBool(Key.IGNORE_WRITE_ERROR);
            if (ignoreWriteError == null) {
                originalConfig.set(Key.IGNORE_WRITE_ERROR, Constant.DEFAULT_IGNORE_WRITE_ERROR);
                LOG.info("The parameter [" + Key.IGNORE_WRITE_ERROR +
                        "] will be default value: " + Constant.DEFAULT_IGNORE_WRITE_ERROR);
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            ArrayList<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private TSDBConnection conn;
        private int batchSize;
        private int retrySize;
        private boolean ignoreWriteError;

        @Override
        public void init() {
            Configuration writerSliceConfig = getPluginJobConf();
            String address = writerSliceConfig.getString(Key.ENDPOINT);
            this.conn = new TSDBConnection(address);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE);
            this.retrySize = writerSliceConfig.getInt(Key.MAX_RETRY_TIME);
            this.ignoreWriteError = writerSliceConfig.getBool(Key.IGNORE_WRITE_ERROR);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record lastRecord = null;
                Record record;
                int count = 0;
                StringBuilder dps = new StringBuilder();
                while ((record = recordReceiver.getFromReader()) != null) {
                    final int recordLength = record.getColumnNumber();
                    for (int i = 0; i < recordLength; i++) {
                        dps.append(record.getColumn(i).asString());
                        dps.append(",");
                        count++;
                        if (count == batchSize) {
                            count = 0;
                            batchPut(record, "[" + dps.substring(0, dps.length() - 1) + "]");
                            dps = new StringBuilder();
                        }
                    }
                    lastRecord = record;
                }
                if (StringUtils.isNotBlank(dps.toString())) {
                    batchPut(lastRecord, "[" + dps.substring(0, dps.length() - 1) + "]");
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(TSDBWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
        }

        private void batchPut(final Record record, final String dps) {
            try {
                RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() {
                        if (!conn.put(dps)) {
                            getTaskPluginCollector().collectDirtyRecord(record, "Put data points failed!");
                            throw DataXException.asDataXException(TSDBWriterErrorCode.RUNTIME_EXCEPTION,
                                    "Put data points failed!");
                        }
                        return 0;
                    }
                }, retrySize, 60000L, true);
            } catch (Exception e) {
                if (ignoreWriteError) {
                    LOG.warn("Ignore write exceptions and continue writing.");
                } else {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.RETRY_WRITER_EXCEPTION, e);
                }
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
