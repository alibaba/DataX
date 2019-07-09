package com.alibaba.datax.plugin.reader.tablestorereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TableStoreReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private TableStoreReaderMasterProxy proxy = new TableStoreReaderMasterProxy();

        @Override
        public void init() {
            LOG.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreReaderError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreReaderError.ERROR, Common.getDetailMessage(e), e);
            }
            LOG.info("init() end ...");
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin ...");

            // search query 不能使用多线程|进程执行, 只能拆分成一个任务
            if (adviceNumber <= 0) {
                throw DataXException.asDataXException(TableStoreReaderError.ERROR, "Datax input adviceNumber <= 0.");
            }

            List<Configuration> confs = this.proxy.split(adviceNumber);

            LOG.info("split() end ...");

            return confs;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private TableStoreReaderSlaveProxy proxy = new TableStoreReaderSlaveProxy();

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("startRead() begin ...");
            try {
                this.proxy.read(recordSender, getPluginJobConf());
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreReaderError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(TableStoreReaderError.ERROR, Common.getDetailMessage(e), e);
            }
            LOG.info("startRead() end ...");
        }
    }
}
