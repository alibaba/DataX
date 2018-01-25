package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.utils.Common;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;

public class OtsReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        @Override
        public void init() {
            LOG.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (OTSException e) {
                LOG.error("OTSException. ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()});
                LOG.error("Stack", e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException. ErrorCode:{}, ErrorMsg:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage()});
                LOG.error("Stack", e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, Common.getDetailMessage(e), e);
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

            if (adviceNumber <= 0) {
                throw DataXException.asDataXException(OtsReaderError.ERROR, "Datax input adviceNumber <= 0.");
            }

            List<Configuration> confs = null;

            try {
                confs = this.proxy.split(adviceNumber);
            } catch (OTSException e) {
                LOG.error("OTSException. ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()});
                LOG.error("Stack", e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException. ErrorCode:{}, ErrorMsg:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage()});
                LOG.error("Stack", e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, Common.getDetailMessage(e), e);
            }

            LOG.info("split() end ...");
            return confs;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private OtsReaderSlaveProxy proxy = new OtsReaderSlaveProxy();

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
                this.proxy.read(recordSender,getPluginJobConf());
            } catch (OTSException e) {
                LOG.error("OTSException. ErrorCode:{}, ErrorMsg:{}, RequestId:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage(), e.getRequestId()});
                LOG.error("Stack", e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (ClientException e) {
                LOG.error("ClientException. ErrorCode:{}, ErrorMsg:{}", 
                        new Object[]{e.getErrorCode(), e.getMessage()});
                LOG.error("Stack", e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), Common.getDetailMessage(e), e);
            } catch (IllegalArgumentException e) {
                LOG.error("IllegalArgumentException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.INVALID_PARAM, Common.getDetailMessage(e), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, Common.getDetailMessage(e), e);
            }
            LOG.info("startRead() end ...");
        }

    }
}
