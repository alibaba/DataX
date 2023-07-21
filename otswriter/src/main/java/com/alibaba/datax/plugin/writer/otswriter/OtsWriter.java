package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSMode;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OtsWriter {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private IOtsWriterMasterProxy proxy;

        @Override
        public void init() {
            LOG.info("init() begin ...");
            proxy = new OtsWriterMasterProxy();
            try {
                this.proxy.init(getPluginJobConf());
            } catch (TableStoreException e) {
                LOG.error("OTSException: {}", e.toString(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS Client Error"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}", e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }

            LOG.info("init() end ...");
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            try {
                return this.proxy.split(mandatoryNumber);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private IOtsWriterSlaveProxy proxy = null;

        /**
         * 基于配置，构建对应的worker代理
         */
        @Override
        public void init() {
            OTSConf conf = GsonParser.jsonToConf(this.getPluginJobConf().getString(OTSConst.OTS_CONF));
            // 是否使用新接口
            if(conf.isNewVersion()) {
                if (conf.getMode() == OTSMode.MULTI_VERSION) {
                    LOG.info("init OtsWriterSlaveProxyMultiVersion");
                    proxy = new OtsWriterSlaveProxyMultiversion();
                } else {
                    LOG.info("init OtsWriterSlaveProxyNormal");
                    proxy = new OtsWriterSlaveProxyNormal();
                }

            }
            else{
                proxy = new OtsWriterSlaveProxyOld();
            }

            proxy.init(this.getPluginJobConf());

        }

        @Override
        public void destroy() {
            try {
                proxy.close();
            } catch (OTSCriticalException e) {
                LOG.error("OTSCriticalException. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("startWrite() begin ...");

            try {
                proxy.write(lineReceiver, this.getTaskPluginCollector());
            } catch (TableStoreException e) {
                LOG.error("OTSException: {}", e.toString(), e);
                throw DataXException.asDataXException(new OtsWriterError(e.getErrorCode(), "OTS Client Error"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}", e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsWriterError.ERROR, e.toString(), e);
            }

            LOG.info("startWrite() end ...");
        }
    }
}
