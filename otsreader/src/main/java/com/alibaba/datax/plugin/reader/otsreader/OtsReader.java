package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.utils.Constant;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsreader.utils.OtsReaderError;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.aliyun.openservices.ots.ClientException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class OtsReader extends Reader {

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        //private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OtsReader.class);
        private IOtsReaderMasterProxy proxy = null;

        @Override
            public void init() {
            LOG.info("init() begin ...");

            proxy = new OtsReaderMasterProxy();
            try {
                this.proxy.init(getPluginJobConf());
            } catch (TableStoreException e) {
                LOG.error("OTSException: {}", e.toString(), e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS ERROR"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
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
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }

            LOG.info("split() end ...");
            return confs;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        //private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(OtsReader.class);
        private IOtsReaderSlaveProxy proxy = null;

        @Override
        public void init() {

            OTSConf conf = GsonParser.jsonToConf((String) this.getPluginJobConf().get(Constant.ConfigKey.CONF));
            // 是否使用新接口
            if(conf.isNewVersion()) {
                if (conf.getMode() == OTSMode.MULTI_VERSION) {
                    LOG.info("init OtsReaderSlaveProxyMultiVersion");
                    proxy = new OtsReaderSlaveMultiVersionProxy();
                } else {
                    LOG.info("init OtsReaderSlaveProxyNormal");
                    proxy = new OtsReaderSlaveNormalProxy();
                }

            }
            else{
                String metaMode = conf.getMetaMode();
                if (StringUtils.isNotBlank(metaMode) && !metaMode.equalsIgnoreCase("false")) {
                    LOG.info("init OtsMetaReaderSlaveProxy");
                    proxy = new OtsReaderSlaveMetaProxy();
                } else {
                    LOG.info("init OtsReaderSlaveProxyOld");
                    proxy = new OtsReaderSlaveProxyOld();
                }
            }

            proxy.init(this.getPluginJobConf());
        }

        @Override
        public void destroy() {
            try {
                proxy.close();
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {

            try {
                proxy.startRead(recordSender);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }



        }

    }
}
