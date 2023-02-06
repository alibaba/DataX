package com.alibaba.datax.plugin.reader.clickhousereader;

import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

/**
 * @author : donghao
 * @version : 1.0
 * @className : ClickHouseReader
 * @date : 2022-07-20 16:14
 */
public class ClickHouseReader extends Reader {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseReader.class);
    private static final DataBaseType DATABASE_TYPE = DataBaseType.ClickHouse;

    public static class Job extends Reader.Job {
        private Configuration originalConfig;
        private CommonRdbmsReader.Job commonRdbmsReaderMaster;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.commonRdbmsReaderMaster = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderMaster.init(this.originalConfig);
            LOG.info("ClickHouseReader Job初始化成功");
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsReaderMaster.split(this.originalConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderMaster.destroy(this.originalConfig);
        }
    }

    public static class Task extends Reader.Task {

        private Configuration taskConfig;

        private int fetchSize;

        private CommonRdbmsReader.Task commonRdbmsReaderMaster;


        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            int fetchSize = this.taskConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    Constant.DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        String.format("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.", fetchSize));
            }
            commonRdbmsReaderMaster = new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId());
            commonRdbmsReaderMaster.init(taskConfig);
            LOG.info("ClickHouseReader Task初始化成功");
        }

        @Override
        public void destroy() {
            commonRdbmsReaderMaster.destroy(taskConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            commonRdbmsReaderMaster.startRead(taskConfig, recordSender, super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            commonRdbmsReaderMaster.post(taskConfig);
        }
    }

}
