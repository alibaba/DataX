package com.alibaba.datax.plugin.reader.clickhousereader;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson2.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickhouseReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.ClickHouse;
    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseReader.class);

    public static class Job extends Reader.Job {
        private Configuration jobConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderMaster;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();
            this.commonRdbmsReaderMaster = new CommonRdbmsReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderMaster.init(this.jobConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.commonRdbmsReaderMaster.split(this.jobConfig, mandatoryNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderMaster.post(this.jobConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderMaster.destroy(this.jobConfig);
        }
    }

    public static class Task extends Reader.Task {

        private Configuration jobConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderSlave;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();
            this.commonRdbmsReaderSlave = new CommonRdbmsReader.Task(DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderSlave.init(this.jobConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.jobConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, 1000);

            this.commonRdbmsReaderSlave.startRead(this.jobConfig, recordSender, super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderSlave.post(this.jobConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderSlave.destroy(this.jobConfig);
        }
    }
}
