package com.alibaba.datax.plugin.writer.hbase11xwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Hbase11xWriter
 * Created by shf on 16/3/17.
 */
public class Hbase11xWriter extends Writer {
    public static class Job extends Writer.Job {
        private Configuration originConfig = null;
        @Override
        public void init() {
            this.originConfig = this.getPluginJobConf();
            Hbase11xHelper.validateParameter(this.originConfig);
        }

        @Override
        public void  prepare(){
            Boolean truncate = originConfig.getBool(Key.TRUNCATE,false);
            if(truncate){
                Hbase11xHelper.truncateTable(this.originConfig);
            }
        }
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(originConfig.clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy() {

        }
    }
    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        private HbaseAbstractTask hbaseTaskProxy;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            String mode = this.taskConfig.getString(Key.MODE);
            ModeType modeType = ModeType.getByTypeName(mode);

            switch (modeType) {
                case Normal:
                    this.hbaseTaskProxy = new NormalTask(this.taskConfig);
                    break;
                default:
                    throw DataXException.asDataXException(Hbase11xWriterErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持此类模式:" + modeType);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.hbaseTaskProxy.startWriter(lineReceiver,super.getTaskPluginCollector());
        }


        @Override
        public void destroy() {
            if (this.hbaseTaskProxy != null) {
                this.hbaseTaskProxy.close();
            }
        }
    }
}
