package com.alibaba.datax.plugin.writer.adbpgwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.writer.adbpgwriter.copy.Adb4pgClientProxy;
import com.alibaba.datax.plugin.writer.adbpgwriter.util.Adb4pgUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode.*;
import static com.alibaba.datax.plugin.rdbms.util.DataBaseType.PostgreSQL;

/**
 * @author yuncheng
 */
public class AdbpgWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.PostgreSQL;

    public static class Job extends Writer.Job {

        private Configuration originalConfig;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;
        private static final Logger LOG = LoggerFactory.getLogger(Writer.Job.class);

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            LOG.info("in Job.init(), config is:[\n{}\n]", originalConfig.toJSON());
            this.commonRdbmsWriterMaster =  new CommonRdbmsWriter.Job(DATABASE_TYPE);
            //convert to DatabaseConfig, use DatabaseConfig to check user configuration
            Adb4pgUtil.checkConfig(originalConfig);
        }

        @Override
        public void prepare() {

            Adb4pgUtil.prepare(originalConfig);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> splitResult = new ArrayList<Configuration>();
            for(int i = 0; i < adviceNumber; i++) {
                splitResult.add(this.originalConfig.clone());
            }
            return splitResult;
        }

        @Override
        public void post() {

            Adb4pgUtil.post(originalConfig);
        }

        @Override
        public void destroy() {

        }



    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;
        private Adb4pgClientProxy adb4pgClientProxy;
        //Adb4pgClient client;
        @Override
        public void init() {
            this.writerSliceConfig = super.getPluginJobConf();
            this.adb4pgClientProxy = new Adb4pgClientProxy(writerSliceConfig, super.getTaskPluginCollector());
            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE){
                @Override
                public String calcValueHolder(String columnType){
                    if("serial".equalsIgnoreCase(columnType)){
                        return "?::int";
                    }else if("bit".equalsIgnoreCase(columnType)){
                        return "?::bit varying";
                    }
                    return "?::" + columnType;
                }
            };
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            this.adb4pgClientProxy.startWriteWithConnection(recordReceiver, Adb4pgUtil.getAdbpgConnect(writerSliceConfig));
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

    }
}
