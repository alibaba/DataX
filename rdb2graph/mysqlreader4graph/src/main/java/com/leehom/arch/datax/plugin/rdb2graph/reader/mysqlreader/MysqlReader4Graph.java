package com.leehom.arch.datax.plugin.rdb2graph.reader.mysqlreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.CommonRdbms2GraphReader;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Constant;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.DataBaseType;

/**
 * @类名: MysqlReader4Graph
 * @说明: mysql读入
 *        1.表数据同步，datax table模式，schema表写入datax的配置
		  2.表关系同步，按表连接图分组，一对关系一组，表连接关系生成querySql，写入datax的querySql模式配置
 *
 * @author   leehom
 * @Date	 2022年4月30日 上午12:38:34
 * 修改记录：
 *
 * @see 	 
 */
public class MysqlReader4Graph extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;
        private CommonRdbms2GraphReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            Integer userConfigedFetchSize = this.originalConfig.getInt(Constant.FETCH_SIZE);
            if (userConfigedFetchSize != null) {
                LOG.warn("对 mysqlreader 不需要配置 fetchSize, mysqlreader 将会忽略这项配置. 如果您不想再看到此警告,请去除fetchSize 配置.");
            }

            this.originalConfig.set(Constant.FETCH_SIZE, Integer.MIN_VALUE);

            this.commonRdbmsReaderJob = new CommonRdbms2GraphReader.Job(DATABASE_TYPE);
            this.commonRdbmsReaderJob.init(this.originalConfig);
        }

        @Override
        public void preCheck(){
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig,DATABASE_TYPE);

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return this.commonRdbmsReaderJob.split(this.originalConfig, adviceNumber);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;
        private CommonRdbms2GraphReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbms2GraphReader.Task(DATABASE_TYPE,super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);

        }

        @Override
        public void startRead(RecordSender recordSender) {
            int fetchSize = this.readerSliceConfig.getInt(Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig, recordSender,
                    super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
