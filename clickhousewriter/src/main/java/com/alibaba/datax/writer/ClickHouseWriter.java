/**
 * 私有代码，未经许可，不得复制、散播；
 * 否则将可能依法追究责任。
 */

package com.alibaba.datax.writer;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.util.List;

/**
 * Summary：<p></p>
 * Author : Martin
 * Since  : 2019/5/18 2:16
 */
public class ClickHouseWriter
        extends Writer
{
    private static final DataBaseType DATABASE_TYPE = DataBaseType.ClickHouse;

    public static class Job
            extends Writer.Job
    {
        private Configuration configuration = null;
        private CommonRdbmsWriter.Job writerJob = null;

        @Override
        public void preCheck()
        {
            this.init();
            this.writerJob.writerPreCheck(this.configuration, DATABASE_TYPE);
        }

        @Override
        public void init()
        {
            this.configuration = super.getPluginJobConf();
            this.writerJob = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.writerJob.init(this.configuration);
        }

        @Override
        public void prepare()
        {
            this.writerJob.prepare(this.configuration);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber)
        {
            return this.writerJob.split(this.configuration, mandatoryNumber);
        }

        @Override
        public void post()
        {
            this.writerJob.post(this.configuration);
        }

        @Override
        public void destroy()
        {
            this.writerJob.destroy(this.configuration);
        }
    }

    public static class Task
            extends Writer.Task
    {
        private Configuration configuration = null;
        private CommonRdbmsWriter.Task writerTask = null;

        @Override
        public void init()
        {
            this.configuration = super.getPluginJobConf();
            this.writerTask = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.writerTask.init(this.configuration);
        }

        @Override
        public void prepare()
        {
            this.writerTask.prepare(this.configuration);
        }

        public void startWrite(RecordReceiver recordReceiver)
        {
            this.writerTask.startWrite(recordReceiver, this.configuration,
                    super.getTaskPluginCollector());
        }

        @Override
        public void post()
        {
            this.writerTask.post(this.configuration);
        }

        @Override
        public void destroy()
        {
            this.writerTask.destroy(this.configuration);
        }

        @Override
        public boolean supportFailOver()
        {
            String writeMode = configuration.getString(Key.WRITE_MODE);
            return "replace".equalsIgnoreCase(writeMode);
        }
    }
}
