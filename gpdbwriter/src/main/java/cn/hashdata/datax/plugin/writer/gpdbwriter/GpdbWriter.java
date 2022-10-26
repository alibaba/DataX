package cn.hashdata.datax.plugin.writer.gpdbwriter;

import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import cn.hashdata.datax.plugin.writer.gpdbwriter.CopyWriterJob;
import cn.hashdata.datax.plugin.writer.gpdbwriter.CopyWriterTask;

public class GpdbWriter extends Writer {

	public static class Job extends Writer.Job {
		private Configuration originalConfig = null;
		private CopyWriterJob copyJob;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();

			// warn：not like mysql, PostgreSQL only support insert mode, don't
			// use
			String writeMode = this.originalConfig.getString(Key.WRITE_MODE);
			if (null != writeMode) {
				throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
						String.format(
								"写入模式(writeMode)配置有误. 因为Greenplum Database不支持配置参数项 writeMode: %s, Greenplum Database仅使用insert sql 插入数据. 请检查您的配置并作出修改.",
								writeMode));
			}

			int segment_reject_limit = this.originalConfig.getInt("segment_reject_limit", 0);

			if (segment_reject_limit != 0 && segment_reject_limit < 2) {
				throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "segment_reject_limit 必须为0或者大于等于2");
			}

			this.copyJob = new CopyWriterJob();
			this.copyJob.init(this.originalConfig);
		}

		@Override
		public void prepare() {
			this.copyJob.prepare(this.originalConfig);
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			return this.copyJob.split(this.originalConfig, mandatoryNumber);
		}

		@Override
		public void post() {
			this.copyJob.post(this.originalConfig);
		}

		@Override
		public void destroy() {
			this.copyJob.destroy(this.originalConfig);
		}

	}

	public static class Task extends Writer.Task {
		private Configuration writerSliceConfig;
		private CopyWriterTask copyTask;

		@Override
		public void init() {
			this.writerSliceConfig = super.getPluginJobConf();
			this.copyTask = new CopyWriterTask();
			this.copyTask.init(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			this.copyTask.prepare(this.writerSliceConfig);
		}

		public void startWrite(RecordReceiver recordReceiver) {
			this.copyTask.startWrite(recordReceiver, this.writerSliceConfig,
					super.getTaskPluginCollector());
		}

		@Override
		public void post() {
			this.copyTask.post(this.writerSliceConfig);
		}

		@Override
		public void destroy() {
			this.copyTask.destroy(this.writerSliceConfig);
		}
	}
}
