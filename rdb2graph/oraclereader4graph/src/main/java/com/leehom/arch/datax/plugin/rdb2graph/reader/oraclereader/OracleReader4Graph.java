package com.leehom.arch.datax.plugin.rdb2graph.reader.oraclereader;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.CommonRdbms2GraphReader;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Key;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.util.HintUtil;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.DBUtilErrorCode;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.DataBaseType;

/**
 * @类名: OracleReader4Graph
 * @说明: oracle读入
 *        1.表数据同步，datax table模式，schema表写入datax的配置
		  2.表关系同步，按表连接图分组，一对关系一组，表连接关系生成querySql，写入datax的querySql模式配置

 *
 * @author   leehom
 * @Date	 2022年4月28日 下午6:30:15
 * 修改记录：
 *
 * @see 	 
 */
public class OracleReader4Graph extends Reader {

	private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(OracleReader4Graph.Job.class);

		//
		private Configuration originalConfig = null;
		private CommonRdbms2GraphReader.Job commonRdbmsReaderJob;

		@Override
		public void init() {
			// 载入配置
			this.originalConfig = super.getPluginJobConf();			
			// 
			dealFetchSize(this.originalConfig);
			// 
			this.commonRdbmsReaderJob = new CommonRdbms2GraphReader.Job(DATABASE_TYPE);
			// 
			this.commonRdbmsReaderJob.init(this.originalConfig);

			// 注意：要在 this.commonRdbmsReaderJob.init(this.originalConfig); 之后执行，这样可以直接快速判断是否是querySql 模式
			dealHint(this.originalConfig);
		}

		// 检查连接/表是否可查询
        @Override
        public void preCheck(){
        	init();
            // 检测
            this.commonRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);
        }

        // 分片，包括多表分配，表分片
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

		// fetch size
		private void dealFetchSize(Configuration originalConfig) {
			int fetchSize = originalConfig.getInt(
					com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Constant.FETCH_SIZE,
					Constant.DEFAULT_FETCH_SIZE);
			if (fetchSize < 1) {
				throw DataXException
						.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
								String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
										fetchSize));
			}
			originalConfig.set(
					com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Constant.FETCH_SIZE, fetchSize);
		}

		private void dealHint(Configuration originalConfig) {
			String hint = originalConfig.getString(Key.HINT);
			if (StringUtils.isNotBlank(hint)) {
				boolean isTableMode = originalConfig.getBool(com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Constant.IS_TABLE_MODE).booleanValue();
				if(!isTableMode){
					throw DataXException.asDataXException(OracleReaderErrorCode.HINT_ERROR, "当且仅当非 querySql 模式读取 oracle 时才能配置 HINT.");
				}
				HintUtil.initHintConf(DATABASE_TYPE, originalConfig);
			}
		}
	}

	public static class Task extends Reader.Task {

		private Configuration readerSliceConfig;
		private CommonRdbms2GraphReader.Task commonRdbmsReaderTask;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsReaderTask = new CommonRdbms2GraphReader.Task(
					DATABASE_TYPE ,super.getTaskGroupId(), super.getTaskId());
			this.commonRdbmsReaderTask.init(this.readerSliceConfig);
		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = this.readerSliceConfig
					.getInt(com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Constant.FETCH_SIZE);

			this.commonRdbmsReaderTask.startRead(this.readerSliceConfig,
					recordSender, super.getTaskPluginCollector(), fetchSize);
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
