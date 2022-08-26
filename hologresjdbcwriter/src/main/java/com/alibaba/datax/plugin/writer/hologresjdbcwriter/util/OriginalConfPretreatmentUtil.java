package com.alibaba.datax.plugin.writer.hologresjdbcwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.Constant;
import com.alibaba.datax.plugin.writer.hologresjdbcwriter.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class OriginalConfPretreatmentUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(OriginalConfPretreatmentUtil.class);

	public static DataBaseType DATABASE_TYPE;

	public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType) {
		// 检查 username/password 配置（必填）
		originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
		originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);

		doCheckBatchSize(originalConfig);
		simplifyConf(originalConfig);
	}

	public static void doCheckBatchSize(Configuration originalConfig) {
		// 检查batchSize 配置（选填，如果未填写，则设置为默认值）
		int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
		if (batchSize < 1) {
			throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
					"您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[256-1024] (保持128的倍数), 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
					batchSize));
		}

		originalConfig.set(Key.BATCH_SIZE, batchSize);
	}

	public static void simplifyConf(Configuration originalConfig) {
		List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
				Object.class);

		int tableNum = 0;

		for (int i = 0, len = connections.size(); i < len; i++) {
			Configuration connConf = Configuration.from(connections.get(i).toString());

			String jdbcUrl = connConf.getString(Key.JDBC_URL);
			if (StringUtils.isBlank(jdbcUrl)) {
				throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
			}

			List<String> tables = connConf.getList(Key.TABLE, String.class);

			if (null == tables || tables.isEmpty()) {
				throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
						"您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
			}

			// 对每一个connection 上配置的table 项进行解析
			List<String> expandedTables = TableExpandUtil
					.expandTableConf(DATABASE_TYPE, tables);

			if (null == expandedTables || expandedTables.isEmpty()) {
				throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
						"您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
			}

			tableNum += expandedTables.size();

			originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
					i, Key.TABLE), expandedTables);
		}

		originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
	}

}
