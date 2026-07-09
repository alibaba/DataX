package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;

public class ColumnMetaCache {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnMetaCache.class);
	
	private static String tableName;
	//oceanbase数据库可以兼容多表并行进行数据迁移
	private static Map<String ,Triple<List<String>, List<Integer>, List<String>>> columnMeta = new ConcurrentHashMap<>();

	public ColumnMetaCache() {
		
	}
	
	public static void init(Connection connection, final String tableName, final List<String> columns) throws SQLException {
		Triple<List<String>, List<Integer>, List<String>> meta = columnMeta.get(tableName);
		if (meta == null) {
			ColumnMetaCache.tableName = tableName;
			meta = DBUtil.getColumnMetaData(connection,
					tableName, StringUtils.join(columns, ","));
			columnMeta.put(tableName,meta);
			LOG.info("fetch columnMeta of table {} success", tableName);
		}
	}

	public static Triple<List<String>, List<Integer>, List<String>> getColumnMeta(String tableName) {
		return columnMeta.get(tableName);
	}
	
}
