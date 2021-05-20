package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;

public class ColumnMetaCache {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnMetaCache.class);
	
	private static String tableName;
	private static Triple<List<String>, List<Integer>, List<String>> columnMeta = null;

	public ColumnMetaCache() {
		
	}
	
	public static void init(Connection connection, final String tableName, final List<String> columns) throws SQLException {
		if (columnMeta == null) {
			synchronized(ColumnMetaCache.class) {
				ColumnMetaCache.tableName = tableName;
				if (columnMeta == null) {
					columnMeta = DBUtil.getColumnMetaData(connection,
            				tableName, StringUtils.join(columns, ","));
					LOG.info("fetch columnMeta of table {} success", tableName);
				}
			}
		}
	}
	
	public static Triple<List<String>, List<Integer>, List<String>> getColumnMeta() {
		return columnMeta;
	}
	
}
