package com.alibaba.datax.plugin.writer.oceanbasev10writer.util;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter.Task;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class ObWriterUtils {
	protected static final Logger LOG = LoggerFactory.getLogger(Task.class);

	private static String CHECK_MEMSTORE = "select 1 from %s.gv$memstore t where t.total>t.mem_limit * ?";

	private static String compatibleMode = null;

	public static boolean isMemstoreFull(Connection conn, double memstoreThreshold) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		try {
			String sysDbName = "oceanbase";
			if (isOracleMode()) {
				sysDbName = "sys";
			}
			ps = conn.prepareStatement(String.format(CHECK_MEMSTORE, sysDbName));
			ps.setDouble(1, memstoreThreshold);
			rs = ps.executeQuery();
			// 只要有满足条件的,则表示当前租户 有个机器的memstore即将满
			result = rs.next();
		} catch (Throwable e) {
			LOG.error("check memstore fail" + e.getMessage());
			result = false;
		} finally {
			//do not need to close the statment in ob1.0
		}

		LOG.info("isMemstoreFull=" + result);
		return result;
	}

	public static boolean isOracleMode(){
		return (compatibleMode.equals(Config.OB_COMPATIBLE_MODE_ORACLE));
	}

	public static String getCompatibleMode() {
		return compatibleMode;
	}

	public static void setCompatibleMode(String mode) {
		compatibleMode = mode;
	}

	private static String buildDeleteSql (String tableName, List<String> columns) {
		StringBuilder builder = new StringBuilder("DELETE FROM ");
		builder.append(tableName).append(" WHERE ");
		for (int i = 0; i < columns.size(); i++) {
			builder.append(columns.get(i)).append(" = ?");
			if (i != columns.size() - 1) {
				builder.append(" and ");
			}
		}
		return builder.toString();
	}

	private static int[] getColumnIndex(List<String> columnsInIndex, List<String> allColumns) {
		allColumns = allColumns.stream().map(String::toUpperCase).collect(Collectors.toList());
		int[] colIdx = new int[columnsInIndex.size()];
		for (int i = 0; i < columnsInIndex.size(); i++) {
			int index = allColumns.indexOf(columnsInIndex.get(i));
			if (index < 0) {
				throw new RuntimeException(
						String.format("column {} is in unique or primary key but not in the column list.",
								columnsInIndex.get(i)));
			}
			colIdx[i] = index;
		}
		return colIdx;
	}

	public static List<Pair<String, int[]>> buildDeleteSql(Connection conn, String dbName, String tableName,
                                                           List<String> columns) {
		List<Pair<String, int[]>> deleteMeta = new ArrayList();
		Map<String, List<String>> uniqueKeys = getAllUniqueIndex(conn, dbName, tableName);
		for (Map.Entry<String, List<String>> entry : uniqueKeys.entrySet()) {
			List<String> colNames = entry.getValue();
			String deleteSql = buildDeleteSql(tableName, colNames);
			int[] colIdx = getColumnIndex(colNames, columns);
			LOG.info("delete sql [{}], column index: {}", deleteSql, Arrays.toString(colIdx));
			deleteMeta.add(new ImmutablePair(deleteSql, colIdx));
		}
		return deleteMeta;
	}

	// this function is just for oracle mode
	private static Map<String, List<String>> getAllUniqueIndex(Connection conn, String dbName, String tableName) {
		Map<String, List<String>> uniqueKeys = new HashMap();
		if (tableName.contains("\\.")) {
			dbName = tableName.split("\\.")[0];
			tableName = tableName.split("\\.")[1];
		}
		dbName = dbName.toUpperCase();
		String sql = String.format("select cons.CONSTRAINT_NAME AS KEY_NAME, cols.COLUMN_NAME COLUMN_NAME " +
				"from all_constraints cons, all_cons_columns cols " +
				"WHERE cols.table_name = '%s' AND cons.constraint_type in('P', 'U') " +
				"  AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner " +
				"  AND cols.owner = '%s' " +
				"Order by KEY_NAME, cols.POSITION", tableName, dbName);

		LOG.info("get all unique keys by sql {}", sql);

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String keyName = rs.getString("Key_name");
				String columnName = StringUtils.upperCase(rs.getString("Column_name"));
				List<String> s = uniqueKeys.get(keyName);
				if (s == null) {
					s = new ArrayList();
					uniqueKeys.put(keyName, s);
				}
				s.add(columnName);
			}
		} catch (Throwable e) {
			LOG.error("show index from table fail :" + sql, e);
		} finally {
			asyncClose(rs, stmt, null);
		}
		return uniqueKeys;
	}

	/**
	 *
	 * @param tableName
	 * @param columnHolders
	 * @param conn
	 * @param writeMode
	 * @return
	 */
	public static String buildWriteSql(String tableName, List<String> columnHolders,
									   Connection conn, String writeMode, String obUpdateColumns) {
		List<String> valueHolders = new ArrayList<String>(columnHolders.size());
		for (int i = 0; i < columnHolders.size(); i++) {
			valueHolders.add("?");
		}
		String writeDataSqlTemplate = new StringBuilder().append("INSERT INTO " + tableName + " (")
                .append(StringUtils.join(columnHolders, ",")).append(") VALUES(")
                .append(StringUtils.join(valueHolders, ",")).append(")").toString();

        LOG.info("write mode: " + writeMode);

        // update mode
        if (!writeMode.equals("insert")) {
			if (obUpdateColumns == null) {
				Set<String> skipColumns = getSkipColumns(conn, tableName);

				StringBuilder columnList = new StringBuilder();
				for (String column : skipColumns) {
					columnList.append(column).append(",");
				}
				LOG.info("Skip columns: " + columnList.toString());
				writeDataSqlTemplate = writeDataSqlTemplate + onDuplicateKeyUpdateString(columnHolders, skipColumns);
			} else {
				LOG.info("Update columns: " + obUpdateColumns);
				writeDataSqlTemplate = writeDataSqlTemplate + onDuplicateKeyUpdateString(obUpdateColumns);

			}
        }

		return writeDataSqlTemplate;
	}

	private static Set<String> getSkipColumns(Connection conn, String tableName) {
		String sql = "show index from " + tableName;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			Map<String, Set<String>> uniqueKeys = new HashMap<String, Set<String>>();
			while (rs.next()) {
				String nonUnique = rs.getString("Non_unique");
				if (!"0".equals(nonUnique)) {
					continue;
				}
				String keyName = rs.getString("Key_name");
				String columnName = StringUtils.upperCase(rs.getString("Column_name"));
				Set<String> s = uniqueKeys.get(keyName);
				if (s == null) {
					s = new HashSet();
					uniqueKeys.put(keyName, s);
				}
				s.add(columnName);
			}
			// If the table has only one primary/unique key, just skip the column in the update list,
			// it is safe since this primary/unique key does not change when the data in this inserting
			// row conflicts with existing values.
			if (uniqueKeys.size() == 1) {
				return uniqueKeys.values().iterator().next();
            } else if (uniqueKeys.size() > 1) {
                // If this table has more than one primary/unique keys, then just skip the common columns in
                // all primary/unique keys. These columns can be found in every the primary/unique keys so they
                // must be intact when there are at least one primary/unique key conflicts between the new
                // data and existing data. So keeping them unchanged is safe.
                //
                // We can not skip all the columns in primary/unique keys because there might be some fields
                // which do not conflict with existing value. If we skip them in the update list of the INSERT
                // statement, these fields will not get updated, then we will have some fields with new values
                // while some with old values in the same row, which breaks data consistency.
                Iterator<String> keyNameIterator = uniqueKeys.keySet().iterator();
                Set<String> skipColumns = uniqueKeys.get(keyNameIterator.next());
                while(keyNameIterator.hasNext()) {
                    skipColumns.retainAll(uniqueKeys.get(keyNameIterator.next()));
                }
                return skipColumns;
            }
		} catch (Throwable e) {
			LOG.error("show index from table fail :" + sql, e);
		} finally {
			asyncClose(rs, stmt, null);
		}
		return Collections.emptySet();
	}

	/*
	 * build ON DUPLICATE KEY UPDATE sub clause from updateColumns user specified
	 */
	private static String onDuplicateKeyUpdateString(String updateColumns) {
		if (updateColumns == null || updateColumns.length() < 1) {
			return "";
		}
		StringBuilder builder = new StringBuilder();
		builder.append(" ON DUPLICATE KEY UPDATE ");
		List<String> list = new ArrayList<String>();
		for (String column : updateColumns.split(",")) {
			list.add(column + "=VALUES(" + column + ")");
		}
		builder.append(StringUtils.join(list, ','));
		return builder.toString();
	}

	private static String onDuplicateKeyUpdateString(List<String> columnHolders, Set<String> skipColumns) {
		if (columnHolders == null || columnHolders.size() < 1) {
			return "";
		}
		StringBuilder builder = new StringBuilder();
		builder.append(" ON DUPLICATE KEY UPDATE ");
		List<String> list = new ArrayList<String>();
		for (String column : columnHolders) {
			// skip update columns
			if (skipColumns.contains(column.toUpperCase())) {
				continue;
			}
			list.add(column + "=VALUES(" + column + ")");
		}
		if (!list.isEmpty()) {
			builder.append(StringUtils.join(list, ','));
		} else {
			// 如果除了UK 没有别的字段,则更新第一个字段
			String column = columnHolders.get(0);
			builder.append(column + "=VALUES(" + column + ")");
		}
		return builder.toString();
	}

	/**
	 * 休眠n毫秒
	 * 
	 * @param ms
	 *            毫秒
	 */
	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}

	/**
	 * 致命错误
	 * 
	 * @param e
	 * @return
	 */
	public static boolean isFatalError(SQLException e) {
		String sqlState = e.getSQLState();
		if (StringUtils.startsWith(sqlState, "08")) {
			return true;
		}
		final int errorCode = Math.abs(e.getErrorCode());
		switch (errorCode) {
		// Communications Errors
		case 1040: // ER_CON_COUNT_ERROR
		case 1042: // ER_BAD_HOST_ERROR
		case 1043: // ER_HANDSHAKE_ERROR
		case 1047: // ER_UNKNOWN_COM_ERROR
		case 1081: // ER_IPSOCK_ERROR
		case 1129: // ER_HOST_IS_BLOCKED
		case 1130: // ER_HOST_NOT_PRIVILEGED
			// Authentication Errors
		case 1045: // ER_ACCESS_DENIED_ERROR
			// Resource errors
		case 1004: // ER_CANT_CREATE_FILE
		case 1005: // ER_CANT_CREATE_TABLE
		case 1015: // ER_CANT_LOCK
		case 1021: // ER_DISK_FULL
		case 1041: // ER_OUT_OF_RESOURCES
		case 1094: // Unknown thread id: %lu
			// Out-of-memory errors
		case 1037: // ER_OUTOFMEMORY
		case 1038: // ER_OUT_OF_SORTMEMORY
			return true;
		}

		if (StringUtils.isNotBlank(e.getMessage())) {
			final String errorText = e.getMessage().toUpperCase();

			if (errorCode == 0
					&& (errorText.indexOf("COMMUNICATIONS LINK FAILURE") > -1
							|| errorText.indexOf("COULD NOT CREATE CONNECTION") > -1)
					|| errorText.indexOf("NO DATASOURCE") > -1 || errorText.indexOf("NO ALIVE DATASOURCE") > -1
					|| errorText.indexOf("NO OPERATIONS ALLOWED AFTER CONNECTION CLOSED") > -1) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 可恢复的错误
	 * 
	 * @param e
	 * @return
	 */
	public static boolean isRecoverableError(SQLException e) {
		int error = Math.abs(e.getErrorCode());
		// 明确可恢复
		if (white.contains(error)) {
			return true;
		}
		// 明确不可恢复
		if (black.contains(error)) {
			return false;
		}
		// 超过4000的,都是OB特有的ErrorCode
		return error > 4020;
	}

	private static Set<Integer> white = new HashSet<Integer>();
	static {
		int[] errList = { 1213, 1047, 1041, 1094, 4000, 4012 };
		for (int err : errList) {
			white.add(err);
		}
	}
	// 不考虑4000以下的
	private static Set<Integer> black = new HashSet<Integer>();
	static {
		int[] errList = { 4022, 4025, 4026, 4028, 4029, 4031, 4033, 4034, 4037, 4041, 4044 };
		for (int err : errList) {
			black.add(err);
		}
	}

	/**
	 * 由于ObProxy存在bug,事务超时或事务被杀时,conn的close是没有响应的
	 * 
	 * @param rs
	 * @param stmt
	 * @param conn
	 */
	public static void asyncClose(final ResultSet rs, final Statement stmt, final Connection conn) {
		Thread t = new Thread() {
			public void run() {
				DBUtil.closeDBResources(rs, stmt, conn);
			}
		};
		t.setDaemon(true);
		t.start();
	}
}
