package com.alibaba.datax.plugin.rdbms.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TableExpandUtil {

	// schema.table[0-2]more
	// 1 2 3 4 5
	public static Pattern pattern = Pattern
			.compile("(\\w+\\.)?(\\w+)\\[(\\d+)-(\\d+)\\](.*)");

	private TableExpandUtil() {
	}

	/**
	 * Split the table string(Usually contains names of some tables) to a List
	 * that is formated. example: table[0-32] will be splitted into `table0`,
	 * `table1`, `table2`, ... ,`table32` in {@link List}
	 *
	 * @param tables
	 *            a string contains table name(one or many).
	 * @return a split result of table name.
	 *         <p/>
	 *         TODO 删除参数 DataBaseType
	 */
	public static List<String> splitTables(DataBaseType dataBaseType,
			String tables) {
		List<String> splittedTables = new ArrayList<String>();

		String[] tableArrays = tables.split(",");

		String tableName = null;
		for (String tableArray : tableArrays) {
			Matcher matcher = pattern.matcher(tableArray.trim());
			if (!matcher.matches()) {
				tableName = tableArray.trim();
				splittedTables.add(tableName);
			} else {
				String start = matcher.group(3).trim();
				String end = matcher.group(4).trim();
				String tmp = "";
				if (Integer.valueOf(start) > Integer.valueOf(end)) {
					tmp = start;
					start = end;
					end = tmp;
				}
				int len = start.length();
				String schema = null;
				for (int k = Integer.valueOf(start); k <= Integer.valueOf(end); k++) {
					schema = (null == matcher.group(1)) ? "" : matcher.group(1)
							.trim();
					if (start.startsWith("0")) {
						tableName = schema + matcher.group(2).trim()
								+ String.format("%0" + len + "d", k)
								+ matcher.group(5).trim();
						splittedTables.add(tableName);
					} else {
						tableName = schema + matcher.group(2).trim()
								+ String.format("%d", k)
								+ matcher.group(5).trim();
						splittedTables.add(tableName);
					}
				}
			}
		}
		return splittedTables;
	}

	public static List<String> expandTableConf(DataBaseType dataBaseType,
			List<String> tables) {
		List<String> parsedTables = new ArrayList<String>();
		for (String table : tables) {
			List<String> splittedTables = splitTables(dataBaseType, table);
			parsedTables.addAll(splittedTables);
		}

		return parsedTables;
	}

}
