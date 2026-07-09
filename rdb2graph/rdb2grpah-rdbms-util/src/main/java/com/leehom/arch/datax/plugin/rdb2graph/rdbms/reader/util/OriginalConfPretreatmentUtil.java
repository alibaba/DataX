package com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Constant;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Key;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader.Phase;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.DBUtil;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.DBUtilErrorCode;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.DataBaseType;
import com.leehom.arch.datax.plugin.rdb2graph.rdbms.util.TableExpandUtil;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.SchemaUtils;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.PKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;

/**
 * @类名: OriginalConfPretreatmentUtil
 * @说明: 原生配置预处理
 *
 * @author
 * @Date 2022年4月29日 上午10:15:44 修改记录：
 *
 * @see
 * 
 * TODO
 *  1. 封装独立图配置预处理
 *  
 */
public final class OriginalConfPretreatmentUtil {
	private static final Logger LOG = LoggerFactory.getLogger(OriginalConfPretreatmentUtil.class);

	public static DataBaseType DATABASE_TYPE;
	
	/**
	 * @说明：原生配置预处理，这里增加我们关系转图配置
	 *        1. table2node 表模式
	 *        2. fk2rel querySql模式
	 *
	 * @author leehom
	 * @param originalConfig
	 * 
	 */
	public static void doPretreatment(Configuration originalConfig, DbSchema rdbSchema) {
		// 检查 username/password 配置（必填）
		originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
		originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);
		// 
		graphconf(originalConfig, rdbSchema);
		//
		dealWhere(originalConfig);
		//
		simplifyConf(originalConfig);
	}
	
	//
	private static void graphconf(Configuration originalConfig, DbSchema rdbSchema) {
		String phaseStr = originalConfig.getString(Key.GRAPH_PHASE);
		Phase phase = Phase.valueOf(phaseStr.toUpperCase());
		if(phase==Phase.TABLE)
			graphconfTable(originalConfig, rdbSchema);
		if(phase==Phase.REL)
			graphconfRel(originalConfig, rdbSchema);
	}

	private static void graphconfTable(Configuration originalConfig, DbSchema rdbSchema) {
		// 连接配置集合
		List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
		// 遍历
		for (int i = 0, len = conns.size(); i < len; i++) {
			// 获取表
			List<String> tables = originalConfig.getList(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.TABLE), String.class);
			// 设置了table不使用schema，这样支持表内分片
			if(tables.isEmpty()) {
				//
				String[] tns = SchemaUtils.extractTableNames("", rdbSchema.getTables());
				//
				originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.TABLE), tns);
			}
		}
	}
	
	private static void graphconfRel(Configuration originalConfig, DbSchema rdbSchema) {
		// 连接配置集合
		List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
		List<TableMetadata> tables = rdbSchema.getTables();
		for (int i = 0, len = conns.size(); i < len; i++) {
			//
			List<String> querySqls = new ArrayList<>();
			List<String> relFroms = new ArrayList<>();
			List<String> relFks = new ArrayList<>();
			// 按表遍历，但数量是按fk
			for(int j=0;j<rdbSchema.getTables().size();j++) {
				TableMetadata table= tables.get(j);
				// 连接表
				if(table.isLinkTable()) {
					// 保证结果集字段顺序
					String selectSql = SchemaUtils.extractFieldNames(table.getFields());
					String querySql = "select " + selectSql + " from " + table.getName();
					querySqls.add(querySql);
					relFroms.add(table.getName());
					relFks.add(table.getLinkFrom().getFkName());
				} else { // 其他表
					List<FKConstraintMetadata> fks = table.getFks();
					for(FKConstraintMetadata fk : fks) {
						// 构建关系 querySql模式
						// 占位
						PKConstraintMetadata pk = table.getPk();
						// 0：主键，1：外键，2：表
						String relFromFields = SchemaUtils.extractFieldNames(pk.getFields());
					    // fk字段不为空
					    String whereSql = SchemaUtils.extractFieldWhereNotNull(fk.getFields(), " and ");
					    String relToFields = SchemaUtils.extractFieldNames(fk.getFields());
						String querySql = MessageFormat.format(Constant.REL_QUERY_SQL_PATTERN, 
								relFromFields, relToFields, table.getName(), whereSql);
						querySqls.add(querySql);
						relFroms.add(table.getName());
						relFks.add(fk.getFkName());
					}	
				}
			}
			// 写入connection节点
			originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.QUERY_SQL), querySqls);
			originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Constant.REL_FROM), relFroms);
			originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Constant.REL_FK), relFks);
		}
		
	}
	
	// 获取表邻接数组位置
	private static int locate(String tableName, List<TableMetadata> tables) {
		int l = 0;
		for(TableMetadata table : tables) {
			if(tableName.equals(table.getName()))
				break;
			l++;
		}
		return l;
			
	}
	
	public static void dealWhere(Configuration originalConfig) {
		String where = originalConfig.getString(Key.WHERE, null);
		if (StringUtils.isNotBlank(where)) {
			String whereImprove = where.trim();
			if (whereImprove.endsWith(";") || whereImprove.endsWith("；")) {
				whereImprove = whereImprove.substring(0, whereImprove.length() - 1);
			}
			originalConfig.set(Key.WHERE, whereImprove);
		}
	}

	/**
	 * 对配置进行初步处理：
	 * <ol>
	 * <li>处理同一个数据库配置了多个jdbcUrl的情况</li>
	 * <li>识别并标记是采用querySql 模式还是 table 模式</li>
	 * <li>对 table 模式，确定分表个数，并处理 column 转 *事项</li>
	 * </ol>
	 */
	private static void simplifyConf(Configuration originalConfig) {
		boolean isTableMode = recognizeTableOrQuerySqlMode(originalConfig);
		originalConfig.set(Constant.IS_TABLE_MODE, isTableMode);
		//
		dealJdbcAndTable(originalConfig);
		dealColumnConf(originalConfig);
	}
	
	private static void dealJdbcAndTable(Configuration originalConfig) {
		String username = originalConfig.getString(Key.USERNAME);
		String password = originalConfig.getString(Key.PASSWORD);
		boolean checkSlave = originalConfig.getBool(Key.CHECK_SLAVE, false);
		boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);
		boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);

		List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
		List<String> preSql = originalConfig.getList(Key.PRE_SQL, String.class);

		int tableNum = 0;
		// 遍历连接配置
		for (int i = 0, len = conns.size(); i < len; i++) {
			Configuration connConf = Configuration.from(conns.get(i).toString());

			connConf.getNecessaryValue(Key.JDBC_URL, DBUtilErrorCode.REQUIRED_VALUE);
			List<String> jdbcUrls = connConf.getList(Key.JDBC_URL, String.class);

			String jdbcUrl;
			if (isPreCheck) {
				jdbcUrl = DBUtil.chooseJdbcUrlWithoutRetry(DATABASE_TYPE, jdbcUrls, username, password, preSql,
						checkSlave);
			} else {
				jdbcUrl = DBUtil.chooseJdbcUrl(DATABASE_TYPE, jdbcUrls, username, password, preSql, checkSlave);
			}

			jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);

			// 回写到connection[i].jdbcUrl
			originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL), jdbcUrl);

			LOG.info("Available jdbcUrl:{}.", jdbcUrl);

			if (isTableMode) {
				// table 方式
				// 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
				List<String> tables = connConf.getList(Key.TABLE, String.class);
				// 支持分库分表
				List<String> expandedTables = TableExpandUtil.expandTableConf(DATABASE_TYPE, tables);
				if (null == expandedTables || expandedTables.isEmpty()) {
					throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
							String.format("您所配置的读取数据库表:%s 不正确. 因为DataX根据您的配置找不到这张表. 请检查您的配置并作出修改." + "请先了解 DataX 配置.",
									StringUtils.join(tables, ",")));
				}
				tableNum += expandedTables.size();

				originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.TABLE), expandedTables);
			} else {
				// 说明是配置的 querySql 方式，不做处理.
			}
		}

		originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
	}

	// 列配置处理
	private static void dealColumnConf(Configuration originalConfig) {
		boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);

		List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);

		if (isTableMode) {
			if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
				throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置读取数据库表的列信息. "
						+ "正确的配置方式是给 column 配置上您需要读取的列名称,用英文逗号分隔. 例如: \"column\": [\"id\", \"name\"],请参考上述配置并作出修改.");
			} else {
				String splitPk = originalConfig.getString(Key.SPLIT_PK, null);

				if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
					LOG.warn("您的配置文件中的列配置存在一定的风险. 因为您未配置读取数据库表的列，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");
					// 回填其值，需要以 String 的方式转交后续处理
					originalConfig.set(Key.COLUMN, "*");
				} else {
					String jdbcUrl = originalConfig
							.getString(String.format("%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL));

					String username = originalConfig.getString(Key.USERNAME);
					String password = originalConfig.getString(Key.PASSWORD);

					String tableName = originalConfig
							.getString(String.format("%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

					List<String> allColumns = DBUtil.getTableColumns(DATABASE_TYPE, jdbcUrl, username, password,
							tableName);
					LOG.info("table:[{}] has columns:[{}].", tableName, StringUtils.join(allColumns, ","));
					// warn:注意mysql表名区分大小写
					allColumns = ListUtil.valueToLowerCase(allColumns);
					List<String> quotedColumns = new ArrayList<String>();

					for (String column : userConfiguredColumns) {
						if ("*".equals(column)) {
							throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
									"您的配置文件中的列配置信息有误. 因为根据您的配置，数据库表的列中存在多个*. 请检查您的配置并作出修改. ");
						}

						quotedColumns.add(column);
						// 以下判断没有任何意义
//                        if (null == column) {
//                            quotedColumns.add(null);
//                        } else {
//                            if (allColumns.contains(column.toLowerCase())) {
//                                quotedColumns.add(column);
//                            } else {
//                                // 可能是由于用户填写为函数，或者自己对字段进行了`处理或者常量
//                            	quotedColumns.add(column);
//                            }
//                        }
					}

					originalConfig.set(Key.COLUMN_LIST, quotedColumns);
					originalConfig.set(Key.COLUMN, StringUtils.join(quotedColumns, ","));
					if (StringUtils.isNotBlank(splitPk)) {
						if (!allColumns.contains(splitPk.toLowerCase())) {
							throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
									String.format("您的配置文件中的列配置信息有误. 因为根据您的配置，您读取的数据库表:%s 中没有主键名为:%s. 请检查您的配置并作出修改.",
											tableName, splitPk));
						}
					}

				}
			}
		} else {
			// querySql模式，不希望配制 column，那样是混淆不清晰的
			if (null != userConfiguredColumns && userConfiguredColumns.size() > 0) {
				LOG.warn("您的配置有误. 由于您读取数据库表采用了querySql的方式, 所以您不需要再配置 column. 如果您不想看到这条提醒，请移除您源头表中配置中的 column.");
				originalConfig.remove(Key.COLUMN);
			}

			// querySql模式，不希望配制 where，那样是混淆不清晰的
			String where = originalConfig.getString(Key.WHERE, null);
			if (StringUtils.isNotBlank(where)) {
				LOG.warn("您的配置有误. 由于您读取数据库表采用了querySql的方式, 所以您不需要再配置 where. 如果您不想看到这条提醒，请移除您源头表中配置中的 where.");
				originalConfig.remove(Key.WHERE);
			}

			// querySql模式，不希望配制 splitPk，那样是混淆不清晰的
			String splitPk = originalConfig.getString(Key.SPLIT_PK, null);
			if (StringUtils.isNotBlank(splitPk)) {
				LOG.warn("您的配置有误. 由于您读取数据库表采用了querySql的方式, 所以您不需要再配置 splitPk. 如果您不想看到这条提醒，请移除您源头表中配置中的 splitPk.");
				originalConfig.remove(Key.SPLIT_PK);
			}
		}

	}

	// 识别是表模式还是querySql模式
	// 验证
	private static boolean recognizeTableOrQuerySqlMode(Configuration originalConfig) {
		List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
		//
		List<Boolean> tableModeFlags = new ArrayList<Boolean>();
		List<Boolean> querySqlModeFlags = new ArrayList<Boolean>();

		String table = null;
		String querySql = null;

		boolean isTableMode = false;
		boolean isQuerySqlMode = false;
		//
		for (int i = 0, len = conns.size(); i < len; i++) {
			Configuration connConf = Configuration.from(conns.get(i).toString());
			table = connConf.getString(Key.TABLE, null);
			querySql = connConf.getString(Key.QUERY_SQL, null);

			isTableMode = StringUtils.isNotBlank(table);
			tableModeFlags.add(isTableMode);

			isQuerySqlMode = StringUtils.isNotBlank(querySql);
			querySqlModeFlags.add(isQuerySqlMode);

			if (false == isTableMode && false == isQuerySqlMode) {
				// table 和 querySql 二者均未配制
				throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MISSING,
						"配置有误. 因为table和querySql应该配置并且只能配置一个. 请检查您的配置并作出修改.");
			} else if (true == isTableMode && true == isQuerySqlMode) {
				// table 和 querySql 二者均配置
				throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
						"配置凌乱了. 因为datax不能同时既配置table又配置querySql.请检查您的配置并作出修改.");
			}
		}
		// 混合配制 table 和 querySql
		if (!ListUtil.checkIfValueSame(tableModeFlags) || !ListUtil.checkIfValueSame(tableModeFlags)) {
			throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
					"配置凌乱了. 不能同时既配置table又配置querySql. 请检查您的配置并作出修改.");
		}
		return tableModeFlags.get(0);
	}

}
