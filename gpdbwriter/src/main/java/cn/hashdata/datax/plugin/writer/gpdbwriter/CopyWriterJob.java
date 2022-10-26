package cn.hashdata.datax.plugin.writer.gpdbwriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;

public class CopyWriterJob extends CommonRdbmsWriter.Job {
	private static final Logger LOG = LoggerFactory.getLogger(CopyWriterJob.class);
	private List<String> tables = null;

	public CopyWriterJob() {
		super(DataBaseType.PostgreSQL);
	}

	@Override
	public void init(Configuration originalConfig) {
		super.init(originalConfig);
	}

	@Override
	public void prepare(Configuration originalConfig) {
		String username = originalConfig.getString(Key.USERNAME);
		String password = originalConfig.getString(Key.PASSWORD);

		List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
		Configuration connConf = Configuration.from(conns.get(0).toString());

		// 这里的 jdbcUrl 已经 append 了合适后缀参数
		String jdbcUrl = connConf.getString(Key.JDBC_URL);
		tables = connConf.getList(Key.TABLE, String.class);

		Connection conn = DBUtil.getConnection(DataBaseType.PostgreSQL, jdbcUrl, username, password);

		List<String> sqls = new ArrayList<String>();

		for (String table : tables) {
			sqls.add("SELECT gp_truncate_error_log('" + table + "');");
			LOG.info("为 {} 清理 ERROR LOG. context info:{}.", table, jdbcUrl);
		}

		WriterUtil.executeSqls(conn, sqls, jdbcUrl, DataBaseType.PostgreSQL);
		DBUtil.closeDBResources(null, null, conn);

		super.prepare(originalConfig);
	}

	@Override
	public void post(Configuration originalConfig) {
		super.post(originalConfig);

		String username = originalConfig.getString(Key.USERNAME);
		String password = originalConfig.getString(Key.PASSWORD);

		// 已经由 prepare 进行了appendJDBCSuffix处理
		String jdbcUrl = originalConfig.getString(Key.JDBC_URL);

		Connection conn = DBUtil.getConnection(DataBaseType.PostgreSQL, jdbcUrl, username, password);

		for (String table : tables) {
			int errors = 0;
			ResultSet res = null;
			String sql = "SELECT count(*) from gp_read_error_log('" + table + "');";

			try {
				res = DBUtil.query(conn, sql, 10);
				if (res.next()) {
					errors = res.getInt(1);
				}
				res.close();
				conn.commit();
			} catch (SQLException e) {
				LOG.debug("Fail to get error log info:" + e.getMessage());
			}

			if (errors > 0) {
				LOG.warn("加载表 {} 时发现 {} 条错误数据, 使用 \"SELECT * from gp_read_error_log('{}');\" 查看详情", table,
						errors, table);
			}
		}

		DBUtil.closeDBResources(null, null, conn);
	}
}