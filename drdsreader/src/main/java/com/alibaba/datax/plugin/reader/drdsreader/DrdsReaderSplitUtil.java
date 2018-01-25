package com.alibaba.datax.plugin.reader.drdsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

public class DrdsReaderSplitUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(DrdsReaderSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig,
                                              int adviceNumber) {
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
        int tableNumber = originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK);

        if (isTableMode && tableNumber == 1) {
            //需要先把内层的 table,connection 先放到外层
            String table = originalSliceConfig.getString(String.format("%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)).trim();
            originalSliceConfig.set(Key.TABLE, table);

            //注意：这里的 jdbcUrl 不是从数组中获取的，因为之前的  master init 方法已经进行过预处理
            String jdbcUrl = originalSliceConfig.getString(String.format("%s[0].%s", Constant.CONN_MARK, Key.JDBC_URL)).trim();

            originalSliceConfig.set(Key.JDBC_URL, DataBaseType.DRDS.appendJDBCSuffixForReader(jdbcUrl));

            originalSliceConfig.remove(Constant.CONN_MARK);
            return doDrdsReaderSplit(originalSliceConfig);
        } else {
            throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR, "您的配置信息中的表(table)的配置有误. 因为Drdsreader 只需要读取一张逻辑表,后台会通过DRDS Proxy自动获取实际对应物理表的数据. 请检查您的配置并作出修改.");
        }
    }

    private static List<Configuration> doDrdsReaderSplit(Configuration originalSliceConfig) {
        List<Configuration> splittedConfigurations = new ArrayList<Configuration>();

        Map<String, List<String>> topology = getTopology(originalSliceConfig);
        if (null == topology || topology.isEmpty()) {
            throw DataXException.asDataXException(DrdsReaderErrorCode.GET_TOPOLOGY_FAILED,
                    "获取 drds 表拓扑结构失败, 拓扑结构不能为空.");
        } else {
            String table = originalSliceConfig.getString(Key.TABLE).trim();
            String column = originalSliceConfig.getString(Key.COLUMN).trim();
            String where = originalSliceConfig.getString(Key.WHERE, null);
            // 不能带英语分号结尾
            String sql = SingleTableSplitUtil
                    .buildQuerySql(column, table, where);
            // 根据拓扑拆分任务
            for (Map.Entry<String, List<String>> entry : topology.entrySet()) {
                String group = entry.getKey();
                StringBuilder sqlbuilder = new StringBuilder();
                sqlbuilder.append("/*+TDDL({'extra':{'MERGE_UNION':'false'},'type':'direct',");
                sqlbuilder.append("'vtab':'").append(table).append("',");
                sqlbuilder.append("'dbid':'").append(group).append("',");
                sqlbuilder.append("'realtabs':[");
                Iterator<String> it = entry.getValue().iterator();
                while (it.hasNext()) {
                    String realTable = it.next();
                    sqlbuilder.append('\'').append(realTable).append('\'');
                    if (it.hasNext()) {
                        sqlbuilder.append(',');
                    }
                }
                sqlbuilder.append("]})*/");
                sqlbuilder.append(sql);
                Configuration param = originalSliceConfig.clone();
                param.set(Key.QUERY_SQL, sqlbuilder.toString());
                splittedConfigurations.add(param);
            }

            return splittedConfigurations;
        }
    }


    private static Map<String, List<String>> getTopology(Configuration configuration) {
        Map<String, List<String>> topology = new HashMap<String, List<String>>();

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String logicTable = configuration.getString(Key.TABLE).trim();

        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = DBUtil.getConnection(DataBaseType.DRDS, jdbcURL, username, password);
            rs = DBUtil.query(conn, "SHOW TOPOLOGY " + logicTable);
            while (DBUtil.asyncResultSetNext(rs)) {
                String groupName = rs.getString("GROUP_NAME");
                String tableName = rs.getString("TABLE_NAME");
                List<String> tables = topology.get(groupName);
                if (tables == null) {
                    tables = new ArrayList<String>();
                    topology.put(groupName, tables);
                }
                tables.add(tableName);
            }

            return topology;
        } catch (Exception e) {
            throw DataXException.asDataXException(DrdsReaderErrorCode.GET_TOPOLOGY_FAILED,
                    String.format("获取 drds 表拓扑结构失败.根据您的配置, datax获取不到拓扑信息。相关上下文信息:表:%s, jdbcUrl:%s . 请联系 drds 管理员处理.", logicTable, jdbcURL), e);
        } finally {
            DBUtil.closeDBResources(rs, null, conn);
        }
    }

}

