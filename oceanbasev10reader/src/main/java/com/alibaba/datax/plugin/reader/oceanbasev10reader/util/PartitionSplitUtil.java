package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.HintUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.ObReaderKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author johnrobbet
 */
public class PartitionSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionSplitUtil.class);

    public static List<Configuration> splitByPartition (Configuration configuration) {
        List<Configuration> allSlices = new ArrayList<>();
        List<Object> conns = configuration.getList(Constant.CONN_MARK, Object.class);
        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = configuration.clone();
            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);
            sliceConfig.remove(Constant.CONN_MARK);

            List<String> tables = connConf.getList(Key.TABLE, String.class);
            for (String table : tables) {
                Configuration tempSlice = sliceConfig.clone();
                tempSlice.set(Key.TABLE, table);
                allSlices.addAll(splitSinglePartitionTable(tempSlice));
            }
        }

        return allSlices;
    }

    private static List<Configuration> splitSinglePartitionTable(Configuration configuration) {
        String table = configuration.getString(Key.TABLE);
        String where = configuration.getString(Key.WHERE, null);
        String column = configuration.getString(Key.COLUMN);
        final boolean weakRead = configuration.getBool(Key.WEAK_READ, true);

        List<Configuration> slices = new ArrayList();
        PartInfo partInfo = getObPartInfoBySQL(configuration, table);
        if (partInfo != null && partInfo.isPartitionTable()) {
            String partitionType = partInfo.getPartType();
            for (String partitionName : partInfo.getPartList()) {
                LOG.info(String.format("add %s %s for table %s", partitionType, partitionName, table));
                Configuration slice = configuration.clone();
                slice.set(ObReaderKey.PARTITION_NAME, partitionName);
                slice.set(ObReaderKey.PARTITION_TYPE, partitionType);
                slice.set(Key.QUERY_SQL,
                    ObReaderUtils.buildQuerySql(weakRead, column,
                        String.format("%s partition(%s)", table, partitionName), where));
                slices.add(slice);
            }
        } else {
            LOG.info("fail to get table part info or table is not partitioned, proceed as non-partitioned table.");

            Configuration slice = configuration.clone();
            slice.set(Key.QUERY_SQL, ObReaderUtils.buildQuerySql(weakRead, column, table, where));
            slices.add(slice);
        }

        return slices;
    }

    private static PartInfo getObPartInfoBySQL(Configuration config, String table) {
        PartInfo partInfo = new PartInfo(PartType.NONPARTITION);
        List<String> partList;
        Connection conn = null;
        try {
            String jdbcUrl = config.getString(Key.JDBC_URL);
            String username = config.getString(Key.USERNAME);
            String password = config.getString(Key.PASSWORD);
            String dbname = ObReaderUtils.getDbNameFromJdbcUrl(jdbcUrl);
            String allTable = "__all_table";

            conn = DBUtil.getConnection(DataBaseType.OceanBase, jdbcUrl, username, password);
            String obVersion = getResultsFromSql(conn, "select version()").get(0);

            LOG.info("obVersion: " + obVersion);

            if (ObReaderUtils.compareObVersion("2.2.76", obVersion) < 0) {
                allTable = "__all_table_v2";
            }

            String queryPart = String.format(
                "select p.part_name " +
                    "from oceanbase.__all_part p, oceanbase.%s t, oceanbase.__all_database d " +
                    "where p.table_id = t.table_id " +
                    "and d.database_id = t.database_id " +
                    "and d.database_name = '%s' " +
                    "and t.table_name = '%s'", allTable, dbname, table);
            String querySubPart = String.format(
                "select p.sub_part_name " +
                    "from oceanbase.__all_sub_part p, oceanbase.%s t, oceanbase.__all_database d " +
                    "where p.table_id = t.table_id " +
                    "and d.database_id = t.database_id " +
                    "and d.database_name = '%s' " +
                    "and t.table_name = '%s'", allTable, dbname, table);
            if (config.getString(ObReaderKey.OB_COMPATIBILITY_MODE).equals("ORACLE")) {
                queryPart = String.format(
                    "select partition_name from all_tab_partitions where TABLE_OWNER = '%s' and table_name = '%s'",
                    dbname.toUpperCase(), table.toUpperCase());
                querySubPart = String.format(
                    "select subpartition_name from all_tab_subpartitions where TABLE_OWNER = '%s' and table_name = '%s'",
                    dbname.toUpperCase(), table.toUpperCase());
            }

            PartType partType = PartType.SUBPARTITION;

            // try subpartition first
            partList = getResultsFromSql(conn, querySubPart);

            // if table is not sub-partitioned, the try partition
            if (partList.isEmpty()) {
                partList = getResultsFromSql(conn, queryPart);
                partType = PartType.PARTITION;
            }

            if (!partList.isEmpty()) {
                partInfo = new PartInfo(partType);
                partInfo.addPart(partList);
            }
        } catch (Exception ex) {
            LOG.error("error when get partition list: " + ex.getMessage());
        } finally {
            DBUtil.closeDBResources(null, conn);
        }

        return partInfo;
    }

    private static List<String> getResultsFromSql(Connection conn, String sql) {
        List<String> list = new ArrayList();
        Statement stmt = null;
        ResultSet rs = null;

        LOG.info("executing sql: " + sql);

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                list.add(rs.getString(1));
            }
        } catch (Exception e) {
            LOG.error("error when executing sql: " + e.getMessage());
        } finally {
            DBUtil.closeDBResources(rs, stmt, null);
        }

        return list;
    }
}
