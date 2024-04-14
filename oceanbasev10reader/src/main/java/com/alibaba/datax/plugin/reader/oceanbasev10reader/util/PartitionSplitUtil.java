package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.ObVersion;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.ext.ObReaderKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author johnrobbet
 */
public class PartitionSplitUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionSplitUtil.class);

    private static final String ORACLE_GET_SUBPART_TEMPLATE =
        "select subpartition_name "
            + "from dba_tab_subpartitions "
            + "where table_name = '%s' and table_owner = '%s'";

    private static final String ORACLE_GET_PART_TEMPLATE =
        "select partition_name "
            + "from dba_tab_partitions "
            + "where table_name = '%s' and table_owner = '%s'";

    private static final String MYSQL_GET_PART_TEMPLATE =
        "select p.part_name "
            + "from oceanbase.__all_part p, oceanbase.%s t, oceanbase.__all_database d "
            + "where p.table_id = t.table_id "
            + "and d.database_id = t.database_id "
            + "and d.database_name = '%s' "
            + "and t.table_name = '%s'";

    private static final String MYSQL_GET_SUBPART_TEMPLATE =
        "select p.sub_part_name "
            + "from oceanbase.__all_sub_part p, oceanbase.%s t, oceanbase.__all_database d "
            + "where p.table_id = t.table_id "
            + "and d.database_id = t.database_id "
            + "and d.database_name = '%s' "
            + "and t.table_name = '%s'";

    /**
     * get partition info from data dictionary in ob oracle mode
     * @param config
     * @param tableName
     * @return
     */
    public static PartInfo getObOraclePartInfoBySQL(Configuration config, String tableName) {
        PartInfo partInfo;
        DataBaseType dbType = ObReaderUtils.databaseType;
        String jdbcUrl = config.getString(Key.JDBC_URL);
        String username = config.getString(Key.USERNAME);
        String password = config.getString(Key.PASSWORD);
        String dbname = ObReaderUtils.getDbNameFromJdbcUrl(jdbcUrl).toUpperCase();
        Connection conn = DBUtil.getConnection(dbType, jdbcUrl, username, password);
        tableName = tableName.toUpperCase();

        // check if the table has subpartitions or not
        String getSubPartSql = String.format(ORACLE_GET_SUBPART_TEMPLATE, tableName, dbname);
        List<String> partList = ObReaderUtils.getResultsFromSql(conn, getSubPartSql);
        if (partList != null && partList.size() > 0) {
            partInfo = new PartInfo(PartType.SUBPARTITION);
            partInfo.addPart(partList);
            return partInfo;
        }

        String getPartSql = String.format(ORACLE_GET_PART_TEMPLATE, tableName, dbname);
        partList = ObReaderUtils.getResultsFromSql(conn, getPartSql);
        if (partList != null && partList.size() > 0) {
            partInfo = new PartInfo(PartType.PARTITION);
            partInfo.addPart(partList);
            return partInfo;
        }

        // table is not partitioned
        partInfo = new PartInfo(PartType.NONPARTITION);
        return partInfo;
    }

    public static List<Configuration> splitByPartition (Configuration configuration) {
        List<Configuration> allSlices = new ArrayList<>();
        List<Object> connections = configuration.getList(Constant.CONN_MARK, Object.class);
        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration sliceConfig = configuration.clone();
            Configuration connConf = Configuration.from(connections.get(i).toString());
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
            LOG.info("table is not partitioned.");

            Configuration slice = configuration.clone();
            slice.set(Key.QUERY_SQL, ObReaderUtils.buildQuerySql(weakRead, column, table, where));
            slices.add(slice);
        }

        return slices;
    }

    public static PartInfo getObPartInfoBySQL(Configuration config, String table) {
        boolean isOracleMode = config.getString(ObReaderKey.OB_COMPATIBILITY_MODE).equals("ORACLE");
        if (isOracleMode) {
            return getObOraclePartInfoBySQL(config, table);
        } else {
            return getObMySQLPartInfoBySQL(config, table);
        }
    }

    public static PartInfo getObMySQLPartInfoBySQL(Configuration config, String table) {
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
            ObVersion obVersion = ObReaderUtils.getObVersion(conn);
            if (obVersion.compareTo(ObVersion.V2276) >= 0 &&
                obVersion.compareTo(ObVersion.V4000) < 0) {
                allTable = "__all_table_v2";
            }

            String querySubPart = String.format(MYSQL_GET_SUBPART_TEMPLATE, allTable, dbname, table);

            PartType partType = PartType.SUBPARTITION;

            // try subpartition first
            partList = ObReaderUtils.getResultsFromSql(conn, querySubPart);

            // if table is not sub-partitioned, the try partition
            if (partList.isEmpty()) {
                String queryPart = String.format(MYSQL_GET_PART_TEMPLATE, allTable, dbname, table);
                partList = ObReaderUtils.getResultsFromSql(conn, queryPart);
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
}
