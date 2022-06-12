package com.alibaba.datax.plugin.reader.odpsreader.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.reader.odpsreader.Constant;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqliteUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqliteUtil.class);

    private Connection connection = null;
    private Statement stmt = null;

    private String partitionName = "partitionName";

    private String createSQLTemplate = "Create Table DataXODPSReaderPPR (" + partitionName +" String, %s)";
    private String insertSQLTemplate = "Insert Into DataXODPSReaderPPR Values (%s)";
    private String selectSQLTemplate = "Select * From DataXODPSReaderPPR Where %s";

    public SqliteUtil() throws ClassNotFoundException, SQLException {

        Class.forName("org.sqlite.JDBC");
        this.connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        this.stmt = this.connection.createStatement();
    }

    public void loadAllPartitionsIntoSqlite(Table table, List<String> allOriginPartitions) throws SQLException {
        List<String> partitionColumnList = new ArrayList<String>();
        String partition = allOriginPartitions.get(0);
        String[] partitionSpecs = partition.split(",");
        List<String> partitionKeyList = new ArrayList<String>();
        for (String partitionKeyValue : partitionSpecs) {
            String partitionKey = partitionKeyValue.split("=")[0];
            partitionColumnList.add(String.format("%s String", partitionKey));
            partitionKeyList.add(partitionKey);
        }
        String createSQL = String.format(createSQLTemplate, StringUtils.join(partitionColumnList.toArray(), ","));
        LOGGER.info(createSQL);
        this.stmt.execute(createSQL);

        insertAllOriginPartitionIntoSqlite(table, partitionKeyList);
    }

    /**
     * 根据用户配置的过滤条件, 从sqlite中select出符合的partition列表
     * @param userHintConfiguredPartitions
     * @return
     */
    public List<String> selectUserConfiguredPartition(List<String> userHintConfiguredPartitions) throws SQLException {
        List<String> selectedPartitionsFromSqlite = new ArrayList<String>();
        for (String partitionWhereConditions : userHintConfiguredPartitions) {
            String selectUserConfiguredPartitionsSql = String.format(selectSQLTemplate,
                StringUtils.remove(partitionWhereConditions, Constant.PARTITION_FILTER_HINT));
            LOGGER.info(selectUserConfiguredPartitionsSql);
            ResultSet rs = stmt.executeQuery(selectUserConfiguredPartitionsSql);
            while (rs.next()) {
                selectedPartitionsFromSqlite.add(getPartitionsValue(rs));
            }
        }
        return selectedPartitionsFromSqlite;
    }

    private String getPartitionsValue (ResultSet rs) throws SQLException {
        List<String> partitions = new ArrayList<String>();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        Integer columnCounter = rs.getMetaData().getColumnCount();
        for (int columnIndex = 2; columnIndex <= columnCounter; columnIndex++) {
            partitions.add(String.format("%s=%s", rsMetaData.getColumnName(columnIndex), rs.getString(columnIndex)));
        }
        return StringUtils.join(partitions, ",");
    }

    /**
     * 将odps table里所有partition值载入sqlite中
     * @param table
     * @param partitionKeyList
     * @throws SQLException
     */
    private void insertAllOriginPartitionIntoSqlite(Table table, List<String> partitionKeyList) throws SQLException {
        List<Partition> partitions = table.getPartitions();
        for (Partition partition : partitions){
            List<String> partitionColumnValue = new ArrayList<String>();
            partitionColumnValue.add("\""+partition.getPartitionSpec().toString()+"\"");
            for (String partitionKey : partitionKeyList) {
                partitionColumnValue.add("\""+partition.getPartitionSpec().get(partitionKey)+"\"");
            }
            String insertPartitionValueSql = String.format(insertSQLTemplate, StringUtils.join(partitionColumnValue, ","));
            this.stmt.execute(insertPartitionValueSql);
        }
    }
}
