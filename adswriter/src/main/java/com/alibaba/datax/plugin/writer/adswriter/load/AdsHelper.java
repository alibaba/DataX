/**
 * 
 */
package com.alibaba.datax.plugin.writer.adswriter.load;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.writer.adswriter.AdsException;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnDataType;
import com.alibaba.datax.plugin.writer.adswriter.ads.ColumnInfo;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

public class AdsHelper {
    private static final Logger LOG = LoggerFactory
            .getLogger(AdsHelper.class);

    private String adsURL;
    private String userName;
    private String password;
    private String schema;
    private Long socketTimeout;
    private String suffix;

    public AdsHelper(String adsUrl, String userName, String password, String schema, Long socketTimeout, String suffix) {
        this.adsURL = adsUrl;
        this.userName = userName;
        this.password = password;
        this.schema = schema;
        this.socketTimeout = socketTimeout;
        this.suffix = suffix;
    }

    public String getAdsURL() {
        return adsURL;
    }

    public void setAdsURL(String adsURL) {
        this.adsURL = adsURL;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Obtain the table meta information.
     * 
     * @param table The table
     * @return The table meta information
     * @throws com.alibaba.datax.plugin.writer.adswriter.AdsException
     */
    public TableInfo getTableInfo(String table) throws AdsException {

        if (table == null) {
            throw new AdsException(AdsException.ADS_TABLEMETA_TABLE_NULL, "Table is null.", null);
        }

        if (adsURL == null) {
            throw new AdsException(AdsException.ADS_CONN_URL_NOT_SET, "ADS JDBC connection URL was not set.", null);
        }

        if (userName == null) {
            throw new AdsException(AdsException.ADS_CONN_USERNAME_NOT_SET,
                    "ADS JDBC connection user name was not set.", null);
        }

        if (password == null) {
            throw new AdsException(AdsException.ADS_CONN_PASSWORD_NOT_SET, "ADS JDBC connection password was not set.",
                    null);
        }

        if (schema == null) {
            throw new AdsException(AdsException.ADS_CONN_SCHEMA_NOT_SET, "ADS JDBC connection schema was not set.",
                    null);
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = AdsUtil.prepareJdbcUrl(this.adsURL, this.schema, this.socketTimeout, this.suffix);

            Properties connectionProps = new Properties();
            connectionProps.put("user", userName);
            connectionProps.put("password", password);
            connection = DriverManager.getConnection(url, connectionProps);
            statement = connection.createStatement();
            // ads 表名、schema名不区分大小写, 提高用户易用性, 注意列顺序性
            String columnMetaSql = String.format("select ordinal_position,column_name,data_type,type_name,column_comment from information_schema.columns where table_schema = `'%s'` and table_name = `'%s'` order by ordinal_position", schema.toLowerCase(), table.toLowerCase());
            LOG.info(String.format("检查列信息sql语句:%s", columnMetaSql));
            rs = statement.executeQuery(columnMetaSql);

            TableInfo tableInfo = new TableInfo();
            List<ColumnInfo> columnInfoList = new ArrayList<ColumnInfo>();
            while (DBUtil.asyncResultSetNext(rs)) {
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setOrdinal(rs.getInt(1));
                columnInfo.setName(rs.getString(2));
                //columnInfo.setDataType(ColumnDataType.getDataType(rs.getInt(3))); //for ads version < 0.7
                //columnInfo.setDataType(ColumnDataType.getTypeByName(rs.getString(3).toUpperCase())); //for ads version 0.8
                columnInfo.setDataType(ColumnDataType.getTypeByName(rs.getString(4).toUpperCase())); //for ads version 0.8 & 0.7
                columnInfo.setComment(rs.getString(5));
                columnInfoList.add(columnInfo);
            }
            if (columnInfoList.isEmpty()) {
                throw DataXException.asDataXException(AdsWriterErrorCode.NO_ADS_TABLE, table + "不存在或者查询不到列信息. ");
            }
            tableInfo.setColumns(columnInfoList);
            tableInfo.setTableSchema(schema);
            tableInfo.setTableName(table);
            DBUtil.closeDBResources(rs, statement, null);
            
            String tableMetaSql = String.format("select update_type, partition_type, partition_column, partition_count, primary_key_columns from information_schema.tables where table_schema = `'%s'` and table_name = `'%s'`", schema.toLowerCase(), table.toLowerCase());
            LOG.info(String.format("检查表信息sql语句:%s", tableMetaSql));
            statement = connection.createStatement();
            rs = statement.executeQuery(tableMetaSql);
            while (DBUtil.asyncResultSetNext(rs)) {
                tableInfo.setUpdateType(rs.getString(1));
                tableInfo.setPartitionType(rs.getString(2));
                tableInfo.setPartitionColumn(rs.getString(3));
                tableInfo.setPartitionCount(rs.getInt(4));
                //primary_key_columns  ads主键是逗号分隔的，可以有多个
                String primaryKeyColumns = rs.getString(5);
                if (StringUtils.isNotBlank(primaryKeyColumns)) {
                    tableInfo.setPrimaryKeyColumns(Arrays.asList(StringUtils.split(primaryKeyColumns, ",")));
                } else {
                    tableInfo.setPrimaryKeyColumns(null);
                }
                break;
            }
            DBUtil.closeDBResources(rs, statement, null);
            return tableInfo;

        } catch (ClassNotFoundException e) {
            throw new AdsException(AdsException.OTHER, e.getMessage(), e);
        } catch (SQLException e) {
            throw new AdsException(AdsException.OTHER, e.getMessage(), e);
        } catch ( DataXException e) {
            throw e;
        } catch (Exception e) {
            throw new AdsException(AdsException.OTHER, e.getMessage(), e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // Ignore exception
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // Ignore exception
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore exception
                }
            }
        }

    }

    /**
     * Submit LOAD DATA command.
     * 
     * @param table The target ADS table
     * @param partition The partition option in the form of "(partition_name,...)"
     * @param sourcePath The source path
     * @param overwrite
     * @return
     * @throws AdsException
     */
    public String loadData(String table, String partition, String sourcePath, boolean overwrite) throws AdsException {

        if (table == null) {
            throw new AdsException(AdsException.ADS_LOADDATA_TABLE_NULL, "ADS LOAD DATA table is null.", null);
        }

        if (sourcePath == null) {
            throw new AdsException(AdsException.ADS_LOADDATA_SOURCEPATH_NULL, "ADS LOAD DATA source path is null.",
                    null);
        }

        if (adsURL == null) {
            throw new AdsException(AdsException.ADS_CONN_URL_NOT_SET, "ADS JDBC connection URL was not set.", null);
        }

        if (userName == null) {
            throw new AdsException(AdsException.ADS_CONN_USERNAME_NOT_SET,
                    "ADS JDBC connection user name was not set.", null);
        }

        if (password == null) {
            throw new AdsException(AdsException.ADS_CONN_PASSWORD_NOT_SET, "ADS JDBC connection password was not set.",
                    null);
        }

        if (schema == null) {
            throw new AdsException(AdsException.ADS_CONN_SCHEMA_NOT_SET, "ADS JDBC connection schema was not set.",
                    null);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA FROM ");
        if (sourcePath.startsWith("'") && sourcePath.endsWith("'")) {
            sb.append(sourcePath);
        } else {
            sb.append("'" + sourcePath + "'");
        }
        if (overwrite) {
            sb.append(" OVERWRITE");
        }
        sb.append(" INTO TABLE ");
        sb.append(schema + "." + table);
        if (partition != null && !partition.trim().equals("")) {
            String partitionTrim = partition.trim();
            if(partitionTrim.startsWith("(") && partitionTrim.endsWith(")")) {
                sb.append(" PARTITION " + partition);
            } else {
                sb.append(" PARTITION " + "(" + partition + ")");
            }
        }

        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = AdsUtil.prepareJdbcUrl(this.adsURL, this.schema, this.socketTimeout, this.suffix);
            Properties connectionProps = new Properties();
            connectionProps.put("user", userName);
            connectionProps.put("password", password);
            connection = DriverManager.getConnection(url, connectionProps);
            statement = connection.createStatement();
            LOG.info("正在从ODPS数据库导数据到ADS中: "+sb.toString());
            LOG.info("由于ADS的限制，ADS导数据最少需要20分钟，请耐心等待");
            rs = statement.executeQuery(sb.toString());

            String jobId = null;
            while (DBUtil.asyncResultSetNext(rs)) {
                jobId = rs.getString(1);
            }

            if (jobId == null) {
                throw new AdsException(AdsException.ADS_LOADDATA_JOBID_NOT_AVAIL,
                        "Job id is not available for the submitted LOAD DATA." + jobId, null);
            }

            return jobId;

        } catch (ClassNotFoundException e) {
            throw new AdsException(AdsException.ADS_LOADDATA_FAILED, e.getMessage(), e);
        } catch (SQLException e) {
            throw new AdsException(AdsException.ADS_LOADDATA_FAILED, e.getMessage(), e);
        } catch (Exception e) {
            throw new AdsException(AdsException.ADS_LOADDATA_FAILED, e.getMessage(), e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    // Ignore exception
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    // Ignore exception
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Ignore exception
                }
            }
        }

    }

    /**
     * Check the load data job status.
     * 
     * @param jobId The job id to
     * @return true if load data job succeeded, false if load data job failed.
     * @throws AdsException
     */
    public boolean checkLoadDataJobStatus(String jobId) throws AdsException {

        if (adsURL == null) {
            throw new AdsException(AdsException.ADS_CONN_URL_NOT_SET, "ADS JDBC connection URL was not set.", null);
        }

        if (userName == null) {
            throw new AdsException(AdsException.ADS_CONN_USERNAME_NOT_SET,
                    "ADS JDBC connection user name was not set.", null);
        }

        if (password == null) {
            throw new AdsException(AdsException.ADS_CONN_PASSWORD_NOT_SET, "ADS JDBC connection password was not set.",
                    null);
        }

        if (schema == null) {
            throw new AdsException(AdsException.ADS_CONN_SCHEMA_NOT_SET, "ADS JDBC connection schema was not set.",
                    null);
        }

        try {
            String state = this.checkLoadDataJobStatusWithRetry(jobId);
            if (state == null) {
                throw new AdsException(AdsException.JOB_NOT_EXIST, "Target job does not exist for id: " + jobId, null);
            }
            if (state.equals("SUCCEEDED")) {
                return true;
            } else if (state.equals("FAILED")) {
                throw new AdsException(AdsException.JOB_FAILED, "Target job failed for id: " + jobId, null);
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new AdsException(AdsException.OTHER, e.getMessage(), e);
        } 
    }
    
    private String checkLoadDataJobStatusWithRetry(final String jobId)
            throws AdsException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            final String finalAdsUrl = this.adsURL;
            final String finalSchema = this.schema;
            final Long finalSocketTimeout = this.socketTimeout;
            final String suffix = this.suffix;
            return RetryUtil.executeWithRetry(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Connection connection = null;
                    Statement statement = null;
                    ResultSet rs = null;
                    try {
                        
                        String url = AdsUtil.prepareJdbcUrl(finalAdsUrl, finalSchema, finalSocketTimeout, suffix);
                        Properties connectionProps = new Properties();
                        connectionProps.put("user", userName);
                        connectionProps.put("password", password);
                        connection = DriverManager.getConnection(url,
                                connectionProps);
                        statement = connection.createStatement();

                        String sql = "select state from information_schema.job_instances where job_id like '"
                                + jobId + "'";
                        rs = statement.executeQuery(sql);
                        String state = null;
                        while (DBUtil.asyncResultSetNext(rs)) {
                            state = rs.getString(1);
                        }
                        return state;
                    } finally {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (SQLException e) {
                                // Ignore exception
                            }
                        }
                        if (statement != null) {
                            try {
                                statement.close();
                            } catch (SQLException e) {
                                // Ignore exception
                            }
                        }
                        if (connection != null) {
                            try {
                                connection.close();
                            } catch (SQLException e) {
                                // Ignore exception
                            }
                        }
                    }
                }
            }, 3, 1000L, true);
        } catch (Exception e) {
            throw new AdsException(AdsException.OTHER, e.getMessage(), e);
        }
    }
}
