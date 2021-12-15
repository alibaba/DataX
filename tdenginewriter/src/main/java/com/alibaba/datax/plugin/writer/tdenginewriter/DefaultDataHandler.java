package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBPreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 默认DataHandler
 */
public class DefaultDataHandler implements DataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataHandler.class);

    static {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long handle(RecordReceiver lineReceiver, Properties properties, TaskPluginCollector collector) {
        SchemaManager schemaManager = new SchemaManager(properties);
        if (!schemaManager.configValid()) {
            return 0;
        }

        try {
            Connection conn = getTaosConnection(properties);
            if (conn == null) {
                return 0;
            }
            if (schemaManager.shouldGuessSchema()) {
                // 无法从配置文件获取表结构信息，尝试从数据库获取
                LOG.info(Msg.get("try_get_schema_from_db"));
                boolean success = schemaManager.getFromDB(conn);
                if (!success) {
                    return 0;
                }
            } else {

            }
            int batchSize = Integer.parseInt(properties.getProperty(Key.BATCH_SIZE, "1000"));
            if (batchSize < 5) {
                // batchSize太小，会增加自动类型推断错误的概率，建议改大后重试
                LOG.error(Msg.get("batch_size_too_small"));
                return 0;
            }
            return write(lineReceiver, conn, batchSize, schemaManager, collector);
        } catch (Exception e) {
            LOG.error("write failed " + e.getMessage());
            e.printStackTrace();
        }
        return 0;
    }


    private Connection getTaosConnection(Properties properties) throws SQLException {
        // 检查必要参数
        String host = properties.getProperty(Key.HOST);
        String port = properties.getProperty(Key.PORT);
        String dbname = properties.getProperty(Key.DBNAME);
        String user = properties.getProperty(Key.USER);
        String password = properties.getProperty(Key.PASSWORD);
        if (host == null || port == null || dbname == null || user == null || password == null) {
            String keys = String.join(" ", Key.HOST, Key.PORT, Key.DBNAME, Key.USER, Key.PASSWORD);
            LOG.error("Required options missing, please check: " + keys);
            return null;
        }
        String jdbcUrl = String.format("jdbc:TAOS://%s:%s/%s?user=%s&password=%s", host, port, dbname, user, password);
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        LOG.info("TDengine connection established, host:{} port:{} dbname:{} user:{}", host, port, dbname, user);
        return DriverManager.getConnection(jdbcUrl, connProps);
    }

    /**
     * 使用SQL批量写入<br/>
     *
     * @return 成功写入记录数
     * @throws SQLException
     */
    private long write(RecordReceiver lineReceiver, Connection conn, int batchSize, SchemaManager scm, TaskPluginCollector collector) throws SQLException {
        Record record = lineReceiver.getFromReader();
        if (record == null) {
            return 0;
        }
        String pq = String.format("INSERT INTO ? USING %s TAGS(%s) (%s) values (%s)", scm.getStable(), scm.getTagValuesPlaceHolder(), scm.getJoinedFieldNames(), scm.getFieldValuesPlaceHolder());
        LOG.info("Prepared SQL: {}", pq);
        try (TSDBPreparedStatement stmt = (TSDBPreparedStatement) conn.prepareStatement(pq)) {
            JDBCBatchWriter batchWriter = new JDBCBatchWriter(conn, stmt, scm, batchSize, collector);
            do {
                batchWriter.append(record);
            } while ((record = lineReceiver.getFromReader()) != null);
            batchWriter.flush();
            return batchWriter.getCount();
        }
    }
}