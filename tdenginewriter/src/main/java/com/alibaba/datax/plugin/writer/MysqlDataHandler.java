package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlDataHandler implements DataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlDataHandler.class);
    Connection conn;

    @Override
    public long handle(RecordReceiver lineReceiver, Configuration configuration) {
        Properties properties = CommonUtil.toProperties(configuration);

        long count = 0;
        try {
            conn = getConnection(properties);

            Record record;
            while ((record = lineReceiver.getFromReader()) != null) {

                int recordLength = record.getColumnNumber();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < recordLength; i++) {
                    Column column = record.getColumn(i);
                    sb.append(column.asString()).append("\t");
                }
                sb.setLength(sb.length() - 1);
                LOG.debug(sb.toString());

                count++;
            }


        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return count;
    }

    private Connection getConnection(Properties properties) {
        String host = properties.getProperty(Key.HOST);
        int port = Integer.parseInt(properties.getProperty(Key.PORT));
        String dbname = properties.getProperty(Key.DBNAME);
        String user = properties.getProperty(Key.USER);
        String password = properties.getProperty(Key.PASSWORD);
        String url = "jdbc:TAOS://" + host + ":" + port + "/" + dbname + "?user=" + user + "&password=" + password;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, properties);
            LOG.info("TDengine connection established, host: " + host + ", port: " + port + ", dbname: " + dbname + ", user: " + user);
        } catch (SQLException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
        return connection;
    }
}
