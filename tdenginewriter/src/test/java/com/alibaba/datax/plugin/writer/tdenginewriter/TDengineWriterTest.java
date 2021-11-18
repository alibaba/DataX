package com.alibaba.datax.plugin.writer.tdenginewriter;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TDengineWriterTest {


    @Test
    public void testGetSchema() throws ClassNotFoundException, SQLException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        String jdbcUrl = String.format("jdbc:TAOS://%s:%s/%s?user=%s&password=%s", "wozai.fun", "6030", "test", "root", "taosdata");
        Connection conn = DriverManager.getConnection(jdbcUrl);
        SchemaManager schemaManager = new SchemaManager();
        schemaManager.setStable("test1");
        schemaManager.getFromDB(conn);
    }
}
