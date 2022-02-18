package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

public class Opentsdb2TDengineTest {

    @Test
    public void opentsdb2tdengine() throws SQLException {
        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/o2t-1.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        try {
            Engine.entry(params);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        // assert
        String jdbcUrl = "jdbc:TAOS://192.168.56.105:6030/test?timestampFormat=TIMESTAMP";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from weather_temperature");
            int rows = 0;
            while (rs.next()) {
                rows = rs.getInt("count(*)");
            }
            Assert.assertEquals(5, rows);
            stmt.close();
        }
    }

}
