package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

public class EngineTest {

    @Test
    public void opentsdb2tdengine() throws SQLException {
        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/opentsdb2tdengine.json"};
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

    @Test
    public void mysql2tdengine() {
        // given
        // MYSQL SQL:
        // create table t(id int primary key AUTO_INCREMENT, f1 tinyint, f2 smallint, f3 int, f4 bigint, f5 float, f6 double,ts timestamp, dt datetime,f7 nchar(64))
        // insert into t(f1,f2,f3,f4,f5,f6,ts,dt,f7) values(1,2,3,4,5,6,'2022-01-28 12:00:00','2022-01-28 12:00:00', 'beijing');
        // TDengine SQL:
        // create table t(ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint, f5 float, f6 double, dt timestamp,f7 nchar(64));

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/mysql2tdengine.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        try {
            Engine.entry(params);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tdengine2tdengine() {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/tdengine2tdengine.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        try {
            Engine.entry(params);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
