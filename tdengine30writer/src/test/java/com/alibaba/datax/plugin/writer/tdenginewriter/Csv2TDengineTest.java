package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Ignore
public class Csv2TDengineTest {

    private static final String host = "192.168.56.105";

    @Test
    public void case01() throws Throwable {
        // given
        prepareTable();

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/csv2t.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    public void prepareTable() throws SQLException {
        final String url = "jdbc:TAOS-RS://" + host + ":6041";
        try (Connection conn = DriverManager.getConnection(url, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test");
            stmt.execute("create table test.weather (ts timestamp, temperature bigint, humidity double, is_normal bool) " +
                    "tags(device_id binary(10),address nchar(10))");
        }
    }


}
