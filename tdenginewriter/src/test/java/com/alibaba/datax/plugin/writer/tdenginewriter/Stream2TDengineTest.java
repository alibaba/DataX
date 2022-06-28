package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Stream2TDengineTest {

    private String host2 = "192.168.56.105";

    @Test
    public void s2t_case1() throws Throwable {
        // given
        createSupTable("ms");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/defaultJob.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void s2t_case2() throws Throwable {
        // given
        createSupTable("us");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/defaultJob.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void s2t_case3() throws Throwable {
        // given
        createSupTable("ns");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/defaultJob.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    void createSupTable(String precision) throws SQLException {

        final String url = "jdbc:TAOS-RS://" + host2 + ":6041/";
        try (Connection conn = DriverManager.getConnection(url, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists db2 precision '" + precision + "'");
            stmt.execute("create table db2.stb2(ts1 timestamp, ts2 timestamp,ts3 timestamp,ts4 timestamp,ts5 timestamp," +
                    "ts6 timestamp,ts7 timestamp, ts8 timestamp, ts9 timestamp, ts10 timestamp, f1 tinyint, f2 smallint," +
                    "f3 int, f4 bigint, f5 float, f6 double," +
                    "f7 bool, f8 binary(100), f9 nchar(100)) tags(t1 timestamp,t2 timestamp,t3 timestamp,t4 timestamp," +
                    "t5 timestamp,t6 timestamp,t7 timestamp, t8 tinyint, t9 smallint, t10 int, t11 bigint, t12 float," +
                    "t13 double, t14 bool, t15 binary(100), t16 nchar(100))");

            stmt.close();
        }

    }

}
