package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DM2TDengineTest {

    private String host1 = "192.168.0.72";
    private String host2 = "192.168.1.93";
    private final Random random = new Random(System.currentTimeMillis());

    @Test
    public void dm2t_case01() throws Throwable {
        // given
        createSupTable();

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/dm2t-1.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void dm2t_case02() throws Throwable {
        // given
        createSupAndSubTable();

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/dm2t-2.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void dm2t_case03() throws Throwable {
        // given
        createTable();

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/dm2t-3.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void dm2t_case04() throws Throwable {
        // given
        createSupTable();

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/dm2t-4.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    private void createSupTable() throws SQLException {
        final String url2 = "jdbc:TAOS-RS://" + host2 + ":6041";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists  db2");
            stmt.execute("create table db2.stb2(ts timestamp, f2 smallint, f4 bigint,f5 float, " +
                    "f6 double, f7 double, f8 bool, f9 nchar(100), f10 nchar(200)) tags(f1 tinyint,f3 int)");
            stmt.close();
        }
    }

    private void createSupAndSubTable() throws SQLException {
        final String url2 = "jdbc:TAOS-RS://" + host2 + ":6041";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists  db2");
            stmt.execute("create table db2.stb2(ts timestamp, f2 smallint, f4 bigint,f5 float, " +
                    "f6 double, f7 double, f8 bool, f9 nchar(100), f10 nchar(200)) tags(f1 tinyint,f3 int)");
            for (int i = 0; i < 10; i++) {
                stmt.execute("create table db2.t" + (i + 1) + "_" + i + " using db2.stb2 tags(" + (i + 1) + "," + i + ")");
            }
            stmt.close();
        }
    }

    private void createTable() throws SQLException {
        final String url2 = "jdbc:TAOS-RS://" + host2 + ":6041";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists  db2");
            stmt.execute("create table db2.stb2(ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint,f5 float, " +
                    "f6 double, f7 double, f8 bool, f9 nchar(100), f10 nchar(200))");
            stmt.close();
        }
    }

    @Before
    public void before() throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long ts = System.currentTimeMillis();

        final String url = "jdbc:dm://" + host1 + ":5236";
        try (Connection conn = DriverManager.getConnection(url, "TESTUSER", "test123456")) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists stb1");
            stmt.execute("create table stb1(ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint, f5 float, " +
                    "f6 double, f7 NUMERIC(10,2), f8 BIT, f9 VARCHAR(100), f10 VARCHAR2(200))");
            for (int i = 0; i < 10; i++) {
                String sql = "insert into stb1 values('" + sdf.format(new Date(ts + i * 1000)) + "'," + (i + 1) + "," +
                        random.nextInt(100) + "," + i + ",4,5.55,6.666,7.77," + (random.nextBoolean() ? 1 : 0) +
                        ",'abcABC123','北京朝阳望京DM')";
                stmt.execute(sql);
            }
        }
    }

}
