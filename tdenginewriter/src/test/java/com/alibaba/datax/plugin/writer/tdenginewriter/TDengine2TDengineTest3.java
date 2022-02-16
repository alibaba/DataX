package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;

public class TDengine2TDengineTest3 {

    private static final String host1 = "192.168.56.105";
    private static final String host2 = "192.168.1.93";
    private static Random random = new Random(System.currentTimeMillis());

    @Test
    public void case_03() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/t2t-3.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Before
    public void before() throws SQLException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String ts = sdf.format(new Date(System.currentTimeMillis()));

        final String url = "jdbc:TAOS-RS://" + host1 + ":6041";
        try (Connection conn = DriverManager.getConnection(url, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db1");
            stmt.execute("create database if not exists db1");
            stmt.execute("create table db1.stb1 (ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint, " +
                    "f5 float, f6 double, f7 bool, f8 binary(100), f9 nchar(100)) tags(t1 timestamp, t2 tinyint, " +
                    "t3 smallint, t4 int, t5 bigint, t6 float, t7 double, t8 bool, t9 binary(100), t10 nchar(1000))");
            for (int i = 1; i <= 10; i++) {
                String sql = "insert into db1.t" + i + " using db1.stb1 tags('" + ts + "'," + i + ",2,3,4,5.0,6.0,true,'abc123ABC','北京朝阳望京') " +
                        "values(now+" + i + "s, " + random.nextInt(100) + "," + random.nextInt(100) + "," +
                        random.nextInt(100) + "," + random.nextInt(100) + "," + random.nextFloat() + "," +
                        random.nextDouble() + "," + random.nextBoolean() + ",'abc123ABC','北京朝阳望京')";
                stmt.execute(sql);
            }
        }

        final String url2 = "jdbc:TAOS-RS://" + host2 + ":6041";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists db2");
            stmt.execute("create table db2.stb2 (ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint," +
                    " f5 float, f6 double, f7 bool, f8 binary(100), f9 nchar(100)) tags(t1 timestamp, t2 tinyint, " +
                    "t3 smallint, t4 int, t5 bigint, t6 float, t7 double, t8 bool, t9 binary(100), t10 nchar(1000))");

            stmt.execute("create table db2.t1 using db2.stb2 tags('" + ts + "',1,2,3,4,5.0,6.0,true,'abc123ABC','北京朝阳望京')");
            stmt.close();
        }

    }
}
