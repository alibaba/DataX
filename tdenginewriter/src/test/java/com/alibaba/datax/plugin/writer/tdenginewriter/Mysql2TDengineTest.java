package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.core.Engine;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;

public class Mysql2TDengineTest {

    private static final String host1 = "192.168.56.105";
    private static final String host2 = "192.168.1.93";
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void mysql2tdengine() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/m2t-1.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Before
    public void before() throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String ts = sdf.format(new Date(System.currentTimeMillis()));

        final String url = "jdbc:mysql://" + host1 + ":3306/?useSSL=false&useUnicode=true&charset=UTF-8&generateSimpleParameterMetadata=true";
        try (Connection conn = DriverManager.getConnection(url, "root", "123456")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db1");
            stmt.execute("create database if not exists db1");
            stmt.execute("use db1");
            stmt.execute("create table stb1(id int primary key AUTO_INCREMENT, " +
                    "f1 tinyint, f2 smallint, f3 int, f4 bigint, " +
                    "f5 float, f6 double, " +
                    "ts timestamp, dt datetime," +
                    "f7 nchar(100), f8 varchar(100))");
            for (int i = 1; i <= 10; i++) {
                String sql = "insert into stb1(f1, f2, f3, f4, f5, f6, ts, dt, f7, f8) values(" +
                        i + "," + random.nextInt(100) + "," + random.nextInt(100) + "," + random.nextInt(100) + "," +
                        random.nextFloat() + "," + random.nextDouble() + ", " +
                        "'" + ts + "', '" + ts + "', " +
                        "'中国北京朝阳望京abc', '中国北京朝阳望京adc')";
                stmt.execute(sql);
            }

            stmt.close();
        }

        final String url2 = "jdbc:TAOS-RS://" + host2 + ":6041/";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists db2");
            stmt.execute("create table db2.stb2(" +
                    "ts timestamp, dt timestamp, " +
                    "f1 tinyint, f2 smallint, f3 int, f4 bigint, " +
                    "f5 float, f6 double, " +
                    "f7 nchar(100), f8 nchar(100))");

            stmt.close();
        }

    }

}
