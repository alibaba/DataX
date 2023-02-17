package com.alibaba.datax.plugin.writer.tdengine30writer;

import com.alibaba.datax.core.Engine;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;

public class Mysql2TDengineTest2 {

    private static final String host1 = "192.168.56.105";
    private static final String host2 = "192.168.56.105";
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void test2() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/m2t-2.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Before
    public void before() throws SQLException {
        final String[] tagList = {"北京", "海淀", "上海", "河北", "天津"};

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String ts = sdf.format(new Date(System.currentTimeMillis()));

        final String url = "jdbc:mysql://" + host1 + ":3306/?useSSL=false&useUnicode=true&charset=UTF-8&generateSimpleParameterMetadata=true";
        try (Connection conn = DriverManager.getConnection(url, "root", "123456")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db1");
            stmt.execute("create database if not exists db1");
            stmt.execute("use db1");
            stmt.execute("create table stb1(id int primary key AUTO_INCREMENT, " +
                    "f1 int, f2 float, f3 double, f4 varchar(100), t1 varchar(100), ts timestamp)");
            for (int i = 1; i <= 10; i++) {
                String sql = "insert into stb1(f1, f2, f3, f4, t1, ts) values("
                        + random.nextInt(100) + "," + random.nextFloat() * 100 + "," + random.nextDouble() * 100
                        + ",'" + RandomStringUtils.randomAlphanumeric(10)
                        + "', '" + tagList[random.nextInt(tagList.length)]
                        + "', '" + (ts + i * 1000) + "')";
                stmt.execute(sql);
            }

            stmt.close();
        }

        final String url2 = "jdbc:TAOS://" + host2 + ":6030/";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists db2");
            stmt.execute("create table db2.stb2(ts timestamp, f1 int, f2 float, f3 double, f4 nchar(100)) tags(t1 nchar(100))");

            stmt.close();
        }

    }

}
