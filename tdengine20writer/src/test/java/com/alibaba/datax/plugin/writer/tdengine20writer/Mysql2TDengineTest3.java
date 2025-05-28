package com.alibaba.datax.plugin.writer.tdengine20writer;

import com.alibaba.datax.core.Engine;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Mysql2TDengineTest3 {

    private static final String host1 = "192.168.56.105";
    private static final String host2 = "192.168.56.105";
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void test2() throws Throwable {
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/m2t-3.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Before
    public void before() throws SQLException {
        // given
        long ts_start = new Date(System.currentTimeMillis()).getTime();
        final int columnSize = 10;
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        final String url = "jdbc:mysql://" + host1 + ":3306/?useSSL=false&useUnicode=true&charset=UTF-8&generateSimpleParameterMetadata=true";
        try (Connection conn = DriverManager.getConnection(url, "root", "123456")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db1");
            stmt.execute("create database if not exists db1");
            stmt.execute("use db1");
            stmt.execute("create table stb1(id int primary key AUTO_INCREMENT, "
                    + IntStream.range(1, columnSize).mapToObj(i -> "f" + i + " int").collect(Collectors.joining(",")) + ", "
                    + IntStream.range(1, columnSize).mapToObj(i -> "t" + i + " varchar(20)").collect(Collectors.joining(",")) + ", ts timestamp)");
            for (int i = 1; i <= 10; i++) {
                String sql = "insert into stb1("
                        + IntStream.range(1, columnSize).mapToObj(index -> "f" + index).collect(Collectors.joining(",")) + ", "
                        + IntStream.range(1, columnSize).mapToObj(index -> "t" + index).collect(Collectors.joining(","))
                        + ", ts) values("
                        + IntStream.range(1, columnSize).mapToObj(index -> random.nextInt(10) + "").collect(Collectors.joining(","))
                        + ","
                        + IntStream.range(1, columnSize).mapToObj(index -> "'" + RandomStringUtils.randomAlphanumeric(15) + "'").collect(Collectors.joining(","))
                        + ", '" + sdf.format(new Date(ts_start + i * 1000)) + "')";
                stmt.execute(sql);
            }

            stmt.close();
        }

        final String url2 = "jdbc:TAOS://" + host2 + ":6030/";
        try (Connection conn = DriverManager.getConnection(url2, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db2");
            stmt.execute("create database if not exists db2");
            stmt.execute("create table db2.stb2(ts timestamp, "
                    + IntStream.range(1, 101).mapToObj(i -> "f" + i + " int").collect(Collectors.joining(",")) + ") tags("
                    + IntStream.range(1, 101).mapToObj(i -> "t" + i + " nchar(20)").collect(Collectors.joining(","))
                    + ")"
            );

            stmt.close();
        }
    }


}
