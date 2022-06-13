package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.core.Engine;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

@Ignore
public class TDengine2DMTest {
    private static final String host1 = "192.168.56.105";
    private static final String host2 = "192.168.0.72";

    private final Random random = new Random(System.currentTimeMillis());

    @Test
    public void t2dm_case01() throws Throwable {
        // given
        createSupTable("ms");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/t2dm.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void t2dm_case02() throws Throwable {
        // given
        createSupTable("us");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/t2dm.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void t2dm_case03() throws Throwable {
        // given
        createSupTable("ns");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/t2dm.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    private void createSupTable(String precision) throws SQLException {
        final String url = "jdbc:TAOS-RS://" + host1 + ":6041/";
        try (Connection conn = DriverManager.getConnection(url, "root", "taosdata")) {
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists db1");
            stmt.execute("create database if not exists db1 precision '" + precision + "'");
            stmt.execute("create table db1.stb1(ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint, f5 float, " +
                    "f6 double, f7 bool, f8 binary(100), f9 nchar(100)) tags(t1 timestamp, t2 tinyint, t3 smallint, " +
                    "t4 int, t5 bigint, t6 float, t7 double, t8 bool, t9 binary(100), t10 nchar(100))");

            for (int i = 1; i <= 10; i++) {
                stmt.execute("insert into db1.tb" + i + " using db1.stb1 tags(now, " + random.nextInt(10) + "," +
                        random.nextInt(10) + "," + random.nextInt(10) + "," + random.nextInt(10) + "," +
                        random.nextFloat() + "," + random.nextDouble() + "," + random.nextBoolean() + ",'abcABC123'," +
                        "'北京朝阳望京') values(now+" + i + "s, " + random.nextInt(10) + "," + random.nextInt(10) + "," +
                        +random.nextInt(10) + "," + random.nextInt(10) + "," + random.nextFloat() + "," +
                        random.nextDouble() + "," + random.nextBoolean() + ",'abcABC123','北京朝阳望京')");
            }
            stmt.close();
        }

        final String url2 = "jdbc:dm://" + host2 + ":5236";
        try (Connection conn = DriverManager.getConnection(url2, "TESTUSER", "test123456")) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists stb2");
            stmt.execute("create table stb2(ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint, f5 float, " +
                    "f6 double, f7 BIT, f8 VARCHAR(100), f9 VARCHAR2(200), t1 timestamp, t2 tinyint, t3 smallint, " +
                    "t4 int, t5 bigint, t6 float, t7 double, t8 BIT, t9 VARCHAR(100), t10 VARCHAR2(200))");
        }
    }

}
