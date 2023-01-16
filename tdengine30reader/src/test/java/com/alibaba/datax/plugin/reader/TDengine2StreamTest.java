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
public class TDengine2StreamTest {

    private static final String host = "192.168.56.105";
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void case01() throws Throwable {
        // given
        prepare("ms");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/t2stream-1.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }

    @Test
    public void case02() throws Throwable {
        // given
        prepare("ms");

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/t2stream-2.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }


    private void prepare(String precision) throws SQLException {
        final String url = "jdbc:TAOS-RS://" + host + ":6041/";
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
    }


}
