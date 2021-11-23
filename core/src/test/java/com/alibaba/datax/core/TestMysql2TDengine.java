package com.alibaba.datax.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * 测试从mysql到TD
 */
public class TestMysql2TDengine {

    @Test
    public void genTestData() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost/mysql?" +
                    "user=root&password=passw0rd");
            stmt = conn.createStatement();
            stmt.execute("create database if not exists test");
            stmt.execute("use test");
            stmt.execute("drop table weather");
            stmt.execute("CREATE TABLE IF NOT EXISTS weather(station varchar(100), latitude DOUBLE, longtitude DOUBLE, `date` DATETIME, tmax INT, tmin INT)");
            pstmt = conn.prepareStatement("insert into weather(station, latitude, longtitude, `date`, tmax, tmin) values (?, ?, ?, ?, ?, ?)");
            genRandomData(pstmt);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore

                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore

                stmt = null;
            }

            if (pstmt != null) {
                pstmt.close();
            }
        }

    }

    private void genRandomData(PreparedStatement psmt) throws SQLException {
        Random random = new Random();
        Calendar calendar = Calendar.getInstance();
        calendar.set(1990, 0, 1, 1, 0, 0);
        List<String> stations = Arrays.asList("STA", "STB", "STC");
        for (int i = 0; i < (10 * 100 * 24); i++) {
            for (int j = 0; j < 3; j++) {
                psmt.setString(1, stations.get(j));
                psmt.setDouble(2, random.nextDouble() * 1000);
                psmt.setDouble(3, random.nextDouble() * 1000);
                psmt.setTimestamp(4, new java.sql.Timestamp(calendar.getTime().getTime()));
                psmt.setInt(5, random.nextInt(100));
                psmt.setInt(6, random.nextInt(100));
                psmt.addBatch();
            }
            calendar.add(Calendar.MINUTE, 60);
            if (i % 1000 == 0) {
                psmt.executeBatch();
            }
        }
        psmt.executeBatch();
    }

    @Test
    public void prepareTDengine() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = DriverManager.getConnection("jdbc:TAOS://127.0.0.1:6030/log?user=root&password=taosdata");
            stmt = conn.createStatement();
            stmt.execute("create database if not exists test");
            stmt.execute("drop stable if exists test.weather");
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    @Test
    public void test() {
        System.out.println(System.getProperty("java.library.path"));
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/main/job/mysql2tdengine.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        try {
            Engine.entry(params);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }



}