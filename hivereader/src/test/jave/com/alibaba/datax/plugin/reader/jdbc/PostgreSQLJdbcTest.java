package com.alibaba.datax.plugin.reader.jdbc;

import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class PostgreSQLJdbcTest
{
    private static String driverName = Driver.class.getName();

    public static void main(String[] args)
    {
        try {
            Class.forName(driverName);
            Connection con = null;
            con = DriverManager.getConnection("jdbc:postgresql://192.168.118.128:5432/db_martin", "postgres", "123456");
            Statement stmt = con.createStatement();
            ResultSet rs = null;
            String sql = "select * from scm_test.tbl_user;";
            System.out.println("Running: " + sql);
            rs = stmt.executeQuery(sql);
            final ResultSetMetaData metaData = rs.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                System.out.println("---line---");
                for (int i = 1; i <= columnCount; i++) {
                    final String string = rs.getString(i);
                    System.out.print("columnIndex: " + i + ", data: " + string + ", columnTypeName: " + metaData.getColumnTypeName(i) + ", columnType: " + metaData.getColumnType(i) + "\n");
                }
//                  System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" +  rs.getString(3) + "\t" +  rs.getString(4));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
        }
    }
}