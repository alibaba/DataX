package com.alibaba.datax.plugin.reader.sybasereader;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class SybaseDatabaseUnitTest {
    private Connection connection;

    @Before
    public void setUp() {
        // 连接到 Sybase 数据库
        String jdbcUrl = "jdbc:sybase:Tds:192.172.172.80:1680/database";
        String username = "admin";
        String password = "admin123";

        try {
            connection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testDatabaseQuery() throws SQLException {
        String query = "SELECT COUNT(*) FROM your_table";
        int expectedRowCount = 10; // 假设期望返回的行数是 10

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
        int rowCount = resultSet.getInt(1);

        assertEquals(expectedRowCount, rowCount);
    }
}
