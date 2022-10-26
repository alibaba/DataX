package com.alibaba.datax.plugin.writer.tdenginewriter;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SchemaManagerTest {

    private static Connection conn;

    @Test
    public void loadTableMeta() throws SQLException {
        // given
        SchemaManager schemaManager = new SchemaManager(conn);
        List<String> tables = Arrays.asList("stb1", "stb2", "tb1", "tb3", "weather");

        // when
        Map<String, TableMeta> tableMetaMap = schemaManager.loadTableMeta(tables);

        // then
        TableMeta stb1 = tableMetaMap.get("stb1");
        Assert.assertEquals(TableType.SUP_TABLE, stb1.tableType);
        Assert.assertEquals("stb1", stb1.tbname);
        Assert.assertEquals(3, stb1.columns);
        Assert.assertEquals(1, stb1.tags);
        Assert.assertEquals(2, stb1.tables);

        TableMeta tb3 = tableMetaMap.get("tb3");
        Assert.assertEquals(TableType.SUB_TABLE, tb3.tableType);
        Assert.assertEquals("tb3", tb3.tbname);
        Assert.assertEquals(4, tb3.columns);
        Assert.assertEquals("stb2", tb3.stable_name);

        TableMeta weather = tableMetaMap.get("weather");
        Assert.assertEquals(TableType.NML_TABLE, weather.tableType);
        Assert.assertEquals("weather", weather.tbname);
        Assert.assertEquals(6, weather.columns);
        Assert.assertNull(weather.stable_name);
    }

    @Test
    public void loadColumnMetas() {
        // given
        SchemaManager schemaManager = new SchemaManager(conn);
        List<String> tables = Arrays.asList("stb1", "stb2", "tb1", "tb3", "weather");

        // when
        Map<String, List<ColumnMeta>> columnMetaMap = schemaManager.loadColumnMetas(tables);

        // then
        List<ColumnMeta> stb1 = columnMetaMap.get("stb1");
        Assert.assertEquals(4, stb1.size());
    }

    @Test
    public void loadTagTableNameMap() throws SQLException {
        // given
        SchemaManager schemaManager = new SchemaManager(conn);
        String table = "stb3";

        // when
        Map<String, String> tagTableMap = schemaManager.loadTagTableNameMap(table);

        // then
        Assert.assertEquals(2, tagTableMap.keySet().size());
        Assert.assertTrue(tagTableMap.containsKey("11.1abc"));
        Assert.assertTrue(tagTableMap.containsKey("22.2defg"));
        Assert.assertEquals("tb5", tagTableMap.get("11.1abc"));
        Assert.assertEquals("tb6", tagTableMap.get("22.2defg"));
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        conn = DriverManager.getConnection("jdbc:TAOS-RS://192.168.56.105:6041", "root", "taosdata");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists scm_test");
            stmt.execute("create database if not exists scm_test");
            stmt.execute("use scm_test");
            stmt.execute("create table stb1(ts timestamp, f1 int, f2 int) tags(t1 int)");
            stmt.execute("create table stb2(ts timestamp, f1 int, f2 int, f3 int) tags(t1 int, t2 int)");
            stmt.execute("insert into tb1 using stb1 tags(1) values(now, 1, 2)");
            stmt.execute("insert into tb2 using stb1 tags(2) values(now, 1, 2)");
            stmt.execute("insert into tb3 using stb2 tags(1,1) values(now, 1, 2, 3)");
            stmt.execute("insert into tb4 using stb2 tags(2,2) values(now, 1, 2, 3)");
            stmt.execute("create table weather(ts timestamp, f1 int, f2 int, f3 int, t1 int, t2 int)");
            stmt.execute("create table stb3(ts timestamp, f1 int) tags(t1 int, t2 float, t3 nchar(32))");
            stmt.execute("insert into tb5 using stb3 tags(1,1.1,'abc') values(now, 1)");
            stmt.execute("insert into tb6 using stb3 tags(2,2.2,'defg') values(now, 2)");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}