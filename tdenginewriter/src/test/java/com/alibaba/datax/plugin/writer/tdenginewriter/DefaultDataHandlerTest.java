package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DefaultDataHandlerTest {

    private static final String host = "192.168.1.93";
    private static Connection conn;

    private final TaskPluginCollector taskPluginCollector = new TDengineWriter.Task().getTaskPluginCollector();

    @Test
    public void writeSupTableBySQL() throws SQLException {
        // given
        createSupAndSubTable();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"tbname\", \"ts\", \"f1\", \"f2\", \"t1\"]," +
                "\"table\":[\"stb1\"]," +
                "\"jdbcUrl\":\"jdbc:TAOS-RS://" + host + ":6041/test\"," +
                "\"batchSize\": \"1000\"" +
                "}");
        long current = System.currentTimeMillis();
        List<Record> recordList = IntStream.range(1, 11).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("tb" + (i + 10)));
            record.addColumn(new DateColumn(current + 1000 * i));
            record.addColumn(new LongColumn(1));
            record.addColumn(new LongColumn(2));
            record.addColumn(new LongColumn(i));
            return record;
        }).collect(Collectors.toList());


        // when
        DefaultDataHandler handler = new DefaultDataHandler(configuration, taskPluginCollector);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(conn);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setTbnameColumnMetasMap(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then
        Assert.assertEquals(10, count);
    }

    @Test
    public void writeSupTableBySQL_2() throws SQLException {
        // given
        createSupAndSubTable();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"tbname\", \"ts\", \"f1\", \"t1\"]," +
                "\"table\":[\"stb1\"]," +
                "\"jdbcUrl\":\"jdbc:TAOS-RS://" + host + ":6041/test\"," +
                "\"batchSize\": \"1000\"" +
                "}");
        long current = System.currentTimeMillis();
        List<Record> recordList = IntStream.range(1, 11).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("tb" + (i + 10)));
            record.addColumn(new DateColumn(current + 1000 * i));
            record.addColumn(new LongColumn(1));
            record.addColumn(new LongColumn(i));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(configuration, taskPluginCollector);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(conn);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setTbnameColumnMetasMap(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then
        Assert.assertEquals(10, count);
    }

    @Test
    public void writeSupTableBySchemaless() throws SQLException {
        // given
        createSupTable();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"ts\", \"f1\", \"f2\", \"t1\"]," +
                "\"table\":[\"stb1\"]," +
                "\"jdbcUrl\":\"jdbc:TAOS://" + host + ":6030/scm_test\"," +
                "\"batchSize\": \"1000\"" +
                "}");
        String jdbcUrl = configuration.getString("jdbcUrl");
        Connection connection = DriverManager.getConnection(jdbcUrl, "root", "taosdata");
        long current = System.currentTimeMillis();
        List<Record> recordList = IntStream.range(1, 11).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new DateColumn(current + 1000 * i));
            record.addColumn(new LongColumn(1));
            record.addColumn(new LongColumn(2));
            record.addColumn(new StringColumn("t" + i + " 22"));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(configuration, taskPluginCollector);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(connection);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setTbnameColumnMetasMap(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(connection, recordList);

        // then
        Assert.assertEquals(10, count);
    }

    @Test
    public void writeSubTableWithTableName() throws SQLException {
        // given
        createSupAndSubTable();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"tbname\", \"ts\", \"f1\", \"f2\", \"t1\"]," +
                "\"table\":[\"tb1\"]," +
                "\"jdbcUrl\":\"jdbc:TAOS-RS://" + host + ":6041/test\"," +
                "\"batchSize\": \"1000\"" +
                "}");
        long current = System.currentTimeMillis();
        List<Record> recordList = IntStream.range(1, 11).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn("tb" + i));
            record.addColumn(new DateColumn(current + 1000 * i));
            record.addColumn(new LongColumn(1));
            record.addColumn(new LongColumn(2));
            record.addColumn(new LongColumn(i));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(configuration, taskPluginCollector);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(conn);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setTbnameColumnMetasMap(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then
        Assert.assertEquals(1, count);
    }

    @Test
    public void writeSubTableWithoutTableName() throws SQLException {
        // given
        createSupAndSubTable();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"ts\", \"f1\", \"f2\", \"t1\"]," +
                "\"table\":[\"tb1\"]," +
                "\"jdbcUrl\":\"jdbc:TAOS-RS://" + host + ":6041/test\"," +
                "\"batchSize\": \"1000\"," +
                "\"ignoreTagsUnmatched\": \"true\"" +
                "}");
        long current = System.currentTimeMillis();
        List<Record> recordList = IntStream.range(1, 11).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new DateColumn(current + 1000 * i));
            record.addColumn(new LongColumn(1));
            record.addColumn(new LongColumn(2));
            record.addColumn(new LongColumn(i));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(configuration, taskPluginCollector);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(conn);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setTbnameColumnMetasMap(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then
        Assert.assertEquals(1, count);
    }

    @Test
    public void writeNormalTable() throws SQLException {
        // given
        createSupAndSubTable();
        Configuration configuration = Configuration.from("{" +
                "\"username\": \"root\"," +
                "\"password\": \"taosdata\"," +
                "\"column\": [\"ts\", \"f1\", \"f2\", \"t1\"]," +
                "\"table\":[\"weather\"]," +
                "\"jdbcUrl\":\"jdbc:TAOS-RS://" + host + ":6041/test\"," +
                "\"batchSize\": \"1000\"," +
                "\"ignoreTagsUnmatched\": \"true\"" +
                "}");
        long current = System.currentTimeMillis();
        List<Record> recordList = IntStream.range(1, 11).mapToObj(i -> {
            Record record = new DefaultRecord();
            record.addColumn(new DateColumn(current + 1000 * i));
            record.addColumn(new LongColumn(1));
            record.addColumn(new LongColumn(2));
            record.addColumn(new LongColumn(i));
            return record;
        }).collect(Collectors.toList());

        // when
        DefaultDataHandler handler = new DefaultDataHandler(configuration, taskPluginCollector);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(conn);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setTbnameColumnMetasMap(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(conn, recordList);

        // then
        Assert.assertEquals(10, count);
    }

    private void createSupAndSubTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists scm_test");
            stmt.execute("create database if not exists scm_test");
            stmt.execute("use scm_test");
            stmt.execute("create table stb1(ts timestamp, f1 int, f2 int) tags(t1 nchar(32))");
            stmt.execute("create table stb2(ts timestamp, f1 int, f2 int, f3 int) tags(t1 int, t2 int)");
            stmt.execute("create table tb1 using stb1 tags(1)");
            stmt.execute("create table tb2 using stb1 tags(2)");
            stmt.execute("create table tb3 using stb2 tags(1,1)");
            stmt.execute("create table tb4 using stb2 tags(2,2)");
            stmt.execute("create table weather(ts timestamp, f1 int, f2 int, f3 int, t1 int, t2 int)");
        }
    }

    private void createSupTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists scm_test");
            stmt.execute("create database if not exists scm_test");
            stmt.execute("use scm_test");
            stmt.execute("create table stb1(ts timestamp, f1 int, f2 int) tags(t1 nchar(32))");
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041", "root", "taosdata");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}