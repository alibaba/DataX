package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
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

public class DefaultDataHandlerTest2 {

    private static final String host = "192.168.1.93";
    private static Connection conn;

    @Test
    public void writeSupTableBySchemaless() throws SQLException {
        // given
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
        DefaultDataHandler handler = new DefaultDataHandler(configuration);
        List<String> tables = configuration.getList("table", String.class);
        SchemaManager schemaManager = new SchemaManager(connection);
        Map<String, TableMeta> tableMetas = schemaManager.loadTableMeta(tables);
        Map<String, List<ColumnMeta>> columnMetas = schemaManager.loadColumnMetas(tables);
        handler.setTableMetas(tableMetas);
        handler.setColumnMetas(columnMetas);
        handler.setSchemaManager(schemaManager);

        int count = handler.writeBatch(connection, recordList);

        // then
        Assert.assertEquals(10, count);
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041", "root", "taosdata");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists scm_test");
            stmt.execute("create database if not exists scm_test");
            stmt.execute("use scm_test");
            stmt.execute("create table stb1(ts timestamp, f1 int, f2 int) tags(t1 nchar(32))");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }
}