package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBReaderConfigTest {

    @Test
    public void buildsTreeQueryAndAppliesDefaults() {
        Configuration config = Configuration.from("{"
                + "\"host\":\"localhost\",\"port\":6667,"
                + "\"username\":\"user\",\"password\":\"secret\","
                + "\"device\":\"root.factory.device\","
                + "\"measurements\":[\"temperature\",\"humidity\"],"
                + "\"where\":\"time >= 0\"}");

        IoTDBReaderConfig.validateAndSetDefaults(config);

        assertEquals("tree", config.getString(Key.MODEL));
        assertEquals(1000, (int) config.getInt(Key.FETCH_SIZE));
        assertEquals("SELECT temperature,humidity FROM root.factory.device WHERE time >= 0",
                IoTDBReaderConfig.queries(config).get(0));
    }

    @Test
    public void tableModelRequiresExplicitQuery() {
        Configuration config = Configuration.from("{"
                + "\"model\":\"table\",\"host\":\"localhost\",\"port\":6667,"
                + "\"username\":\"user\",\"password\":\"secret\"}");
        try {
            IoTDBReaderConfig.validateAndSetDefaults(config);
            fail("Expected a missing-query validation error.");
        } catch (DataXException expected) {
            assertTrue(expected.getMessage().contains("querySql"));
        }
    }

    @Test
    public void mapsTableRowsWithTimeColumnAndTypedValues() {
        RowRecord row = new RowRecord(1234L);
        Field region = new Field(TSDataType.STRING);
        region.setBinaryV(new org.apache.tsfile.utils.Binary("north", StandardCharsets.UTF_8));
        Field value = new Field(TSDataType.DOUBLE);
        value.setDoubleV(21.5d);
        row.addField(region);
        row.addField(value);

        TestRecord record = new TestRecord();
        IoTDBReaderRecordMapper.append(row, record,
                list("Time", "region", "temperature"),
                list("TIMESTAMP", "STRING", "DOUBLE"), 0);

        assertEquals(3, record.getColumnNumber());
        assertEquals(Long.valueOf(1234L), record.getColumn(0).asLong());
        assertEquals("north", record.getColumn(1).asString());
        assertEquals(Double.valueOf(21.5d), record.getColumn(2).asDouble());
    }

    @Test
    public void preservesTypeForNullFields() {
        Column column = IoTDBReaderRecordMapper.toColumn(null, TSDataType.INT32);
        assertNull(column.getRawData());
    }

    private static List<String> list(String... values) {
        List<String> result = new ArrayList<>();
        for (String value : values) {
            result.add(value);
        }
        return result;
    }

    private static final class TestRecord implements Record {
        private final List<Column> columns = new ArrayList<>();

        @Override
        public void addColumn(Column column) {
            columns.add(column);
        }

        @Override
        public void setColumn(int i, Column column) {
            columns.set(i, column);
        }

        @Override
        public Column getColumn(int i) {
            return columns.get(i);
        }

        @Override
        public int getColumnNumber() {
            return columns.size();
        }

        @Override
        public int getByteSize() {
            return 0;
        }

        @Override
        public int getMemorySize() {
            return 0;
        }

        @Override
        public void setMeta(java.util.Map<String, String> meta) {
        }

        @Override
        public java.util.Map<String, String> getMeta() {
            return null;
        }

        @Override
        public String toString() {
            return columns.toString();
        }
    }
}
