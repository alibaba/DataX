package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBWriterRecordMapperTest {

    @Test
    public void mapsTreeRecordUsingExplicitTypesAndNonzeroTimePosition() {
        IoTDBWriterConfig config = IoTDBWriterConfig.validateAndSetDefaults(Configuration.from("{"
                + "\"model\":\"tree\",\"host\":\"localhost\",\"port\":6667,"
                + "\"username\":\"user\",\"password\":\"secret\","
                + "\"device\":\"root.factory.device\","
                + "\"measurements\":[\"count\",\"active\"],"
                + "\"dataTypes\":[\"INT32\",\"BOOLEAN\"],"
                + "\"timeColumnPosition\":1}"));
        TestRecord record = record(new StringColumn("42"), new DateColumn(1234L), new BoolColumn(true));
        Tablet tablet = IoTDBWriterRecordMapper.newTablet(config, 10);

        IoTDBWriterRecordMapper.append(record, tablet, config, 0);

        assertEquals(1, tablet.getRowSize());
        assertEquals(1234L, tablet.getTimestamps()[0]);
        assertEquals(42, ((int[]) tablet.getValues()[0])[0]);
        assertTrue(((boolean[]) tablet.getValues()[1])[0]);
    }

    @Test
    public void buildsTableTabletWithDeclaredColumnCategories() {
        IoTDBWriterConfig config = IoTDBWriterConfig.validateAndSetDefaults(Configuration.from("{"
                + "\"model\":\"table\",\"host\":\"localhost\",\"port\":6667,"
                + "\"username\":\"user\",\"password\":\"secret\","
                + "\"table\":\"readings\",\"columnNames\":[\"site\",\"temperature\"],"
                + "\"dataTypes\":[\"STRING\",\"DOUBLE\"],"
                + "\"columnCategories\":[\"TAG\",\"FIELD\"]}"));
        Tablet tablet = IoTDBWriterRecordMapper.newTablet(config, 10);
        TestRecord record = record(new DateColumn(5678L), new StringColumn("north"),
                new DoubleColumn(21.5d));

        IoTDBWriterRecordMapper.append(record, tablet, config, 0);

        assertEquals(5678L, tablet.getTimestamps()[0]);
        assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
        assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(1));
        assertEquals("north", new String(((org.apache.tsfile.utils.Binary[]) tablet.getValues()[0])[0]
                .getValues(), StandardCharsets.UTF_8));
        assertEquals(21.5d, ((double[]) tablet.getValues()[1])[0], 0.00001d);
    }

    @Test
    public void rejectsTableTagAfterField() {
        try {
            IoTDBWriterConfig.validateAndSetDefaults(Configuration.from("{"
                    + "\"model\":\"table\",\"host\":\"localhost\",\"port\":6667,"
                    + "\"username\":\"user\",\"password\":\"secret\","
                    + "\"table\":\"readings\",\"columnNames\":[\"value\",\"site\"],"
                    + "\"dataTypes\":[\"DOUBLE\",\"STRING\"],"
                    + "\"columnCategories\":[\"FIELD\",\"TAG\"]}"));
            fail("Expected a table column category validation error.");
        } catch (DataXException expected) {
            assertTrue(expected.getMessage().contains("TAG columns must precede"));
        }
    }

    @Test
    public void rejectsRecordWidthMismatch() {
        IoTDBWriterConfig config = IoTDBWriterConfig.validateAndSetDefaults(Configuration.from("{"
                + "\"host\":\"localhost\",\"port\":6667,"
                + "\"username\":\"user\",\"password\":\"secret\","
                + "\"device\":\"root.factory.device\",\"measurements\":[\"temperature\"],"
                + "\"dataTypes\":[\"DOUBLE\"]}"));
        try {
            IoTDBWriterRecordMapper.append(record(new DateColumn(1L)),
                    IoTDBWriterRecordMapper.newTablet(config, 1), config, 0);
            fail("Expected a record width validation error.");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("input columns including time"));
        }
    }

    private static TestRecord record(Column... columns) {
        TestRecord record = new TestRecord();
        for (Column column : columns) {
            record.addColumn(column);
        }
        return record;
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
