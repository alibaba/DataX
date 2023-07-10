package com.alibaba.datax.plugin.writer.cnosdbwriter.format.opentsdb;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriter;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.ICnosDBRequestBuilder;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Optional;

public class CnosDBOpenTSDBRequestBuilderTest extends TestCase {
    public void testAppendRecord() {
        int precisionToMultiplier = CnosDBWriter.precisionToMultiplier("ms");
        ICnosDBRequestBuilder builder = new CnosDBOpenTSDBRequestBuilder(1024, 2, precisionToMultiplier);

        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            CnosDBOpenTSDBPoint p1 = new CnosDBOpenTSDBPoint(10001, "test", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<String> write_batch = builder.appendRecord(r1);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            CnosDBOpenTSDBPoint p2 = new CnosDBOpenTSDBPoint(10002, "test", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<String> write_batch = builder.appendRecord(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 value=1 10001000000\n" +
                    "test,ta=a2 value=2 10002000000\n", write_batch.get());
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a3");
            CnosDBOpenTSDBPoint p3 = new CnosDBOpenTSDBPoint(10003, "test", tags, 3.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<String> write_batch = builder.appendRecord(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a4");
            CnosDBOpenTSDBPoint p4 = new CnosDBOpenTSDBPoint(10004, "test", tags, 4.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<String> write_batch = builder.appendRecord(r4);
            assert write_batch.isPresent();
            assertEquals("test,ta=a3 value=3.0 10003000000\n" +
                    "test,ta=a4 value=4.0 10004000000\n", write_batch.get());
        }
    }

    public void testPrecisionMultiplier() {
        int precisionToMultiplier = CnosDBWriter.precisionToMultiplier("ns");
        ICnosDBRequestBuilder builder = new CnosDBOpenTSDBRequestBuilder(0, 0, precisionToMultiplier);
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            CnosDBOpenTSDBPoint p1 = new CnosDBOpenTSDBPoint(10001, "test", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<String> write_batch = builder.appendRecord(r1);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 value=1 10001\n", write_batch.get());
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            CnosDBOpenTSDBPoint p2 = new CnosDBOpenTSDBPoint(1687622400000L, "test", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<String> write_batch = builder.appendRecord(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a2 value=2 1687622400000\n", write_batch.get());
        }
    }
}
