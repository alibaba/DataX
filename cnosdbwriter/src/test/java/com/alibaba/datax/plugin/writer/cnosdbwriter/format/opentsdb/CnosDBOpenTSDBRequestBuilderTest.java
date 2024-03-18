package com.alibaba.datax.plugin.writer.cnosdbwriter.format.opentsdb;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriter;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriterConfigFieldExtra;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.ICnosDBRequestBuilder;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
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
            Optional<CharSequence> write_batch = builder.append(r1);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            CnosDBOpenTSDBPoint p2 = new CnosDBOpenTSDBPoint(10002, "test", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 value=1 10001000000\n" +
                    "test,ta=a2 value=2 10002000000\n", write_batch.get().toString());
        }
        builder.clear();
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a3");
            CnosDBOpenTSDBPoint p3 = new CnosDBOpenTSDBPoint(10003, "test", tags, 3.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a4");
            CnosDBOpenTSDBPoint p4 = new CnosDBOpenTSDBPoint(10004, "test", tags, 4.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("test,ta=a3 value=3.0 10003000000\n" +
                    "test,ta=a4 value=4.0 10004000000\n", write_batch.get().toString());
        }
    }

    public void testAppendRecordWithExtra() {
        int precisionToMultiplier = CnosDBWriter.precisionToMultiplier("ms");
        Map<String, String> tagsExtra = new HashMap<String, String>() {{
            put("t1", "t1_1");
            put("t2", "t2_1");
        }};
        Map<String, String> tagsExtraMetric2 = new HashMap<String, String>() {{
            put("ta", "ta_1");
            put("tb", "tb_1");
        }};
        Map<String, CnosDBWriterConfigFieldExtra> fieldsExtra = new HashMap<String, CnosDBWriterConfigFieldExtra>() {{
            put("metric_1", new CnosDBWriterConfigFieldExtra("table_1", "f1", null));
            put("metric_2", new CnosDBWriterConfigFieldExtra("table_2", "f2", tagsExtraMetric2));
            put("metric_3", new CnosDBWriterConfigFieldExtra("table_2", "f3", tagsExtraMetric2));
        }};
        CnosDBWriter.mergeFieldsExtraTags(fieldsExtra, tagsExtra);

        ICnosDBRequestBuilder builder = new CnosDBOpenTSDBRequestBuilder(1024, 2, precisionToMultiplier,
                tagsExtra, fieldsExtra);

        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            CnosDBOpenTSDBPoint p1 = new CnosDBOpenTSDBPoint(10001, "metric_1", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("tb", "b1");
            CnosDBOpenTSDBPoint p2 = new CnosDBOpenTSDBPoint(10002, "metric_2", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("table_1,ta=a1,t1=t1_1,t2=t2_1 f1=1 10001000000\n" +
                    "table_2,tb=b1,ta=ta_1 f2=2 10002000000\n", write_batch.get().toString());
        }
        builder.clear();

        // Test set the default extra tags.
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            CnosDBOpenTSDBPoint p3 = new CnosDBOpenTSDBPoint(10003, "metric_2", tags, 3.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("tb", "b2");
            CnosDBOpenTSDBPoint p4 = new CnosDBOpenTSDBPoint(10004, "metric_2", tags, 4.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("table_2,ta=a2,tb=tb_1 f2=3.0 10003000000\n" +
                    "table_2,tb=b2,ta=ta_1 f2=4.0 10004000000\n", write_batch.get().toString());
        }
        builder.clear();

        // Test metrics to be put to the same table.
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a3");
            CnosDBOpenTSDBPoint p3 = new CnosDBOpenTSDBPoint(10005, "metric_2", tags, 5.0);
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn(p3.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r3);
            assert !write_batch.isPresent();
        }
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("tb", "b3");
            CnosDBOpenTSDBPoint p4 = new CnosDBOpenTSDBPoint(10006, "metric_3", tags, 6.0);
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn(p4.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r4);
            assert write_batch.isPresent();
            assertEquals("table_2,ta=a3,tb=tb_1 f2=5.0 10005000000\n" +
                    "table_2,tb=b3,ta=ta_1 f3=6.0 10006000000\n", write_batch.get().toString());
        }
    }

    public void testPrecisionMultiplier() {
        int precisionToMultiplier = CnosDBWriter.precisionToMultiplier("s");
        ICnosDBRequestBuilder builder = new CnosDBOpenTSDBRequestBuilder(0, 0, precisionToMultiplier);
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a1");
            CnosDBOpenTSDBPoint p1 = new CnosDBOpenTSDBPoint(10001, "test", tags, 1L);
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn(p1.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r1);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 value=1 10001000000000\n", write_batch.get().toString());
        }
        builder.clear();
        {
            HashMap<String, String> tags = new HashMap<>();
            tags.put("ta", "a2");
            CnosDBOpenTSDBPoint p2 = new CnosDBOpenTSDBPoint(16876L, "test", tags, 2L);
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn(p2.toJSON()));
            Optional<CharSequence> write_batch = builder.append(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a2 value=2 16876000000000\n", write_batch.get().toString());
        }
    }
}
