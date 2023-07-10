package com.alibaba.datax.plugin.writer.cnosdbwriter.format.datax;

import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriter;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.ICnosDBRequestBuilder;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Optional;

public class CnosDBDataXRequestBuilderTest extends TestCase {

    public void testAppendRecord() {
        HashMap<Integer, String> tagIndexes = new HashMap<>();
        tagIndexes.put(0, "ta");
        HashMap<Integer, String> fieldIndexes = new HashMap<>();
        fieldIndexes.put(1, "fa");
        fieldIndexes.put(3, "fb");
        int timeIndex = 2;
        int precisionToMultiplier = CnosDBWriter.precisionToMultiplier("ms");
        ICnosDBRequestBuilder builder = new CnosDBDataXRequestBuilder(1024, 2, precisionToMultiplier, "test", tagIndexes, fieldIndexes, timeIndex);
        {
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn("a1"));
            r1.addColumn(new DoubleColumn(1.0));
            r1.addColumn(new LongColumn(10001L));
            r1.addColumn(new StringColumn("fb1"));
            Optional<String> write_batch = builder.appendRecord(r1);
            assert !write_batch.isPresent();
        }
        {
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn("a2"));
            r2.addColumn(new DoubleColumn(2F));
            r2.addColumn(new LongColumn(10002L));
            r2.addColumn(new StringColumn("fb2"));
            Optional<String> write_batch = builder.appendRecord(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 fa=1.0,fb=\"fb1\" 10001000000\n" +
                    "test,ta=a2 fa=2.0,fb=\"fb2\" 10002000000\n", write_batch.get());
        }
        {
            Record r3 = new DefaultRecord();
            r3.addColumn(new StringColumn("a3"));
            r3.addColumn(new DoubleColumn(3.0));
            r3.addColumn(new LongColumn(10003L));
            r3.addColumn(new StringColumn("fb3"));
            Optional<String> write_batch = builder.appendRecord(r3);
            assert !write_batch.isPresent();
        }
        {
            Record r4 = new DefaultRecord();
            r4.addColumn(new StringColumn("a4"));
            r4.addColumn(new DoubleColumn(4F));
            r4.addColumn(new LongColumn(10004L));
            r4.addColumn(new StringColumn("fb4"));
            Optional<String> write_batch = builder.appendRecord(r4);
            assert write_batch.isPresent();
            assertEquals("test,ta=a3 fa=3.0,fb=\"fb3\" 10003000000\n" +
                    "test,ta=a4 fa=4.0,fb=\"fb4\" 10004000000\n", write_batch.get());
        }
    }

    public void testPrecisionMultiplier() {
        HashMap<Integer, String> tagIndexes = new HashMap<>();
        tagIndexes.put(0, "ta");
        HashMap<Integer, String> fieldIndexes = new HashMap<>();
        fieldIndexes.put(1, "fa");
        fieldIndexes.put(3, "fb");
        int timeIndex = 2;
        int precisionMultiplier = CnosDBWriter.precisionToMultiplier("ns");
        ICnosDBRequestBuilder builder = new CnosDBDataXRequestBuilder(0, 0, precisionMultiplier, "test", tagIndexes, fieldIndexes, timeIndex);
        {
            Record r1 = new DefaultRecord();
            r1.addColumn(new StringColumn("a1"));
            r1.addColumn(new DoubleColumn(1.0));
            r1.addColumn(new LongColumn(10001L));
            r1.addColumn(new StringColumn("fb1"));
            Optional<String> write_batch = builder.appendRecord(r1);
            assert write_batch.isPresent();
            assertEquals("test,ta=a1 fa=1.0,fb=\"fb1\" 10001\n", write_batch.get());
        }
        {
            Record r2 = new DefaultRecord();
            r2.addColumn(new StringColumn("a2"));
            r2.addColumn(new DoubleColumn(2.0));
            r2.addColumn(new LongColumn(1687622400000L));
            r2.addColumn(new StringColumn("fb2"));
            Optional<String> write_batch = builder.appendRecord(r2);
            assert write_batch.isPresent();
            assertEquals("test,ta=a2 fa=2.0,fb=\"fb2\" 1687622400000\n", write_batch.get());
        }

    }
}
