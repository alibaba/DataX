package com.alibaba.datax.plugin.writer.cnosdbwriter;

import junit.framework.TestCase;

public class CnosDBWriteBatchBuilderTest extends TestCase {

    public void testWriteBatchBuilder() {
        CnosDBWriteBatchBuilder builder = new CnosDBWriteBatchBuilder(8);
        {
            builder.startWriteRecord("table_1");
            builder.appendStringField("col_a", "a1");
            builder.appendTag("col_b", "b1 b1");
            builder.appendStringField("col_c", "c1");
            builder.appendTime("123");
            builder.appendBigintField("col_d", "1000");
            builder.appendDoubleField("col_e", "3.14");
            builder.endWriteRecord();

            assert builder.isFull();
            assertEquals("table_1,col_b=b1\\ b1 col_a=\"a1\",col_c=\"c1\",col_d=1000i,col_e=3.14 123\n", builder.getBuffer().toString());
            builder.clearBuffer();
            assert !builder.isFull();
            assert builder.isEmpty();
        }
        {
            builder.startWriteRecord("table_2");
            builder.appendTag("col_f", "f1");
            assertEquals("table_2,col_f=f1", builder.takeKey());
            assert builder.takeValue().isEmpty();
            assert builder.takeTime().isEmpty();
            builder.endWriteRecord();
            builder.clearBuffer();
        }
        {
            builder.startWriteRecord("table_3");
            builder.appendTag("col_g", "g1");
            builder.appendDoubleField("col_h", "2000");
            builder.appendTime("124");
            builder.endWriteRecord();

            builder.startWriteRecord("table_3");
            builder.appendTag("col_g", "g2");
            builder.appendDoubleField("col_h", "2001");
            builder.appendTime("125");
            builder.endWriteRecord();

            builder.startWriteRecord("table_4");
            builder.appendTag("col_i", "i1");
            builder.appendBigintField("col_j", "3000");
            builder.appendTime("126");
            builder.endWriteRecord();

            assertEquals("table_3,col_g=g1 col_h=2000 124\n" +
                    "table_3,col_g=g2 col_h=2001 125\n" +
                    "table_4,col_i=i1 col_j=3000i 126\n", builder.getBuffer().toString());
        }
    }

    public void testAppendEscapedFieldValue() {
        CnosDBWriteBatchBuilder builder = new CnosDBWriteBatchBuilder(8);

        // a -> "a"
        builder.appendEscapedStringField("a");
        assertEquals("\"a\"", builder.takeValue());

        // alpha -> "alpha"
        builder.appendEscapedStringField("alpha");
        assertEquals("\"alpha\"", builder.takeValue());

        // " -> "\""
        builder.appendEscapedStringField("\"");
        assertEquals("\"\\\"\"", builder.takeValue());

        // "" -> "\"\""
        builder.appendEscapedStringField("\"\"");
        assertEquals("\"\\\"\\\"\"", builder.takeValue());

        // \ab\\ -> "\ab\\"
        builder.appendEscapedStringField("\\ab\\\\");
        assertEquals("\"\\ab\\\\\"", builder.takeValue());
    }
}