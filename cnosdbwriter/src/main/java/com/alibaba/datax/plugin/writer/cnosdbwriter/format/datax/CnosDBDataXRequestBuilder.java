package com.alibaba.datax.plugin.writer.cnosdbwriter.format.datax;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriteBatchBuilder;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.ICnosDBRequestBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class CnosDBDataXRequestBuilder implements ICnosDBRequestBuilder {
    private final String table;
    private final Map<Integer, String> tagIndexes;
    private final Map<Integer, String> fieldIndexes;
    private final Integer timeIndex;

    private int count;
    private final int batchSize;
    private final CnosDBWriteBatchBuilder buffer;
    private final Consumer<Long> timeAppender;
    /**
     * Config tagsExtra, tags also in tagIndexes will be removed,
     * and this field will never be null.
     */
    private final Map<String, String> tagsExtra;

    public CnosDBDataXRequestBuilder(
            int capacity, int batchSize, int precisionMultiplier, String table,
            Map<Integer, String> tagIndexes, Map<Integer, String> fieldIndexes, Integer timeIndex
    ) {
        this(capacity, batchSize, precisionMultiplier, table, tagIndexes, fieldIndexes, timeIndex, null);
    }

    public CnosDBDataXRequestBuilder(
            int capacity, int batchSize, int precisionMultiplier, String table,
            Map<Integer, String> tagIndexes, Map<Integer, String> fieldIndexes, Integer timeIndex,
            Map<String, String> tagsExtra
    ) {
        this.table = table;
        this.tagIndexes = tagIndexes;
        this.fieldIndexes = fieldIndexes;
        this.timeIndex = timeIndex;

        this.count = 0;
        this.batchSize = batchSize;
        this.buffer = new CnosDBWriteBatchBuilder(capacity);

        if (precisionMultiplier == 1) {
            this.timeAppender = this.buffer::appendTime;
        } else {
            // For string 1000000, get 000000
            String suffix = String.valueOf(precisionMultiplier).substring(1);
            this.timeAppender = i -> {
                this.buffer.appendTime(i);
                this.buffer.appendTime(suffix);
            };
        }

        if (tagsExtra == null) {
            this.tagsExtra = new HashMap<>(0);
        } else {
            this.tagsExtra = tagsExtra;
        }
    }

    @Override
    public Optional<CharSequence> append(Record record) {
        this.buffer.startWriteRecord(this.table);
        for (int i = 0; i < record.getColumnNumber(); i++) {
            Column col = record.getColumn(i);
            if (this.tagIndexes.containsKey(i)) {
                this.buffer.appendTag(this.tagIndexes.get(i), col.asString());
            } else if (this.fieldIndexes.containsKey(i)) {
                String key = this.fieldIndexes.get(i);
                switch (col.getType()) {
                    case BAD:
                    case NULL:
                        // Skip BAD or NULL.
                        break;
                    case INT:
                    case LONG:
                        this.buffer.appendBigintField(key, col.asString());
                        break;
                    case DOUBLE:
                        this.buffer.appendDoubleField(key, col.asString());
                        break;
                    case STRING:
                        this.buffer.appendStringField(key, col.asString());
                        break;
                    case BOOL:
                        this.buffer.appendBooleanField(key, col.asBoolean());
                        break;
                    case DATE:
                        // Transform DataX DATE to CnosDB BIGINT.
                        // Append suffix 'I' to integer values (INT or LONG).
                        this.buffer.appendBigintField(key, String.valueOf(col.asDate().getTime()));
                        break;
                    case BYTES:
                        break;
                }

            } else if (this.timeIndex == i) {
                this.timeAppender.accept(col.asDate().getTime());
            }
        }
        for (Map.Entry<String, String> e : this.tagsExtra.entrySet()) {
            this.buffer.appendTag(e.getKey(), e.getValue());
        }
        this.buffer.endWriteRecord();

        this.count += 1;
        if (this.buffer.isFull() || this.count >= this.batchSize) {
            this.count = 0;
            return Optional.of(this.buffer.getBuffer());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public CharSequence get() {
        return this.buffer.getBuffer();
    }

    @Override
    public void clear() {
        this.count = 0;
        this.buffer.clearBuffer();
    }

    @Override
    public int length() {
        return this.buffer.length();
    }
}
