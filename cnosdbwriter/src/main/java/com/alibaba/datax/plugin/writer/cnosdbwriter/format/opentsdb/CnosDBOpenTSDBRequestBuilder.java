package com.alibaba.datax.plugin.writer.cnosdbwriter.format.opentsdb;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriteBatchBuilder;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriterErrorCode;
import com.alibaba.datax.plugin.writer.cnosdbwriter.CnosDBWriterException;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.ICnosDBRequestBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class CnosDBOpenTSDBRequestBuilder implements ICnosDBRequestBuilder {
    private int count;
    private final int batchSize;
    private final CnosDBWriteBatchBuilder buffer;
    private final Consumer<Long> timeAppender;

    public CnosDBOpenTSDBRequestBuilder(int capacity, int batchSize, int precisionMultiplier) {
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
    }

    @Override
    public Optional<String> appendRecord(Record record) throws CnosDBWriterException {
        String line = record.getColumn(0).asString();
        CnosDBOpenTSDBPoint pt;
        try {
            pt = CnosDBOpenTSDBPoint.fromJSON(line);
        } catch (Exception e) {
            String message = String.format("解码 JSON 格式的 OpenTSDB Point 失败: %s", e.getMessage());
            throw new CnosDBWriterException(CnosDBWriterErrorCode.ParseJSONOpenTSDB, message);
        }

        this.buffer.startWriteRecord(pt.getMetric());
        Map<String, String> tagsMap = pt.getTags();
        if (tagsMap != null && !tagsMap.isEmpty()) {
            for (Map.Entry<String, String> e : tagsMap.entrySet()) {
                this.buffer.appendTag(e.getKey(), e.getValue());
            }
        }
        // OpenTSDB value is long or double
        this.buffer.appendDoubleField("value", pt.getValue().toString());
        this.timeAppender.accept(pt.getTimestamp());
        this.buffer.endWriteRecord();

        this.count += 1;
        if (this.buffer.isFull() || this.count >= this.batchSize) {
            this.count = 0;
            return Optional.of(this.buffer.take());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String take() {
        this.count = 0;
        return this.buffer.take();
    }

    @Override
    public int length() {
        return this.buffer.length();
    }
}
