package com.alibaba.datax.plugin.writer.cnosdbwriter.format;

import com.alibaba.datax.common.element.Record;

import java.util.Optional;

public interface ICnosDBRequestBuilder {
    String FORMAT_DATAX = "datax";
    String FORMAT_OPENTSDB = "opentsdb";

    /**
     * Parse record to line protocol, insert into buffer.
     * If buffer is full, return the buffer as a CharSequence.
     *
     * @param record Alibaba DataX Record
     * @return An optional line protocol string.
     */
    Optional<CharSequence> append(Record record);

    /**
     * Return the buffered request as CharSequence
     *
     * @return buffered string
     */
    CharSequence get();

    /**
     * Clear the built data and buffers.
     */
    void clear();

    /**
     * Returns the buffer length
     *
     * @return buffered string length
     */
    int length();
}
