package com.alibaba.datax.plugin.writer.cnosdbwriter;

public class CnosDBWriteBatchBuilder {
    private final StringBuilder buffer;
    private final int capacity;
    private final StringBuilder keyBuffer;
    private final StringBuilder valueBuffer;
    private final StringBuilder timeBuffer;

    public CnosDBWriteBatchBuilder(int capacity) {
        this.buffer = new StringBuilder(capacity);
        this.capacity = capacity;
        this.keyBuffer = new StringBuilder(capacity / 2);
        this.valueBuffer = new StringBuilder(capacity / 2);
        this.timeBuffer = new StringBuilder(32);
    }

    public String take() {
        String str = this.buffer.toString();
        this.buffer.setLength(0);
        return str;
    }

    public String takeKey() {
        String str = this.keyBuffer.toString();
        this.keyBuffer.setLength(0);
        return str;
    }

    public String takeValue() {
        String str = this.valueBuffer.toString();
        this.valueBuffer.setLength(0);
        return str;
    }

    public String takeTime() {
        String str = this.timeBuffer.toString();
        this.timeBuffer.setLength(0);
        return str;
    }

    public void startWriteRecord(Object table) {
        this.keyBuffer.setLength(0);
        this.keyBuffer.append(table);
        this.valueBuffer.setLength(0);
        this.timeBuffer.setLength(0);
    }

    public void endWriteRecord() {
        this.buffer.append(this.keyBuffer)
                .append(" ")
                .append(this.valueBuffer)
                .append(" ")
                .append(this.timeBuffer)
                .append('\n');
    }

    public void appendTag(String key, String value) {
        // TODO escape characters in tag key&values.
        this.keyBuffer.append(",").append(key).append('=').append(value);
    }

    /**
     * Append BIGINT value, with suffix 'i'.
     *
     * @param key   field key
     * @param value field value
     */
    public void appendBigintField(String key, String value) {
        if (this.valueBuffer.length() > 0) {
            this.valueBuffer.append(",");
        }
        this.valueBuffer.append(key).append('=');
        this.valueBuffer.append(value).append('i');
    }

    /**
     * Append DOUBLE value
     *
     * @param key   field key
     * @param value field value
     */
    public void appendDoubleField(String key, String value) {
        if (this.valueBuffer.length() > 0) {
            this.valueBuffer.append(",");
        }
        this.valueBuffer.append(key).append('=');
        this.valueBuffer.append(value);
    }

    /**
     * Append BOOLEAN value, 'T' if true, 'F' if false.
     *
     * @param key   field key
     * @param value field value
     */
    public void appendBooleanField(String key, boolean value) {
        if (this.valueBuffer.length() > 0) {
            this.valueBuffer.append(",");
        }
        this.valueBuffer.append(key).append('=');
        this.valueBuffer.append(value ? 'T' : 'F');
    }

    /**
     * Append STRING value, with surrounding double quotes.
     *
     * @param key   field key
     * @param value field value
     */
    public void appendStringField(String key, String value) {
        if (this.valueBuffer.length() > 0) {
            this.valueBuffer.append(",");
        }
        this.valueBuffer.append(key).append('=');
        this.appendEscapedStringField(value);
    }

    /**
     * insert value into valueBuffer, with surrounding double-quotes and escaped characters.
     *
     * @param value Field value
     */
    protected void appendEscapedStringField(String value) {
        this.valueBuffer.append('\"');
        // {escaped, }
        final boolean[] flags = {false};
        value.chars().mapToObj(c -> (char) c).forEach(c -> {
            if (flags[0]) {
                flags[0] = false;
                this.valueBuffer.append(c);
                return;
            }
            if (c == '\\') {
                flags[0] = true;
                this.valueBuffer.append(c);
                return;
            }
            if (c == '"') {
                this.valueBuffer.append('\\');
                this.valueBuffer.append(c);
                return;
            }

            this.valueBuffer.append(c);
        });
        this.valueBuffer.append('\"');
    }

    /**
     * Append time value for CnosDB table column 'time'.
     *
     * @param time time value.
     */
    public void appendTime(Object time) {
        this.timeBuffer.append(time);
    }

    public int length() {
        return this.buffer.length();
    }

    public boolean isFull() {
        return this.buffer.length() >= this.capacity;
    }

    @Override
    public String toString() {
        return this.buffer.toString();
    }
}
