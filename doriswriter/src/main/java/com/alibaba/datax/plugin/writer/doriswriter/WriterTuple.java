package com.alibaba.datax.plugin.writer.doriswriter;

public class WriterTuple {
    private final String label;
    private final long bytes;
    private final int rows;
    private final byte[] data;
    private final boolean poison;

    public WriterTuple(String label, long bytes, int rows, byte[] data) {
        this.label = label;
        this.bytes = bytes;
        this.rows = rows;
        this.data = data;
        this.poison = false;
    }

    private WriterTuple(boolean poison) {
        this.label = "";
        this.bytes = 0L;
        this.rows = 0;
        this.data = null;
        this.poison = poison;
    }

    public static WriterTuple poisonPill() {
        return new WriterTuple(true);
    }

    public String getLabel() {
        return label;
    }

    public long getBytes() {
        return bytes;
    }

    public int getRows() {
        return rows;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isPoison() {
        return poison;
    }
}
