package com.alibaba.datax.plugin.writer.tdengine30writer;

public class ColumnMeta {
    String field;
    String type;
    int length;
    String note;
    boolean isTag;
    boolean isPrimaryKey;
    Object value;

    @Override
    public String toString() {
        return "ColumnMeta{" + "field='" + field + '\'' + ", type='" + type + '\'' + ", length=" + length + ", note='" +
                note + '\'' + ", isTag=" + isTag + ", isPrimaryKey=" + isPrimaryKey + ", value=" + value + '}';
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ColumnMeta && this.field.equals(((ColumnMeta) obj).field);
    }
}
