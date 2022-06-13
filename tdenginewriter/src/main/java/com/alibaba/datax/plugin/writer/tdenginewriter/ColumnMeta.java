package com.alibaba.datax.plugin.writer.tdenginewriter;

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
        return "ColumnMeta{" +
                "field='" + field + '\'' +
                ", type='" + type + '\'' +
                ", length=" + length +
                ", note='" + note + '\'' +
                ", isTag=" + isTag +
                ", isPrimaryKey=" + isPrimaryKey +
                ", value=" + value +
                '}';
    }
}
