package com.alibaba.datax.plugin.writer.odpswriter;

public class DateTransForm {
    /**
     * 列名称
     */
    private String colName;

    /**
     * 之前是什么格式
     */
    private String fromFormat;

    /**
     * 要转换成什么格式
     */
    private String toFormat;

    public DateTransForm(String colName, String fromFormat, String toFormat) {
        this.colName = colName;
        this.fromFormat = fromFormat;
        this.toFormat = toFormat;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getFromFormat() {
        return fromFormat;
    }

    public void setFromFormat(String fromFormat) {
        this.fromFormat = fromFormat;
    }

    public String getToFormat() {
        return toFormat;
    }

    public void setToFormat(String toFormat) {
        this.toFormat = toFormat;
    }

    @Override
    public String toString() {
        return "DateTransForm{" +
                "colName='" + colName + '\'' +
                ", fromFormat='" + fromFormat + '\'' +
                ", toFormat='" + toFormat + '\'' +
                '}';
    }
}
