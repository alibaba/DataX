package com.alibaba.datax.plugin.writer.adswriter.ads;

/**
 * ADS column meta.<br>
 * <p>
 * select ordinal_position,column_name,data_type,type_name,column_comment <br>
 * from information_schema.columns <br>
 * where table_schema='db_name' and table_name='table_name' <br>
 * and is_deleted=0 <br>
 * order by ordinal_position limit 1000 <br>
 * </p>
 *
 * @since 0.0.1
 */
public class ColumnInfo {

    private int ordinal;
    private String name;
    private ColumnDataType dataType;
    private boolean isDeleted;
    private String comment;

    public int getOrdinal() {
        return ordinal;
    }

    public void setOrdinal(int ordinal) {
        this.ordinal = ordinal;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnDataType getDataType() {
        return dataType;
    }

    public void setDataType(ColumnDataType dataType) {
        this.dataType = dataType;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ColumnInfo [ordinal=").append(ordinal).append(", name=").append(name).append(", dataType=")
                .append(dataType).append(", isDeleted=").append(isDeleted).append(", comment=").append(comment)
                .append("]");
        return builder.toString();
    }

}
