package com.alibaba.datax.plugin.writer.adswriter.odps;

/**
 * ODPS列属性，包含列名和类型 列名和类型与SQL的DESC表或分区显示的列名和类型一致
 *
 * @since 0.0.1
 */
public class FieldSchema {

    /** 列名 */
    private String name;

    /** 列类型，如：string, bigint, boolean, datetime等等 */
    private String type;

    private String comment;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
        builder.append("FieldSchema [name=").append(name).append(", type=").append(type).append(", comment=")
                .append(comment).append("]");
        return builder.toString();
    }

    /**
     * @return "col_name data_type [COMMENT col_comment]"
     */
    public String toDDL() {
        StringBuilder builder = new StringBuilder();
        builder.append(name).append(" ").append(type);
        String comment = this.comment;
        if (comment != null && comment.length() > 0) {
            builder.append(" ").append("COMMENT \"" + comment + "\"");
        }
        return builder.toString();
    }

}
