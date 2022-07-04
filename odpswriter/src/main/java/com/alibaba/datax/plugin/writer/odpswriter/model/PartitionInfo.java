package com.alibaba.datax.plugin.writer.odpswriter.model;

public class PartitionInfo {
    /**
     * 字段名
     */
    private String name;
    /**
     * String
     */
    private String type;
    /**
     * eventTime or function
     * yyyy/MM/dd/HH/mm
     * 可自定义组合
     */
    private String valueMode;
    private String value;
    private String comment;
    /**
     * 自定义分区有效
     * eventTime / constant
     * function
     */
    private String category;
    /**
     * 当 partitionType 为function时
     * functionExpression 为 valueMode 对应的expression
     */
    private String functionExpression;

    public String getFunctionExpression() {
        return functionExpression;
    }

    public void setFunctionExpression(String functionExpression) {
        this.functionExpression = functionExpression;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValueMode() {
        return valueMode;
    }

    public void setValueMode(String valueMode) {
        this.valueMode = valueMode;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
