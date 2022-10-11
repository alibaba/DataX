package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import java.util.List;

public class PrimaryKeyInfo {

    /**
     * 主键类型:PrimaryKeyTypeEnum
     *
     * pk: 单个(业务)主键 specific: 联合主键
     */
    private String type;

    /**
     * 用户定义的联合主键的连接符号
     */
    private String fieldDelimiter;

    /**
     * 主键的列的名称
     */
    private List<String> column;

    public String getType() {
        return type;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public List<String> getColumn() {
        return column;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public void setColumn(List<String> column) {
        this.column = column;
    }
}