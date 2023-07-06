package com.alibaba.datax.plugin.writer.neo4jwriter.config;

/**
 * 由于dataX并不能传输数据的元数据，所以只能在writer端定义每列数据的名字
 * datax does not support data metadata,
 * only the name of each column of data can be defined on neo4j writer
 *
 * @author fuyouj
 */
public class Neo4jProperty {
    public static final String DEFAULT_SPLIT = ",";

    /**
     * name of neo4j field
     */
    private String name;

    /**
     * neo4j type
     * reference by org.neo4j.driver.Values
     */
    private String type;

    /**
     * for date
     */
    private String dateFormat;

    /**
     * for array type
     */
    private String split;

    public Neo4jProperty() {
    }

    public Neo4jProperty(String name, String type, String format, String split) {
        this.name = name;
        this.type = type;
        this.dateFormat = format;
        this.split = split;
    }

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

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getSplit() {
        return getSplitOrDefault();
    }

    public String getSplitOrDefault() {
        if (split == null || "".equals(split)) {
            return DEFAULT_SPLIT;
        }
        return split;
    }

    public void setSplit(String split) {
        this.split = split;
    }
}
