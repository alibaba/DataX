package com.alibaba.datax.plugin.writer.neo4jwriter.config;

import com.alibaba.datax.plugin.writer.neo4jwriter.element.FieldType;

import java.util.Arrays;
import java.util.List;

/**
 * 由于dataX并不能传输数据的元数据，所以只能在writer端定义每列数据的名字
 * datax does not support data metadata,
 * only the name of each column of data can be defined on neo4j writer
 * @author fuyouj
 */
public class Neo4jField {
    public static final String DEFAULT_SPLIT = ",";
    public static final List<Character> DEFAULT_ARRAY_TRIM = Arrays.asList('[',']');

    /**
     * name of neo4j field
     */
    private String fieldName;

    /**
     * neo4j type
     * reference by org.neo4j.driver.Values
     */
    private FieldType fieldType;

    /**
     * for date
     */
    private String dateFormat;

    /**
     * for array type
     */
    private String split;
    /**
     * such as [1,2,3,4,5]
     * split is ,
     * arrayTrimChar is [ ]
     */
    private List<Character> arrayTrimChars;

    public Neo4jField(){}

    public Neo4jField(String fieldName, FieldType fieldType, String format, String split, List<Character> arrayTrimChars) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.dateFormat = format;
        this.split = split;
        this.arrayTrimChars = arrayTrimChars;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
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

    public String getSplitOrDefault(){
        if (split == null || "".equals(split)){
            return DEFAULT_SPLIT;
        }
        return split;
    }

    public void setSplit(String split) {
        this.split = split;
    }

    public List<Character> getArrayTrimChars() {
        return getArrayTrimOrDefault();
    }

    public List<Character> getArrayTrimOrDefault(){
        if (arrayTrimChars == null || arrayTrimChars.isEmpty()){
            return DEFAULT_ARRAY_TRIM;
        }
        return arrayTrimChars;
    }

    public void setArrayTrimChars(List<Character> arrayTrimChars) {
        this.arrayTrimChars = arrayTrimChars;
    }
}
