package com.alibaba.datax.plugin.writer.elasticsearchwriter;

import java.util.List;

/**
 * Created by xiongfeng.bxf on 17/3/2.
 */
public class ElasticSearchColumn {

    private String name;//: "appkey",

    private String type;//": "TEXT",

    private String timezone;

    /**
     * 源头数据格式化处理，datax做的事情
     */
    private String format;

    /**
     * 目标端格式化，es原生支持的格式
     */
    private String dstFormat;

    private boolean array;

    /**
     * 是否使用目标端(ES原生)数组类型
     *
     * 默认是false
     */
    private boolean dstArray = false;

    private boolean jsonArray;

    private boolean origin;

    private List<String> combineFields;

    private String combineFieldsValueSeparator = "-";

    public String getCombineFieldsValueSeparator() {
        return combineFieldsValueSeparator;
    }

    public void setCombineFieldsValueSeparator(String combineFieldsValueSeparator) {
        this.combineFieldsValueSeparator = combineFieldsValueSeparator;
    }

    public List<String> getCombineFields() {
        return combineFields;
    }

    public void setCombineFields(List<String> combineFields) {
        this.combineFields = combineFields;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTimeZone(String timezone) {
        this.timezone = timezone;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isOrigin() { return origin; }

    public void setOrigin(boolean origin) { this.origin = origin; }

    public String getTimezone() {
        return timezone;
    }

    public String getFormat() {
        return format;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public boolean isArray() {
        return array;
    }

    public void setArray(boolean array) {
        this.array = array;
    }

    public boolean isJsonArray() {return jsonArray;}

    public void setJsonArray(boolean jsonArray) {this.jsonArray = jsonArray;}

    public String getDstFormat() {
        return dstFormat;
    }

    public void setDstFormat(String dstFormat) {
        this.dstFormat = dstFormat;
    }

    public boolean isDstArray() {
        return dstArray;
    }

    public void setDstArray(boolean dstArray) {
        this.dstArray = dstArray;
    }
}
