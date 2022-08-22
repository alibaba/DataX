package com.alibaba.datax.plugin.reader.elasticsearchreader;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author kesc
 * @date 2020-05-09 10:49
 */
public class EsField {
    private String name;
    private String alias;
    private Object value;
    private List<EsField> child;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<EsField> getChild() {
        return child;
    }

    public void setChild(List<EsField> child) {
        this.child = child;
    }

    public boolean hasChild() {
        return child != null && !child.isEmpty();
    }

    public String getAlias() {
        return alias;
    }

    public String getFinalName(String nameCase) {
        if (StringUtils.isNotBlank(alias)) {
            return alias;
        } else if (StringUtils.isBlank(name)) {
            return "";
        } else if ("UPPERCASE".equalsIgnoreCase(nameCase)) {
            return name.toUpperCase();
        } else if ("LOWERCASE".equalsIgnoreCase(nameCase)) {
            return name.toLowerCase();
        }
        return name;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
