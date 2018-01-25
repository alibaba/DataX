package com.alibaba.datax.core.transport.transformer;

import java.util.List;
import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/16.
 */
public class TransformerExecutionParas {

    /**
     * 以下是function参数
     */

    private Integer columnIndex;
    private String[] paras;
    private Map<String, Object> tContext;
    private String code;
    private List<String> extraPackage;


    public Integer getColumnIndex() {
        return columnIndex;
    }

    public String[] getParas() {
        return paras;
    }

    public Map<String, Object> gettContext() {
        return tContext;
    }

    public String getCode() {
        return code;
    }

    public List<String> getExtraPackage() {
        return extraPackage;
    }

    public void setColumnIndex(Integer columnIndex) {
        this.columnIndex = columnIndex;
    }

    public void setParas(String[] paras) {
        this.paras = paras;
    }

    public void settContext(Map<String, Object> tContext) {
        this.tContext = tContext;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public void setExtraPackage(List<String> extraPackage) {
        this.extraPackage = extraPackage;
    }
}
