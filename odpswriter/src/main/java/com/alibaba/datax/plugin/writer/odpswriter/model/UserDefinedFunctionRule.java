package com.alibaba.datax.plugin.writer.odpswriter.model;

import java.io.Serializable;
import java.util.List;

public class UserDefinedFunctionRule implements Serializable {
	private static final long serialVersionUID = 1L;
	private String type;
    private List<String> params;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }
}
