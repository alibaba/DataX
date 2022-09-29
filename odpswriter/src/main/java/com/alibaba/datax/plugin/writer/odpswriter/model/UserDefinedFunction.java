package com.alibaba.datax.plugin.writer.odpswriter.model;

import java.io.Serializable;
import java.util.List;

public class UserDefinedFunction implements Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
    private String expression;
    private String inputColumn;
    private List<UserDefinedFunctionRule> variableRule;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getInputColumn() {
        return inputColumn;
    }

    public void setInputColumn(String inputColumn) {
        this.inputColumn = inputColumn;
    }

    public List<UserDefinedFunctionRule> getVariableRule() {
        return variableRule;
    }

    public void setVariableRule(List<UserDefinedFunctionRule> variableRule) {
        this.variableRule = variableRule;
    }
}
