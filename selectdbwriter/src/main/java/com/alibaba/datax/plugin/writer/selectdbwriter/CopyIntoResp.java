package com.alibaba.datax.plugin.writer.selectdbwriter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CopyIntoResp extends BaseResponse{
    private String code;
    private String exception;

    private Map<String,String> result;

    public String getDataCode() {
        return code;
    }

    public String getException() {
        return exception;
    }

    public Map<String, String> getResult() {
        return result;
    }

}
