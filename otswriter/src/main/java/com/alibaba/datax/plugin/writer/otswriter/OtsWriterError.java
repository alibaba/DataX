package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.common.spi.ErrorCode;

public class OtsWriterError implements ErrorCode {
    
    private String code;
    
    private String description;
    
    // TODO
    // 这一块需要DATAX来统一定义分类， OTS基于这些分类在细化
    // 所以暂定两个基础的Error Code，其他错误统一使用OTS的错误码和错误消息
    
    public final static OtsWriterError ERROR = new OtsWriterError(
            "OtsWriterError", 
            "This error represents an internal error of the ots writer plugin, which indicates that the system is not processed.");
    public final static OtsWriterError INVALID_PARAM = new OtsWriterError(
            "OtsWriterInvalidParameter", 
            "This error represents a parameter error, indicating that the user entered the wrong parameter format.");
    
    public OtsWriterError (String code) {
        this.code = code;
        this.description = code;
    }
    
    public OtsWriterError (String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
    
    @Override
    public String toString() {
        return "[ code:" + this.code + ", message:" + this.description + "]";
    }
}
