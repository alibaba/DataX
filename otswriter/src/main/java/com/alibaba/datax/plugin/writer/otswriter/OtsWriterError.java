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
            "该错误表示插件的内部错误，表示系统没有处理到的异常");
    public final static OtsWriterError INVALID_PARAM = new OtsWriterError(
            "OtsWriterInvalidParameter", 
            "该错误表示参数错误，表示用户输入了错误的参数格式等");
    
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
        return this.code;
    }
}
