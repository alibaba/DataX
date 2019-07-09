package com.alibaba.datax.plugin.reader.tablestorereader;

import com.alibaba.datax.common.spi.ErrorCode;

public class TableStoreReaderError implements ErrorCode {
    
    private String code;
    
    private String description;
    
    // TODO
    // 这一块需要DATAX来统一定义分类， OTS基于这些分类在细化
    // 所以暂定两个基础的Error Code，其他错误统一使用OTS的错误码和错误消息
    
    public final static TableStoreReaderError ERROR = new TableStoreReaderError(
            "TableStoreReaderError",
            "该错误表示插件的内部错误，表示系统没有处理到的异常");
    public final static TableStoreReaderError INVALID_PARAM = new TableStoreReaderError(
            "OtsReaderInvalidParameter", 
            "该错误表示参数错误，表示用户输入了错误的参数格式等");
    
    public TableStoreReaderError(String code) {
        this.code = code;
        this.description = code;
    }
    
    public TableStoreReaderError(String code, String description) {
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
    
}
