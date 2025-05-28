package com.alibaba.datax.plugin.writer.restwriter.validator;

import java.util.Map;

import com.alibaba.datax.common.util.Configuration;

/**
 * Created by zhangyongxiang on 2023/8/25 8:09 PM
 **/
public class HeadersParameterValidator
        implements ParameterValidator<Map<String, Object>> {
    
    @Override
    public void validateImmediateValue(final Map<String, Object> parameter) {
        
    }
    
    @Override
    public void validate(final Configuration config, final String path) {
        validateImmediateValue(config.getMap(path));
    }
}
