package com.alibaba.datax.plugin.writer.restwriter.validator;

import com.alibaba.datax.common.util.Configuration;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 18:02
 **/
public interface ParameterValidator<T> {
    
    void validateImmediateValue(T parameter);
    
    void validate(Configuration config, String path);
}
