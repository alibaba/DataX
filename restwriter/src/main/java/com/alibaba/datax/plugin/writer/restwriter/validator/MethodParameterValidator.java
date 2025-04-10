package com.alibaba.datax.plugin.writer.restwriter.validator;

import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;

import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.METHOD_INVALID_EXCEPTION;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 18:39
 **/
public class MethodParameterValidator implements ParameterValidator<String> {
    
    private final List<String> method = Lists.newArrayList("get", "post", "put",
            "patch", "delete");
    
    @Override
    public void validateImmediateValue(final String parameter) {
        if (isBlank(parameter)) {
            throw DataXException.asDataXException(METHOD_INVALID_EXCEPTION,
                    "需要填写method参数");
        }
        if (!this.method.contains(parameter.toLowerCase())) {
            throw DataXException.asDataXException(METHOD_INVALID_EXCEPTION,
                    "method参数值不合法");
        }
    }
    
    @Override
    public void validate(final Configuration config, final String path) {
        validateImmediateValue(config.getString(path));
    }
}
