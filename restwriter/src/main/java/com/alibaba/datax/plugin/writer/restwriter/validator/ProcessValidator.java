package com.alibaba.datax.plugin.writer.restwriter.validator;

import java.util.List;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.restwriter.conf.Operation;

import static com.alibaba.datax.plugin.writer.restwriter.Key.ADDITIONAL_CONCURRENT;
import static com.alibaba.datax.plugin.writer.restwriter.Key.ADDITIONAL_OPERATIONS;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.CONCURRENT_INVALID_EXCEPTION;
import static java.util.Objects.nonNull;
import static org.apache.commons.collections4.ListUtils.emptyIfNull;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/

public class ProcessValidator implements ParameterValidator<Configuration> {
    
    private final ParameterValidator<String> urlValidator;
    
    private final ParameterValidator<String> methodValidator;
    
    public ProcessValidator(ParameterValidator<String> urlValidator,
            ParameterValidator<String> methodValidator) {
        this.urlValidator = urlValidator;
        this.methodValidator = methodValidator;
    }
    
    @Override
    public void validateImmediateValue(final Configuration parameter) {
        if (nonNull(parameter)) {
            final String concurrent = parameter
                    .getString(ADDITIONAL_CONCURRENT);
            if (nonNull(concurrent) && !equalsIgnoreCase(concurrent, "true")
                    && !equalsIgnoreCase(concurrent, "false")) {
                throw DataXException.asDataXException(
                        CONCURRENT_INVALID_EXCEPTION,
                        String.format(
                                "parameter concurrent %s is invalid, allow values: true,false",
                                concurrent));
            }
            List<Operation> operations = parameter
                    .getListWithJson(ADDITIONAL_OPERATIONS, Operation.class);
            emptyIfNull(operations).forEach(operation -> {
                this.urlValidator.validateImmediateValue(operation.getUrl());
                this.methodValidator
                        .validateImmediateValue(operation.getMethod());
            });
        }
    }
    
    @Override
    public void validate(final Configuration config, final String path) {
        validateImmediateValue(config.getConfiguration(path));
    }
}
