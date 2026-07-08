package com.alibaba.datax.plugin.writer.restwriter.validator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.restwriter.conf.Field;
import com.google.common.collect.Sets;

import static com.alibaba.datax.plugin.writer.restwriter.Key.BATCH_SIZE;
import static com.alibaba.datax.plugin.writer.restwriter.Key.FIELDS;
import static com.alibaba.datax.plugin.writer.restwriter.Key.HTTP_HEADERS;
import static com.alibaba.datax.plugin.writer.restwriter.Key.HTTP_METHOD;
import static com.alibaba.datax.plugin.writer.restwriter.Key.MAX_RETRIES;
import static com.alibaba.datax.plugin.writer.restwriter.Key.POSTPROCESS;
import static com.alibaba.datax.plugin.writer.restwriter.Key.PREPROCESS;
import static com.alibaba.datax.plugin.writer.restwriter.Key.RATE_PER_TASK;
import static com.alibaba.datax.plugin.writer.restwriter.Key.URL;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriter.DEFAULT_BATCH_SIZE_VALUE;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriter.DEFAULT_MAX_RETRIES_VALUE;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.BATCH_SIZE_INVALID_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.EMPTY_FIELD_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.FIELDS_INVALID_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.MAX_RETRIES_INVALID_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.RATE_PER_TASK_INVALID_EXCEPTION;
import static java.util.Objects.nonNull;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 18:32
 **/
public class ConfigurationValidator
        implements ParameterValidator<Configuration> {
    
    private final ParameterValidator<String> urlValidator;
    
    private final ParameterValidator<String> methodValidator;
    
    private final ParameterValidator<Map<String, Object>> headersValidator;
    
    private final ParameterValidator<Configuration> processValidator;
    
    public ConfigurationValidator() {
        this.urlValidator = new UrlParameterValidator();
        this.methodValidator = new MethodParameterValidator();
        this.headersValidator = new HeadersParameterValidator();
        this.processValidator = new ProcessValidator(this.urlValidator,
                this.methodValidator);
    }
    
    @Override
    public void validateImmediateValue(final Configuration parameter) {
        this.urlValidator.validate(parameter, URL);
        this.methodValidator.validate(parameter, HTTP_METHOD);
        this.headersValidator.validate(parameter, HTTP_HEADERS);
        
        final Integer maxRetries = parameter.getInt(MAX_RETRIES,
                DEFAULT_MAX_RETRIES_VALUE);
        
        if (maxRetries <= 0) {
            throw DataXException.asDataXException(MAX_RETRIES_INVALID_EXCEPTION,
                    "maxRetries parameter must be greater than 0");
        }
        final Integer batchSize = parameter.getInt(BATCH_SIZE,
                DEFAULT_BATCH_SIZE_VALUE);
        if (batchSize <= 0) {
            throw DataXException.asDataXException(BATCH_SIZE_INVALID_EXCEPTION,
                    "batchSize parameter must be greater than 0");
        }
        final Integer ratePerTask = parameter.getInt(RATE_PER_TASK);
        if (nonNull(ratePerTask) && ratePerTask < 0) {
            throw DataXException.asDataXException(
                    RATE_PER_TASK_INVALID_EXCEPTION,
                    "rate-per-task parameter must be greater than or equals 0");
        }
        final List<Field> fields = parameter.getListWithJson(FIELDS,
                Field.class);
        if (isEmpty(fields)) {
            throw DataXException.asDataXException(EMPTY_FIELD_EXCEPTION,
                    "fields parameter must not be empty");
        }
        
        final Set<String> names = Sets.newHashSet();
        fields.forEach(field -> {
            if (isBlank(field.getName())) {
                throw DataXException.asDataXException(FIELDS_INVALID_EXCEPTION,
                        "field name must not be empty or blank");
            }
            if (names.contains(field.getName())) {
                throw DataXException.asDataXException(FIELDS_INVALID_EXCEPTION,
                        String.format("field name %s duplicate",
                                field.getName()));
            } else {
                names.add(field.getName());
            }
        });
        
        this.processValidator.validate(parameter, PREPROCESS);
        this.processValidator.validate(parameter, POSTPROCESS);
        
    }
    
    @Override
    public void validate(final Configuration config, final String path) {
        validateImmediateValue(config);
    }
}
