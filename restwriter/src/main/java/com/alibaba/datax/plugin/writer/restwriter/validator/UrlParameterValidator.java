package com.alibaba.datax.plugin.writer.restwriter.validator;

import java.util.regex.Pattern;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.URL_INVALID_EXCEPTION;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 18:03
 **/
@Slf4j
public class UrlParameterValidator implements ParameterValidator<String> {
    
    private static final String URL_EXP = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    
    private final Pattern urlPattern;
    
    public UrlParameterValidator() {
        this.urlPattern = Pattern.compile(URL_EXP);
    }
    
    @Override
    public void validate(final Configuration config, final String path) {
        validateImmediateValue(config.getString(path));
    }
    
    @Override
    public void validateImmediateValue(final String parameter) {
        if (isBlank(parameter)) {
            throw DataXException.asDataXException(URL_INVALID_EXCEPTION,
                    "需要填写url参数");
        }
        if (!urlPattern.matcher(parameter).find()) {
            throw DataXException.asDataXException(URL_INVALID_EXCEPTION,
                    "url参数值不合法");
        }
    }
}
