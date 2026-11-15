package com.alibaba.datax.plugin.writer.restwriter.conf;

import java.util.Map;

import com.google.common.collect.Maps;

import lombok.Data;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/
@Data
public class Operation {
    
    private String url;
    
    private String method;
    
    private Map<String, String> headers = Maps.newHashMap();
    
    private String body;
    
    private boolean base64;
    
    private boolean debug;
    
    private int maxRetries = 1;
    
    private String jsonExpression;
    
}
