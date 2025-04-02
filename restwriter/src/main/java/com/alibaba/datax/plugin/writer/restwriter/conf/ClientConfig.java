package com.alibaba.datax.plugin.writer.restwriter.conf;

import lombok.Data;

import static kong.unirest.Config.DEFAULT_MAX_CONNECTIONS;
import static kong.unirest.Config.DEFAULT_MAX_PER_ROUTE;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/
@Data
public class ClientConfig {
    
    private int maxTotal = DEFAULT_MAX_CONNECTIONS;
    
    private int maxPerRoute = DEFAULT_MAX_PER_ROUTE;
    
}
