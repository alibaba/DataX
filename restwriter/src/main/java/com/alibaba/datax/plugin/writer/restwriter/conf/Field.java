package com.alibaba.datax.plugin.writer.restwriter.conf;

import lombok.Data;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:58
 **/
@Data
public class Field {
    
    private String name;
    
    private String type;
    
    private String format;
}
