package com.alibaba.datax.plugin.writer.restwriter.process;

import com.alibaba.datax.plugin.writer.restwriter.Key;

import lombok.Getter;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/

@Getter
public enum ProcessCategory {
    /**
     * pre processing
     */
    PREPROCESS(Key.PREPROCESS),
    /**
     * post processing
     */
    POSTPROCESS(Key.POSTPROCESS);
    
    private String key;
    
    ProcessCategory(final String key) {
        this.key = key;
    }
    
}
