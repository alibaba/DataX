package com.alibaba.datax.plugin.writer.restwriter.conf;

import java.util.List;

import com.alibaba.datax.plugin.writer.restwriter.process.ProcessCategory;
import com.google.common.collect.Lists;

import lombok.Data;

import static com.alibaba.datax.plugin.writer.restwriter.process.ProcessCategory.PREPROCESS;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/

@Data
public class Process {
    
    private boolean concurrent;
    
    private ProcessCategory category = PREPROCESS;
    
    private List<Operation> operations = Lists.newArrayList();
    
    public Process(final ProcessCategory category) {
        this.category = category;
    }
}
