package com.alibaba.datax.plugin.writer.restwriter.process;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.restwriter.conf.Operation;
import com.alibaba.datax.plugin.writer.restwriter.conf.Process;

import lombok.extern.slf4j.Slf4j;

import static com.alibaba.datax.plugin.writer.restwriter.Key.ADDITIONAL_CONCURRENT;
import static com.alibaba.datax.plugin.writer.restwriter.Key.ADDITIONAL_OPERATIONS;
import static java.util.Objects.nonNull;

/**
 * Created by zhangyongxiang on 2023/10/12 19:28
 **/
@Slf4j
public class ProcessFactory {
    
    private final Configuration configuration;
    
    public ProcessFactory(Configuration configuration) {
        this.configuration = configuration;
    }
    
    public Process createProcess(final ProcessCategory category) {
        final Process process = new Process(category);
        final Configuration conf = this.configuration
                .getConfiguration(category.getKey());
        log.info("job configuration key: {}, conf: {}", category, conf);
        if (nonNull(conf)) {
            process.setConcurrent(conf.getBool(ADDITIONAL_CONCURRENT, false));
            process.setOperations(conf.getListWithJson(ADDITIONAL_OPERATIONS,
                    Operation.class));
        }
        return process;
    }
}
