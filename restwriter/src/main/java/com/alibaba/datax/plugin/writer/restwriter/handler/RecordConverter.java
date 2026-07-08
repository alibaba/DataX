package com.alibaba.datax.plugin.writer.restwriter.handler;

import com.alibaba.datax.common.element.Record;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 14:24
 **/
@FunctionalInterface
public interface RecordConverter<T> {
    
    T convert(Record record);
    
}
