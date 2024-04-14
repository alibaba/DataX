package com.alibaba.datax.plugin.writer.oceanbasev10writer.part;

import com.alibaba.datax.common.element.Record;

/**
 * @author cjyyz
 * @date 2023/02/07
 * @since
 */
public interface IObPartCalculator {

    /**
     * 计算 Partition Id
     *
     * @param record
     * @return Long
     */
    Long calculate(Record record);
}