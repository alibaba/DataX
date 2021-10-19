/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import java.util.function.Function;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbElement;

/**
 * @author jerrywang
 *
 */
public interface GdbMapper {
    Function<Record, GdbElement> getMapper(MappingRule rule);
}
