/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import com.alibaba.datax.common.element.Record;
import groovy.lang.Tuple2;

import java.util.List;

/**
 * @author jerrywang
 *
 */
public interface GdbGraph extends AutoCloseable {
	List<Tuple2<Record, Exception>> add(List<Tuple2<Record, GdbElement>> records);

	@Override
	void close();
}
