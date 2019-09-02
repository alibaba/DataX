/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.alibaba.datax.common.element.Column;
import lombok.extern.slf4j.Slf4j;

/**
 * @author jerrywang
 *
 */
@Slf4j
public enum ValueType {
	INT(Integer.class, "int", Column::asLong, Integer::valueOf),
	LONG(Long.class, "long", Column::asLong, Long::valueOf),
	DOUBLE(Double.class, "double", Column::asDouble, Double::valueOf),
	FLOAT(Float.class, "float", Column::asDouble, Float::valueOf),
	BOOLEAN(Boolean.class, "boolean", Column::asBoolean, Boolean::valueOf),
	STRING(String.class, "string", Column::asString, String::valueOf);

	private Class<?> type = null;
	private String shortName = null;
	private Function<Column, Object> columnFunc = null;
	private Function<String, Object> fromStrFunc = null;

	private ValueType(Class<?> type, String name, Function<Column, Object> columnFunc, Function<String, Object> fromStrFunc) {
		this.type = type;
		this.shortName = name;
		this.columnFunc = columnFunc;
		this.fromStrFunc = fromStrFunc;
		
		ValueTypeHolder.shortName2type.put(name, this);
	}
	
	public static ValueType fromShortName(String name) {
		return ValueTypeHolder.shortName2type.get(name);
	}

	public Class<?> type() {
		return this.type;
	}
	
	public String shortName() {
		return this.shortName;
	}
	
	public Object applyColumn(Column column) {
		try {
			if (column == null) {
				return null;
			}
			return columnFunc.apply(column);
		} catch (Exception e) {
			log.error("applyColumn error {}, column {}", e.toString(), column);
			throw e;
		}
	}
	
	public Object fromStrFunc(String str) {
		return fromStrFunc.apply(str);
	}

	private static class ValueTypeHolder {
		private static Map<String, ValueType> shortName2type = new HashMap<>();
	}
}
