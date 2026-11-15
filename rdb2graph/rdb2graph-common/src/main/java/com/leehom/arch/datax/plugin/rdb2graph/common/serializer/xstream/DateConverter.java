/**
 * %序列化器%
 * %1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.common.serializer.xstream;

import java.sql.Timestamp;
import java.util.Date;

import com.leehom.arch.datax.plugin.rdb2graph.common.DateTimeUtils;
import com.thoughtworks.xstream.converters.SingleValueConverter;

/**
 * @类名: DateConverter
 * @说明: 日期转换器
 * 
 * @author leehom
 * @Date 2010-6-15 下午06:09:19 
 * 修改记录：
 * 
 * @see
 */
public class DateConverter implements SingleValueConverter {
	
	/** 日期模式*/
	private String pattern;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thoughtworks.xstream.converters.SingleValueConverter#fromString(java
	 * .lang.String)
	 */
	
	public Object fromString(String str) {
		return DateTimeUtils.StringToDate(str, pattern());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thoughtworks.xstream.converters.SingleValueConverter#toString(java
	 * .lang.Object)
	 */
	
	public String toString(Object obj) {
		return DateTimeUtils.DateToString((Date)obj, pattern());
			
	}
	
	private String pattern() {
		if(pattern==null)
			return DateTimeUtils.defaultPatten;
		else
			return pattern;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.thoughtworks.xstream.converters.ConverterMatcher#canConvert(java.
	 * lang.Class)
	 */
	
	public boolean canConvert(Class type) {
		return type.equals(Date.class) || type.equals(Timestamp.class);
	}

	/**
	 * @return the pattern
	 */
	public String getPattern() {
		return pattern;
	}

	/**
	 * @param pattern the pattern to set
	 */
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

}
