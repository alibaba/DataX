package com.leehom.arch.datax.plugin.rdb2graph.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 类名: DateTimeUtils.java
 * 说明: 日期时间工具类
 *
 * @author   leehom 
 * @Date	 Apr 23, 2009 5:36:41 AM
 *
 * @see 	 
 */
public class DateTimeUtils {
	
	/**
	 * 默认数据模式
	 */
	public static final String defaultPatten = "yyyy-MM-dd HH:mm:ss";
	public static final String defaultPattenMillis = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String defaultDatePatten = "yyyyMMdd";
	public static final String DatePatten2 = "yyyy-MM-dd";
	// ISO8601 Date Type Pattern
	public static final String ISO8601Patten = "yyyy-MM-dd'T'HH:mm:ssZZ";
	public static final String ISO8601PattenNoZone = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String ISO8601PattenWithMillis = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
	
	
	/**
	 * 说明：Date to String
	 *
	 * @author hjli
	 * @Param @param date
	 * @Param @return
	 * @Return String
	 * 
	 */
	public static String DateToString(Date date) {
		return DateToString(date, defaultPatten);
	}
	
	public static String DateToString(Date date, String pattern) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
		return sdf.format(date);
	}
	
	/**
	 * @说明：String to Date
	 *
	 * @author leehong
	 * @param dateStr
	 * @return Date
	 * 
	 * @异常：
	 */
	public static Date StringToDate(String dateStr) {
		return StringToDate(dateStr, defaultPatten);
	}
	
	public static Date StringToDate(String dateStr, String pattern) {
		if(dateStr==null)
			return null;
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			return sdf.parse(dateStr);
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 获取当前日期
	 *
	 * @author hjli
	 * @Param @return
	 * @Return String
	 *
	 * TODO
	 * 
	 */
	public static String getCurrentDateTimeText(String pattern) {
		if ((pattern==null)||(pattern.equals(""))) {
			pattern = defaultPatten;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
		return sdf.format(Calendar.getInstance().getTime());
	}
	
	public static String getCurrentDateTimeText() {
		return getCurrentDateTimeText(defaultPatten);
	}
	 
	/**
	 * 说明：获取前一小时时间
	 *
	 * @author hjli
	 * @Param @param pattern
	 * @Param @return
	 * @Return String
	 *
	 * TODO
	 * 
	 */
	public static String getPreHourText(String pattern) {
		Calendar c = Calendar.getInstance();
		return getPreHourText(c.getTime(), pattern);
	}
	
	public static String getPreHourText(Date date, String pattern) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.HOUR_OF_DAY, -1);
		SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
		return sdf.format(c.getTime());
	}
	
	public static Date getPreHour(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.HOUR_OF_DAY, -1);
		return c.getTime();
	}
	
    /** 获取前n小时时间*/
	public static Date getPreHourN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.HOUR_OF_DAY, 0-n);
		return c.getTime();
	}
	
	public static Date getNextHour(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.HOUR_OF_DAY, 1);
		return c.getTime();
		
	}
	
	public static Date getNextHourN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.HOUR_OF_DAY, n);
		return c.getTime();
		
	}
	
	/**
	 * 说明：获取当前日前一日
	 *
	 * @author hjli
	 * @Param @param pattern
	 * @Param @return
	 * @Return String
	 *
	 * TODO
	 * 
	 */
	public static String getPreDayText(String pattern) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DATE, -1);
		SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
		return sdf.format(c.getTime());
	}
	
	public static Date getPreDay(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, -1);
		return c.getTime();
	}
	
	/** 获取前n天时间*/
	public static Date getPreDayN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, 0-n);
		return c.getTime();
	}
	
	public static Date getNextDay(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, 1);
		return c.getTime();
		
	}
	
	public static Date getNextDayN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, n);
		return c.getTime();
		
	}
	
	public static Date getPreWeek(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.DATE, -7);
		return c.getTime();
	}
	
	/**
	 * 说明：获取当前月前一月
	 *
	 * @author hjli
	 * @Param @param pattern
	 * @Param @return
	 * @Return String
	 * 
	 */
	public static String getPreMonthText(String pattern) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.MONTH, -1);    
		SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
		return sdf.format(c.getTime());
	}
	
	public static Date getPreMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.MONTH, -1);    
		return c.getTime();
	}
	
	/** 获取前n月时间*/
	public static Date getPreMonthN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.MONTH, 0-n);    
		return c.getTime();
	}
	
	public static Date getNextMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.MONTH, 1);
		return c.getTime();
		
	}
	
	public static Date getNextMonthN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.MONTH, n);
		return c.getTime();
		
	}
	
	// 获取前一年
	public static String getPreYearText(String pattern) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.YEAR, -1);    
		SimpleDateFormat sdf = new SimpleDateFormat(pattern); 
		return sdf.format(c.getTime());
	}
	
	public static Date getPreYear(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.YEAR, -1);    
		return c.getTime();
	}
	
	/** 获取前n年*/
	public static Date getPreYearN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.YEAR, 0-n);    
		return c.getTime();
	}
	
	/** 获取下一年*/
	public static Date getNextYear(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.YEAR, 1);
		return c.getTime();
		
	}
	
	public static Date getNextYearN(Date date, int n) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(Calendar.YEAR, n);
		return c.getTime();
		
	}
		
	// the right date type should be like "2012-12-15T00:00:00+08:00"
	public static void validateDateFormat(String dateString) throws Exception {
		Pattern pattern = Pattern.compile("\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}[\\+|\\-]\\d{2}:\\d{2}");
		Matcher matcher = pattern.matcher(dateString);
		boolean isValidated = matcher.matches();
		if (!isValidated) {
			throw new Exception("'" + dateString + "' is not a validate " +
					"date string. Please follow this sample:2012-12-15T00:00:00+08:00");
		}
	}
}
