package com.alibaba.datax.plugin.s3common.util;

import cn.hutool.core.util.StrUtil;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.parquet.io.api.Binary;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/5/19 11:53
 */
public class DateUtil {

    private static final String TIME_ZONE = "GMT+8";

    private static final String STANDARD_DATETIME_FORMAT = "standardDatetimeFormatter";

    private static final String STANDARD_DATETIME_FORMAT_FOR_MILLISECOND= "standardDatetimeFormatterForMillisecond";

    private static final String UN_STANDARD_DATETIME_FORMAT = "unStandardDatetimeFormatter";

    private static final String DATE_FORMAT = "dateFormatter";

    private static final String TIME_FORMAT = "timeFormatter";

    private static final String YEAR_FORMAT = "yearFormatter";

    private static final String START_TIME = "1970-01-01";

    public final static String DATE_REGEX = "(?i)date";

    public final static String TIMESTAMP_REGEX = "(?i)timestamp";

    public final static String DATETIME_REGEX = "(?i)datetime";

    public final static int LENGTH_SECOND = 10;
    public final static int LENGTH_MILLISECOND = 13;
    public final static int LENGTH_MICROSECOND = 16;
    public final static int LENGTH_NANOSECOND = 19;

    public static final int TIMESTAMP_BINARY_LENGTH = 12;
    public static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    public static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    public static ThreadLocal<Map<String,SimpleDateFormat>> datetimeFormatter = ThreadLocal.withInitial(() -> {
            TimeZone timeZone = TimeZone.getTimeZone(TIME_ZONE);

            Map<String, SimpleDateFormat> formatterMap = new HashMap<>();
            SimpleDateFormat standardDatetimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            standardDatetimeFormatter.setTimeZone(timeZone);
            formatterMap.put(STANDARD_DATETIME_FORMAT,standardDatetimeFormatter);

            SimpleDateFormat unStandardDatetimeFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
            unStandardDatetimeFormatter.setTimeZone(timeZone);
            formatterMap.put(UN_STANDARD_DATETIME_FORMAT,unStandardDatetimeFormatter);

            SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
            dateFormatter.setTimeZone(timeZone);
            formatterMap.put(DATE_FORMAT,dateFormatter);

            SimpleDateFormat timeFormatter = new SimpleDateFormat("HH:mm:ss");
            timeFormatter.setTimeZone(timeZone);
            formatterMap.put(TIME_FORMAT,timeFormatter);

            SimpleDateFormat yearFormatter = new SimpleDateFormat("yyyy");
            yearFormatter.setTimeZone(timeZone);
            formatterMap.put(YEAR_FORMAT,yearFormatter);

            SimpleDateFormat standardDatetimeFormatterOfMillisecond = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            standardDatetimeFormatterOfMillisecond.setTimeZone(timeZone);
            formatterMap.put(STANDARD_DATETIME_FORMAT_FOR_MILLISECOND,standardDatetimeFormatterOfMillisecond);

        return formatterMap;
    });

    private DateUtil() {}

    /**
     * @param timestampBinary
     * @return
     */
    public static long getTimestampMillis(Binary timestampBinary) {
        if (timestampBinary.length() != TIMESTAMP_BINARY_LENGTH) {
            return 0;
        }

        byte[] bytes = timestampBinary.getBytes();

        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    public static long julianDayToMillis(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    public static  SimpleDateFormat getSimpleDateFormat(String format) {
        SimpleDateFormat customTimeFormat = null;
        if (format != null)
            customTimeFormat = new SimpleDateFormat(format);
        return customTimeFormat;
    }

    public static java.sql.Date columnToDate(Object column,SimpleDateFormat customTimeFormat) {
        if(column == null) {
            return null;
        } else if(column instanceof String) {
            if (((String) column).length() == 0){
                return null;
            }

            Date date = stringToDate((String)column, customTimeFormat);
            if (null == date) {
                return null;
            }
            return new java.sql.Date(date.getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Date(getMillSecond(rawData.toString()));
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Date(getMillSecond(rawData.toString()));
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Date) column;
        } else if(column instanceof Timestamp) {
            Timestamp ts = (Timestamp) column;
            return new java.sql.Date(ts.getTime());
        } else if(column instanceof Date) {
            Date d = (Date)column;
            return new java.sql.Date(d.getTime());
        }

        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static Timestamp columnToTimestamp(Object column,SimpleDateFormat customTimeFormat) {
        if (column == null) {
            return null;
        } else if(column instanceof String) {
            if (((String) column).length() == 0){
                return null;
            }

            Date date = stringToDate((String)column,customTimeFormat);
            if (null == date) {
                return null;
            }
            return new Timestamp(date.getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new Timestamp(getMillSecond(rawData.toString()));
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new Timestamp(getMillSecond(rawData.toString()));
        } else if (column instanceof java.sql.Date) {
            return new Timestamp(((java.sql.Date) column).getTime());
        } else if(column instanceof Timestamp) {
            return (Timestamp) column;
        } else if(column instanceof Date) {
            Date d = (Date)column;
            return new Timestamp(d.getTime());
        }

        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static long getMillSecond(String data){
        long time  = Long.parseLong(data);
        if(data.length() == LENGTH_SECOND){
            time = Long.parseLong(data) * 1000;
        } else if(data.length() == LENGTH_MILLISECOND){
            time = Long.parseLong(data);
        } else if(data.length() == LENGTH_MICROSECOND){
            time = Long.parseLong(data) / 1000;
        } else if(data.length() == LENGTH_NANOSECOND){
            time = Long.parseLong(data) / 1000000 ;
        } else if(data.length() < LENGTH_SECOND){
            try {
                long day = Long.parseLong(data);
                Date date = datetimeFormatter.get().get(DATE_FORMAT).parse(START_TIME);
                Calendar cal = Calendar.getInstance();
                long addMill = date.getTime() + day * 24 * 3600 * 1000;
                cal.setTimeInMillis(addMill);
                time = cal.getTimeInMillis();
            } catch (Exception ignore){
            }
        }
        return time;
    }

    public static Date stringToDate(String strDate,SimpleDateFormat customTimeFormat)  {
        if(strDate == null || strDate.trim().length() == 0) {
            return null;
        }

        if(customTimeFormat != null){
            try {
                return customTimeFormat.parse(strDate);
            } catch (ParseException ignored) {
            }
        }

        try {
            return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(UN_STANDARD_DATETIME_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(DATE_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(TIME_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return datetimeFormatter.get().get(YEAR_FORMAT).parse(strDate);
        } catch (ParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }

    public static String dateToString(Date date) {
        return datetimeFormatter.get().get(DATE_FORMAT).format(date);
    }

    public static String timestampToString(Date date) {
        return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT).format(date);
    }

    public static String dateToYearString(Date date) {
        return datetimeFormatter.get().get(YEAR_FORMAT).format(date);
    }

    public static SimpleDateFormat getDateTimeFormatter(){
        return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT);
    }

    //获取毫秒级别的日期解析
    public static SimpleDateFormat getDateTimeFormatterForMillisencond(){
        return datetimeFormatter.get().get(STANDARD_DATETIME_FORMAT_FOR_MILLISECOND);
    }

    public static SimpleDateFormat getDateFormatter(){
        return datetimeFormatter.get().get(DATE_FORMAT);
    }

    public static SimpleDateFormat getTimeFormatter(){
        return datetimeFormatter.get().get(TIME_FORMAT);
    }

    public static SimpleDateFormat getYearFormatter(){
        return datetimeFormatter.get().get(YEAR_FORMAT);
    }

    public static SimpleDateFormat buildDateFormatter(String timeFormat){
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        sdf.setTimeZone(TimeZone.getTimeZone(TIME_ZONE));
        return sdf;
    }

    /**
     * 常规自动日期格式识别
     * @param str 时间字符串
     * @return String DateFormat字符串如：yyyy-MM-dd HH:mm:ss
     */
    public static String getDateFormat(String str) {
        if(StrUtil.isBlank(str)){
            return null;
        }
        boolean year = false;
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        if(pattern.matcher(str.substring(0, 4)).matches()) {
            year = true;
        }
        StringBuilder sb = new StringBuilder();
        int index = 0;
        if(!year) {
            if(str.contains("月") || str.contains("-") || str.contains("/")) {
                if(Character.isDigit(str.charAt(0))) {
                    index = 1;
                }
            }else {
                index = 3;
            }
        }
        for (int i = 0; i < str.length(); i++) {
            char chr = str.charAt(i);
            if(Character.isDigit(chr)) {
                if(index==0) {
                    sb.append("y");
                }
                if(index==1) {
                    sb.append("M");
                }
                if(index==2) {
                    sb.append("d");
                }
                if(index==3) {
                    sb.append("H");
                }
                if(index==4) {
                    sb.append("m");
                }
                if(index==5) {
                    sb.append("s");
                }
                if(index==6) {
                    sb.append("S");
                }
            }else {
                if(i>0) {
                    char lastChar = str.charAt(i-1);
                    if(Character.isDigit(lastChar)) {
                        index++;
                    }
                }
                sb.append(chr);
            }
        }
        return sb.toString();
    }

}
