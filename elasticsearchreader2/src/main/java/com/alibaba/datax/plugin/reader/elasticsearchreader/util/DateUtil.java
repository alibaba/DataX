/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.reader.elasticsearchreader.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Date Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class DateUtil {

    private static final String TIME_ZONE = "GMT+8";

    private static final String STANDARD_DATETIME_FORMAT = "standardDatetimeFormatter";

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

            return formatterMap;
    });

    private DateUtil() {}

    public static java.sql.Date columnToDate(Object column,SimpleDateFormat customTimeFormat) {
        if(column == null) {
            return null;
        } else if(column instanceof String) {
            if (((String) column).length() == 0){
                return null;
            }
            return new java.sql.Date(stringToDate((String)column,customTimeFormat).getTime());
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
            return new Timestamp(stringToDate((String)column,customTimeFormat).getTime());
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
}
