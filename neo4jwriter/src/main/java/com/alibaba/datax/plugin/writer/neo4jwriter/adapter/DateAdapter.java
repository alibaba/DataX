package com.alibaba.datax.plugin.writer.neo4jwriter.adapter;


import com.alibaba.datax.plugin.writer.neo4jwriter.config.Neo4jProperty;
import org.testcontainers.shaded.com.google.common.base.Supplier;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * @author fuyouj
 */
public class DateAdapter {
    private static final ThreadLocal<DateTimeFormatter> LOCAL_DATE_FORMATTER_MAP = new ThreadLocal<>();
    private static final ThreadLocal<DateTimeFormatter> LOCAL_TIME_FORMATTER_MAP = new ThreadLocal<>();
    private static final ThreadLocal<DateTimeFormatter> LOCAL_DATE_TIME_FORMATTER_MAP = new ThreadLocal<>();
    private static final String DEFAULT_LOCAL_DATE_FORMATTER = "yyyy-MM-dd";
    private static final String DEFAULT_LOCAL_TIME_FORMATTER = "HH:mm:ss";
    private static final String DEFAULT_LOCAL_DATE_TIME_FORMATTER = "yyyy-MM-dd HH:mm:ss";


    public static LocalDate localDate(String text, Neo4jProperty neo4jProperty) {
        if (LOCAL_DATE_FORMATTER_MAP.get() != null) {
            return LocalDate.parse(text, LOCAL_DATE_FORMATTER_MAP.get());
        }

        String format = getOrDefault(neo4jProperty::getDateFormat, DEFAULT_LOCAL_DATE_FORMATTER);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        LOCAL_DATE_FORMATTER_MAP.set(dateTimeFormatter);
        return LocalDate.parse(text, dateTimeFormatter);
    }

    public static String getOrDefault(Supplier<String> dateFormat, String defaultFormat) {
        String format = dateFormat.get();
        if (null == format || "".equals(format)) {
            return defaultFormat;
        } else {
            return format;
        }
    }

    public static void destroy() {
        LOCAL_DATE_FORMATTER_MAP.remove();
        LOCAL_TIME_FORMATTER_MAP.remove();
        LOCAL_DATE_TIME_FORMATTER_MAP.remove();
    }

    public static LocalTime localTime(String text, Neo4jProperty neo4JProperty) {
        if (LOCAL_TIME_FORMATTER_MAP.get() != null) {
            return LocalTime.parse(text, LOCAL_TIME_FORMATTER_MAP.get());
        }

        String format = getOrDefault(neo4JProperty::getDateFormat, DEFAULT_LOCAL_TIME_FORMATTER);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        LOCAL_TIME_FORMATTER_MAP.set(dateTimeFormatter);
        return LocalTime.parse(text, dateTimeFormatter);
    }

    public static LocalDateTime localDateTime(String text, Neo4jProperty neo4JProperty) {
        if (LOCAL_DATE_TIME_FORMATTER_MAP.get() != null){
            return LocalDateTime.parse(text,LOCAL_DATE_TIME_FORMATTER_MAP.get());
        }
        String format = getOrDefault(neo4JProperty::getDateFormat, DEFAULT_LOCAL_DATE_TIME_FORMATTER);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        LOCAL_DATE_TIME_FORMATTER_MAP.set(dateTimeFormatter);
        return LocalDateTime.parse(text, dateTimeFormatter);
    }
}
