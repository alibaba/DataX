package cn.sensorsdata.datax.plugin.util;

import cn.hutool.core.util.StrUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtil {


    private static Set<String> DATE_PATTERN = new HashSet<>();
    private static Map<String, String> DATE_FORMAT = new HashMap<>();
    private static Set<String> CUSTOMIZE_DATE_FORMAT = new LinkedHashSet<>();

    static {
        DATE_PATTERN.add("\\d{4}-\\d{2}-\\d{2}");
        DATE_FORMAT.put("\\d{4}-\\d{2}-\\d{2}", "yyyy-MM-dd");

        DATE_PATTERN.add("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
        DATE_FORMAT.put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}", "yyyy-MM-dd HH:mm:ss");

        DATE_PATTERN.add("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1,3}");
        DATE_FORMAT.put("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{1,3}", "yyyy-MM-dd HH:mm:ss.SSS");

        DATE_PATTERN.add("\\d{4}-\\d{2}");
        DATE_FORMAT.put("\\d{4}-\\d{2}", "yyyy-MM");

        DATE_PATTERN.add("\\d{6}");
        DATE_FORMAT.put("\\d{6}", "yyyyMM");

        DATE_PATTERN.add("\\d{8}");
        DATE_FORMAT.put("\\d{8}", "yyyyMMdd");

        DATE_PATTERN.add("\\d{14}");
        DATE_FORMAT.put("\\d{14}", "yyyyMMddHHmmss");

        DATE_PATTERN.add("\\d{17}");
        DATE_FORMAT.put("\\d{17}", "yyyyMMddHHmmssSSS");
    }

    public static Date str2Date(String dateStr, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            return sdf.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("时间格式不匹配,pattern:%s,dateStr:%s", pattern, dateStr));
        }
    }

    public static Date str2Date(String dateStr) {
        String format = null;
        for (String pattern : DATE_PATTERN) {
            if (dateStr.matches(pattern)) {
                format = DATE_FORMAT.get(pattern);
                break;
            }
        }
        if (StrUtil.isBlank(format)) {
            Date d = null;
            for (String f : CUSTOMIZE_DATE_FORMAT) {
                try {
                    d = str2Date(dateStr, f);
                    if (!Objects.isNull(d)) {
                        return d;
                    }
                } catch (Exception e) {
                    continue;
                }
            }
            throw new RuntimeException("未找到匹配的时间格式,dateStr" + dateStr);
        }
        return str2Date(dateStr, format);
    }

    public static Date str2DateCustomize(String dateStr) {
        Date d;
        for (String f : CUSTOMIZE_DATE_FORMAT) {
            try {
                d = str2Date(dateStr, f);
                if (!Objects.isNull(d)) {
                    return d;
                }
            } catch (Exception e) {
                continue;
            }
        }
        throw new RuntimeException("未找到匹配的时间格式,dateStr" + dateStr);
    }

    public static String date2Str(Date date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static String date2Str(Date date) {
        return date2Str(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * @param pattern 时间格式的正则表达式
     * @param format  时间格式
     */
    public static void registerPattern(String pattern, String format) {
        if (Objects.isNull(DATE_FORMAT.get(pattern))) {
            DATE_PATTERN.add(pattern);
            DATE_FORMAT.put(pattern, format);
        }
    }

    public static void registerFormat(String format) {
        if (!StrUtil.isBlank(format)) {
            CUSTOMIZE_DATE_FORMAT.add(format);
        }
    }

    public static boolean hasPattern(String dateStr) {
        for (String pattern : DATE_PATTERN) {
            if (dateStr.matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    public static String getPattern(String dateStr) {
        for (String pattern : DATE_PATTERN) {
            if (dateStr.matches(pattern)) {
                return DATE_FORMAT.get(pattern);
            }
        }
        return null;
    }

    public static boolean hasFormatCustomize(String format) {
        return CUSTOMIZE_DATE_FORMAT.contains(format);
    }

    public static boolean hasFormat(String format) {
        Collection<String> values = DATE_FORMAT.values();
        return values.contains(format) || CUSTOMIZE_DATE_FORMAT.contains(format);
    }

}
