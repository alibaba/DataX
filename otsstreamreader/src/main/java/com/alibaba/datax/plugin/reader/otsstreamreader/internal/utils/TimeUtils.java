package com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtils {

    public static final long SECOND_IN_MILLIS = 1000;

    public static final long MINUTE_IN_MILLIS = 60 * 1000;

    public static final int DAY_IN_SEC = 24 * 60 * 60;

    public static final long DAY_IN_MILLIS = DAY_IN_SEC * 1000;

    public static final long HOUR_IN_MILLIS = 60 * MINUTE_IN_MILLIS;

    private static final Logger LOG = LoggerFactory.getLogger(TimeUtils.class);

    public static long sleepMillis(long timeToSleepMillis) {
        if(timeToSleepMillis <= 0L) {
            return 0L;
        } else {
            long startTime = System.currentTimeMillis();

            try {
                Thread.sleep(timeToSleepMillis);
            } catch (InterruptedException var5) {
                Thread.interrupted();
                LOG.warn("Interrupted while sleeping");
            }

            return System.currentTimeMillis() - startTime;
        }
    }

    public static long parseDateToTimestampMillis(String dateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = format.parse(dateStr);
        return date.getTime();
    }

    public static long parseTimeStringToTimestampMillis(String dateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = format.parse(dateStr);
        return date.getTime();
    }


    public static String getTimeInISO8601(Date date) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        df.setTimeZone(tz);
        String nowAsISO = df.format(date);
        return nowAsISO;
    }
}
