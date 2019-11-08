package com.alibaba.datax.plugin.reader.tsdbreader.util;

import java.util.concurrent.TimeUnit;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TimeUtils
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public final class TimeUtils {

    private TimeUtils() {
    }

    private static final long SECOND_MASK = 0xFFFFFFFF00000000L;
    private static final long HOUR_IN_MILL = TimeUnit.HOURS.toMillis(1);

    /**
     * Weather the timestamp is second.
     *
     * @param ts timestamp
     */
    public static boolean isSecond(long ts) {
        return (ts & SECOND_MASK) == 0;
    }

    /**
     * Get the hour.
     *
     * @param ms time in millisecond
     */
    public static long getTimeInHour(long ms) {
        return ms - ms % HOUR_IN_MILL;
    }
}
