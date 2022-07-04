package com.alibaba.datax.plugin.reader.util;

import java.util.concurrent.TimeUnit;

//This file is part of OpenTSDB.

//Copyright (C) 2010-2012  The OpenTSDB Authors.
//Copyright（C）2019 Alibaba Group Holding Ltd.

//

//This program is free software: you can redistribute it and/or modify it

//under the terms of the GNU Lesser General Public License as published by

//the Free Software Foundation, either version 2.1 of the License, or (at your

//option) any later version.  This program is distributed in the hope that it

//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty

//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser

//General Public License for more details.  You should have received a copy

//of the GNU Lesser General Public License along with this program.  If not,

//see <http://www.gnu.org/licenses/>.
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
