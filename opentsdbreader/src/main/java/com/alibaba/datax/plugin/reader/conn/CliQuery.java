package com.alibaba.datax.plugin.reader.conn;

import net.opentsdb.core.*;
import net.opentsdb.utils.DateTime;

import java.util.ArrayList;
import java.util.HashMap;

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
final class CliQuery {

    /**
     * Parses the query from the command lines.
     *
     * @param args    The command line arguments.
     * @param tsdb    The TSDB to use.
     * @param queries The list in which {@link Query}s will be appended.
     */
    static void parseCommandLineQuery(final String[] args,
                                      final TSDB tsdb,
                                      final ArrayList<Query> queries) {
        long start_ts = DateTime.parseDateTimeString(args[0], null);
        if (start_ts >= 0) {
            start_ts /= 1000;
        }
        long end_ts = -1;
        if (args.length > 3) {
            // see if we can detect an end time
            try {
                if (args[1].charAt(0) != '+' && (args[1].indexOf(':') >= 0
                        || args[1].indexOf('/') >= 0 || args[1].indexOf('-') >= 0
                        || Long.parseLong(args[1]) > 0)) {
                    end_ts = DateTime.parseDateTimeString(args[1], null);
                }
            } catch (NumberFormatException ignore) {
                // ignore it as it means the third parameter is likely the aggregator
            }
        }
        // temp fixup to seconds from ms until the rest of TSDB supports ms
        // Note you can't append this to the DateTime.parseDateTimeString() call as
        // it clobbers -1 results
        if (end_ts >= 0) {
            end_ts /= 1000;
        }

        int i = end_ts < 0 ? 1 : 2;
        while (i < args.length && args[i].charAt(0) == '+') {
            i++;
        }

        while (i < args.length) {
            final Aggregator agg = Aggregators.get(args[i++]);
            final boolean rate = "rate".equals(args[i]);
            RateOptions rate_options = new RateOptions(false, Long.MAX_VALUE,
                    RateOptions.DEFAULT_RESET_VALUE);
            if (rate) {
                i++;

                long counterMax = Long.MAX_VALUE;
                long resetValue = RateOptions.DEFAULT_RESET_VALUE;
                if (args[i].startsWith("counter")) {
                    String[] parts = Tags.splitString(args[i], ',');
                    if (parts.length >= 2 && parts[1].length() > 0) {
                        counterMax = Long.parseLong(parts[1]);
                    }
                    if (parts.length >= 3 && parts[2].length() > 0) {
                        resetValue = Long.parseLong(parts[2]);
                    }
                    rate_options = new RateOptions(true, counterMax, resetValue);
                    i++;
                }
            }
            final boolean downsample = "downsample".equals(args[i]);
            if (downsample) {
                i++;
            }
            final long interval = downsample ? Long.parseLong(args[i++]) : 0;
            final Aggregator sampler = downsample ? Aggregators.get(args[i++]) : null;
            final String metric = args[i++];
            final HashMap<String, String> tags = new HashMap<String, String>();
            while (i < args.length && args[i].indexOf(' ', 1) < 0
                    && args[i].indexOf('=', 1) > 0) {
                Tags.parse(tags, args[i++]);
            }
            final Query query = tsdb.newQuery();
            query.setStartTime(start_ts);
            if (end_ts > 0) {
                query.setEndTime(end_ts);
            }
            query.setTimeSeries(metric, tags, agg, rate, rate_options);
            if (downsample) {
                query.downsample(interval, sampler);
            }
            queries.add(query);
        }
    }
}
