package com.alibaba.datax.plugin.reader.conn;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import net.opentsdb.core.*;
import net.opentsdb.core.Internal.Cell;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Tool to dump the data straight from HBase
 *
 * @author Benedict Jin
 * @since 2019-04-17
 */
final class DumpSeries {

    private static final Logger LOG = LoggerFactory.getLogger(DumpSeries.class);

    /**
     * Dump all data points with special metric and time range, then send them all by {@link RecordSender}.
     */
    static void doDump(TSDB tsdb, String[] args, RecordSender sender) throws Exception {
        final ArrayList<Query> queries = new ArrayList<Query>();
        CliQuery.parseCommandLineQuery(args, tsdb, queries);

        List<DataPoint4TSDB> dps = new LinkedList<DataPoint4TSDB>();
        for (final Query query : queries) {
            final List<Scanner> scanners = Internal.getScanners(query);
            for (Scanner scanner : scanners) {
                ArrayList<ArrayList<KeyValue>> rows;
                while ((rows = scanner.nextRows().join()) != null) {
                    for (final ArrayList<KeyValue> row : rows) {
                        final byte[] key = row.get(0).key();
                        final long baseTime = Internal.baseTime(tsdb, key);
                        final String metric = Internal.metricName(tsdb, key);
                        for (final KeyValue kv : row) {
                            formatKeyValue(dps, tsdb, kv, baseTime, metric);
                            for (DataPoint4TSDB dp : dps) {
                                StringColumn tsdbColumn = new StringColumn(dp.toString());
                                Record record = sender.createRecord();
                                record.addColumn(tsdbColumn);
                                sender.sendToWriter(record);
                            }
                            dps.clear();
                        }
                    }
                }
            }
        }
    }

    /**
     * Parse KeyValue into data points.
     */
    private static void formatKeyValue(final List<DataPoint4TSDB> dps, final TSDB tsdb,
                                       final KeyValue kv, final long baseTime, final String metric) {
        Map<String, String> tagKVs = Internal.getTags(tsdb, kv.key());

        final byte[] qualifier = kv.qualifier();
        final int q_len = qualifier.length;

        if (!AppendDataPoints.isAppendDataPoints(qualifier) && q_len % 2 != 0) {
            // custom data object, not a data point
            if (LOG.isDebugEnabled()) {
                LOG.debug("Not a data point");
            }
        } else if (q_len == 2 || q_len == 4 && Internal.inMilliseconds(qualifier)) {
            // regular data point
            final Cell cell = Internal.parseSingleValue(kv);
            if (cell == null) {
                throw new IllegalDataException("Unable to parse row: " + kv);
            }
            dps.add(new DataPoint4TSDB(cell.absoluteTimestamp(baseTime), metric, tagKVs, cell.parseValue()));
        } else {
            final Collection<Cell> cells;
            if (q_len == 3) {
                // append data points
                cells = new AppendDataPoints().parseKeyValue(tsdb, kv);
            } else {
                // compacted column
                cells = Internal.extractDataPoints(kv);
            }
            for (Cell cell : cells) {
                dps.add(new DataPoint4TSDB(cell.absoluteTimestamp(baseTime), metric, tagKVs, cell.parseValue()));
            }
        }
    }
}
