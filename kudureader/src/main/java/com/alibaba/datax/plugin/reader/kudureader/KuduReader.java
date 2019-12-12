package com.alibaba.datax.plugin.reader.kudureader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

/**
 * Created by roy on 2019/12/12 1543.
 */
public class KuduReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private String splitKey;

        private String lowerBound;

        private String upperBound;

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> confList = new ArrayList<Configuration>();

            if ((splitKey != null) && (!"min".equals(lowerBound)) && (!"max".equals(upperBound))) {
                int lowerBound = Integer.parseInt(this.lowerBound);
                int upperBound = Integer.parseInt(this.upperBound);
                int range = (upperBound - lowerBound) + 1;
                int limit = (int) Math.ceil((double) range / (double) adviceNumber);
                int offset;
                for (int page = 0; page < adviceNumber; ++page) {
                    offset = page * limit;
                    Configuration conf = originalConfig.clone();
                    int possibleLowerBound = (lowerBound + offset);
                    int possibleUpperBound = (lowerBound + offset + limit - 1);
                    if (possibleLowerBound > upperBound) {
                        possibleLowerBound = 0;
                        possibleUpperBound = 0;
                    } else {
                        possibleUpperBound = Math.min(possibleUpperBound, upperBound);
                    }
                    conf.set(KeyConstant.LOWER_BOUND, possibleLowerBound);
                    conf.set(KeyConstant.UPPER_BOUND, possibleUpperBound);
                    confList.add(conf);
                }
            } else {
                Configuration conf = originalConfig.clone();
                conf.set(KeyConstant.LOWER_BOUND, "min");
                conf.set(KeyConstant.UPPER_BOUND, "max");
                confList.add(conf);
            }

            return confList;
        }

        @Override
        public void init() {
            originalConfig = super.getPluginJobConf();
            splitKey = originalConfig.getString(KeyConstant.SPLIT_KEY);
            lowerBound = originalConfig.getString(KeyConstant.LOWER_BOUND);
            upperBound = originalConfig.getString(KeyConstant.UPPER_BOUND);
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private KuduClient kuduClient;

        private String tableName = null;

        private String splitKey;

        private String lowerBound;

        private String upperBound;

        @Override
        public void startRead(RecordSender recordSender) {
            KuduTable kuduTable = null;
            try {
                kuduTable = kuduClient.openTable(tableName);
            } catch (KuduException ex) {
                throw DataXException.asDataXException(
                        KuduReaderErrorCode.UNEXCEPT_EXCEPTION,
                        ex.getMessage()
                );
            }

            Schema schema = kuduTable.getSchema();

            KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(kuduTable);
            KuduScanner kuduScanner;

            if ((splitKey != null) && (!"min".equals(lowerBound)) && (!"max".equals(upperBound))) {
                KuduPredicate lowerBoundPredicate = KuduPredicate.newComparisonPredicate(
                        schema.getColumn(splitKey),
                        KuduPredicate.ComparisonOp.GREATER_EQUAL,
                        Integer.parseInt(lowerBound)
                );
                KuduPredicate upperBoundPredicate = KuduPredicate.newComparisonPredicate(
                        schema.getColumn(splitKey),
                        KuduPredicate.ComparisonOp.LESS_EQUAL,
                        Integer.parseInt(upperBound)
                );
                kuduScanner = kuduScannerBuilder
                        .addPredicate(lowerBoundPredicate)
                        .addPredicate(upperBoundPredicate)
                        .build();
            } else {
                kuduScanner = kuduScannerBuilder.build();
            }

            List<ColumnSchema> columnSchemas = schema.getColumns();

            while (kuduScanner.hasMoreRows()) {
                RowResultIterator rows;
                try {
                    rows = kuduScanner.nextRows();
                } catch (KuduException ex) {
                    throw DataXException.asDataXException(
                            KuduReaderErrorCode.UNEXCEPT_EXCEPTION,
                            ex.getMessage()
                    );
                }
                while (rows.hasNext()) {
                    RowResult result = rows.next();

                    Record record = recordSender.createRecord();

                    boolean isDirtyRecord = false;

                    for (ColumnSchema columnSchema : columnSchemas) {
                        if (result.isNull(columnSchema.getName())) {
                            record.addColumn(new StringColumn(null));
                            continue;
                        }

                        Type columnType = columnSchema.getType();
                        switch (columnType) {
                            case INT8:
                                record.addColumn(new LongColumn(Long.valueOf(result.getByte(columnSchema.getName()))));
                                break;
                            case INT16:
                                record.addColumn(new LongColumn(Long.valueOf(result.getShort(columnSchema.getName()))));
                                break;
                            case INT32:
                                record.addColumn(new LongColumn(Long.valueOf(result.getInt(columnSchema.getName()))));
                                break;
                            case INT64:
                                record.addColumn(new LongColumn(result.getLong(columnSchema.getName())));
                                break;
                            case BINARY:
                                record.addColumn(new BytesColumn(result.getString(columnSchema.getName()).getBytes(StandardCharsets.UTF_8)));
                                break;
                            case STRING:
                                record.addColumn(new StringColumn(result.getString(columnSchema.getName())));
                                break;
                            case BOOL:
                                record.addColumn(new BoolColumn(result.getBoolean(columnSchema.getName())));
                                break;
                            case FLOAT:
                                record.addColumn(new DoubleColumn(result.getFloat(columnSchema.getName())));
                                break;
                            case DOUBLE:
                                record.addColumn(new DoubleColumn(result.getDouble(columnSchema.getName())));
                                break;
                            case UNIXTIME_MICROS:
                                record.addColumn(new DateColumn(result.getTimestamp(columnSchema.getName())));
                                break;
                            default:
                                isDirtyRecord = true;
                                getTaskPluginCollector().collectDirtyRecord(
                                        record,
                                        "Invalid kudu data type: " + columnType.getName()
                                );
                                break;
                        }
                        if (isDirtyRecord) {
                            break;
                        }
                    }

                    if (!isDirtyRecord) {
                        recordSender.sendToWriter(record);
                    }
                }
            }

            try {
                kuduScanner.close();
            } catch (KuduException ex) {
                throw DataXException.asDataXException(
                        KuduReaderErrorCode.UNEXCEPT_EXCEPTION,
                        ex.getMessage()
                );
            }
        }

        @Override
        public void init() {
            Configuration readerSliceConfig = super.getPluginJobConf();
            String masterAddresses = readerSliceConfig.getString(KeyConstant.KUDU_MASTER_ADDRESSES);
            tableName = readerSliceConfig.getString(KeyConstant.KUDU_TABlE_NAME);
            kuduClient = (new KuduClient.KuduClientBuilder(masterAddresses)).build();
            lowerBound = readerSliceConfig.getString(KeyConstant.LOWER_BOUND);
            upperBound = readerSliceConfig.getString(KeyConstant.UPPER_BOUND);
            splitKey = readerSliceConfig.getString(KeyConstant.SPLIT_KEY);
        }

        @Override
        public void destroy() {
            try {
                kuduClient.close();
            } catch (KuduException ex) {
                throw DataXException.asDataXException(
                        KuduReaderErrorCode.UNEXCEPT_EXCEPTION,
                        ex.getMessage()
                );
            }
        }

    }
}
