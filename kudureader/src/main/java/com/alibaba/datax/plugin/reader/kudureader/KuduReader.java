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
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

/**
 * Created by roy on 2019/12/12 1543.
 */
public class KuduReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> confList = new ArrayList<Configuration>();
            Configuration conf = originalConfig.clone();
            conf.set(KeyConstant.LOWER_BOUND, "min");
            conf.set(KeyConstant.UPPER_BOUND, "max");
            conf.set(KeyConstant.IS_OBJECTID, false);
            confList.add(conf);

            return confList;
        }

        @Override
        public void init() {
            originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private KuduClient kuduClient;

        private String tableName = null;

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
            KuduScanner kuduScanner = kuduClient.newScannerBuilder(kuduTable).build();
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();

            while (kuduScanner.hasMoreRows()) {
                RowResultIterator rows = null;
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
                        }
                    }

                    recordSender.sendToWriter(record);
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
