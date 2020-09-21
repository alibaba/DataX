package com.q1.datax.plugin.writer.kudu11xwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author daizihao
 * @create 2020-08-31 16:55
 **/
public class KuduWriterTask {
    private final static Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);

    public List<Configuration> columns;
    public String encoding;
    public String insertMode;
    public Double batchSize;
    public long mutationBufferSpace;
    public Boolean isUpsert;
    public Boolean isSkipFail;

    public KuduClient kuduClient;
    public KuduTable table;
    public KuduSession session;
    private Integer primaryKeyIndexUntil;


    public KuduWriterTask(com.alibaba.datax.common.util.Configuration configuration) {
        this.columns = configuration.getListConfiguration(Key.COLUMN);
        this.encoding = configuration.getString(Key.ENCODING);
        this.insertMode = configuration.getString(Key.INSERT_MODE);
        this.batchSize = configuration.getDouble(Key.WRITE_BATCH_SIZE);
        this.mutationBufferSpace = configuration.getLong(Key.MUTATION_BUFFER_SPACE);
        this.isUpsert = !configuration.getString(Key.INSERT_MODE).equals("insert");

        this.kuduClient = Kudu11xHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
        this.table = Kudu11xHelper.getKuduTable(configuration, kuduClient);
        this.session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace((int) mutationBufferSpace);
        this.primaryKeyIndexUntil = Kudu11xHelper.getPrimaryKeyIndexUntil(columns);
//        tableName = configuration.getString(Key.TABLE);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector) {
        Record record;
        AtomicLong counter = new AtomicLong(0L);
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != columns.size()) {
                    throw DataXException.asDataXException(Kudu11xWriterErrorcode.PARAMETER_NUM_ERROR, " number of record fields:" + record.getColumnNumber() + " number of configuration fields:" + columns.size());
                }
                boolean isDirtyRecord = false;


                for (int i = 0; i <= primaryKeyIndexUntil && !isDirtyRecord; i++) {
                    Column column = record.getColumn(i);
                    isDirtyRecord = StringUtils.isBlank(column.asString());
                }

                if (isDirtyRecord) {
                    taskPluginCollector.collectDirtyRecord(record, "primarykey field is null");
                    continue;
                }

                Upsert upsert = table.newUpsert();
                Insert insert = table.newInsert();

                for (int i = 0; i < columns.size(); i++) {
                    PartialRow row;
                    if (isUpsert) {
                        //覆盖更新
                        row = upsert.getRow();
                    } else {
                        //增量更新
                        row = insert.getRow();
                    }
                    Configuration col = columns.get(i);
                    String name = col.getString(Key.NAME);
                    ColumnType type = ColumnType.getByTypeName(col.getString(Key.TYPE));
                    Column column = record.getColumn(col.getInt(Key.INDEX, i));
                    Object rawData = column.getRawData();
                    if (rawData == null) {
                        row.setNull(name);
                        continue;
                    }
                    switch (type) {
                        case INT:
                            row.addInt(name, Integer.parseInt(rawData.toString()));
                            break;
                        case LONG:
                        case BIGINT:
                            row.addLong(name, Long.parseLong(rawData.toString()));
                            break;
                        case FLOAT:
                            row.addFloat(name, Float.parseFloat(rawData.toString()));
                            break;
                        case DOUBLE:
                            row.addDouble(name, Double.parseDouble(rawData.toString()));
                            break;
                        case BOOLEAN:
                            row.addBoolean(name, Boolean.getBoolean(rawData.toString()));
                            break;
                        case STRING:
                        default:
                            row.addString(name, rawData.toString());
                    }
                }
                try {
                    RetryUtil.executeWithRetry(()->{
                        if (isUpsert) {
                            //覆盖更新
                            session.apply(upsert);
                        } else {
                            //增量更新
                            session.apply(insert);
                        }
                        //提前写数据，阈值可自定义
                        if (counter.incrementAndGet() > batchSize * 0.75) {
                            session.flush();
                            counter.set(0L);
                        }
                        return true;
                    },5,1000L,true);

                } catch (Exception e) {
                    LOG.error("Data write failed!", e);
                    if (isSkipFail) {
                        LOG.warn("Because you have configured skipFail is true,this data will be skipped!");
                        taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                    }else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(Kudu11xWriterErrorcode.PUT_KUDU_ERROR, e);
        }
        AtomicInteger i = new AtomicInteger(10);
        try {
            while (i.get() > 0) {
                if (session.hasPendingOperations()) {
                    session.flush();
                    break;
                }
                Thread.sleep(1000L);
                i.decrementAndGet();
            }
        } catch (Exception e) {
            LOG.info("Waiting for data to be inserted...... " + i + "s");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            i.decrementAndGet();
        } finally {
            try {
                session.flush();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

    }


}
