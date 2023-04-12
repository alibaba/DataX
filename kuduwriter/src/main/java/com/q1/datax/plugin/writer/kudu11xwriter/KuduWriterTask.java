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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author daizihao
 * @create 2020-08-31 16:55
 **/
public class KuduWriterTask {
    private final static Logger LOG = LoggerFactory.getLogger(KuduWriterTask.class);

    private List<Configuration> columns;
    private List<List<Configuration>> columnLists;
    private ThreadPoolExecutor pool;
    private String encoding;
    private Double batchSize;
    private Boolean isUpsert;
    private Boolean isSkipFail;
    public KuduClient kuduClient;
    public KuduSession session;
    private KuduTable table;
    private Integer primaryKeyIndexUntil;

    private final Object lock = new Object();

    public KuduWriterTask(Configuration configuration) {
        columns = configuration.getListConfiguration(Key.COLUMN);
        columnLists = Kudu11xHelper.getColumnLists(columns);
        pool = Kudu11xHelper.createRowAddThreadPool(columnLists.size());

        this.encoding = configuration.getString(Key.ENCODING);
        this.batchSize = configuration.getDouble(Key.WRITE_BATCH_SIZE);
        this.isUpsert = !configuration.getString(Key.INSERT_MODE).equalsIgnoreCase("insert");
        this.isSkipFail = configuration.getBool(Key.SKIP_FAIL);
        long mutationBufferSpace = configuration.getLong(Key.MUTATION_BUFFER_SPACE);

        this.kuduClient = Kudu11xHelper.getKuduClient(configuration.getString(Key.KUDU_CONFIG));
        this.table = Kudu11xHelper.getKuduTable(configuration, kuduClient);
        this.session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace((int) mutationBufferSpace);
        this.primaryKeyIndexUntil = Kudu11xHelper.getPrimaryKeyIndexUntil(columns);
//        tableName = configuration.getString(Key.TABLE);
    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector) {
        LOG.info("kuduwriter began to write!");
        Record record;
        LongAdder counter = new LongAdder();
        try {
            while ((record = lineReceiver.getFromReader()) != null) {
                if (record.getColumnNumber() != columns.size()) {
                    throw DataXException.asDataXException(Kudu11xWriterErrorcode.PARAMETER_NUM_ERROR, " number of record fields:" + record.getColumnNumber() + " number of configuration fields:" + columns.size());
                }
                boolean isDirtyRecord = false;


                for (int i = 0; i < primaryKeyIndexUntil && !isDirtyRecord; i++) {
                    Column column = record.getColumn(i);
                    isDirtyRecord = StringUtils.isBlank(column.asString());
                }

                if (isDirtyRecord) {
                    taskPluginCollector.collectDirtyRecord(record, "primarykey field is null");
                    continue;
                }

                CountDownLatch countDownLatch = new CountDownLatch(columnLists.size());
                Upsert upsert = table.newUpsert();
                Insert insert = table.newInsert();
                PartialRow row;
                if (isUpsert) {
                    //覆盖更新
                    row = upsert.getRow();
                } else {
                    //增量更新
                    row = insert.getRow();
                }
                List<Future<?>> futures = new ArrayList<>();
                for (List<Configuration> columnList : columnLists) {
                    Record finalRecord = record;
                    Future<?> future = pool.submit(() -> {
                        try {
                            for (Configuration col : columnList) {
                                String name = col.getString(Key.NAME);
                                ColumnType type = ColumnType.getByTypeName(col.getString(Key.TYPE, "string"));
                                Column column = finalRecord.getColumn(col.getInt(Key.INDEX));
                                String rawData = column.asString();
                                if (rawData == null) {
                                    synchronized (lock) {
                                        row.setNull(name);
                                    }
                                    continue;
                                }
                                switch (type) {
                                    case INT:
                                        synchronized (lock) {
                                            row.addInt(name, Integer.parseInt(rawData));
                                        }
                                        break;
                                    case LONG:
                                    case BIGINT:
                                        synchronized (lock) {
                                            row.addLong(name, Long.parseLong(rawData));
                                        }
                                        break;
                                    case FLOAT:
                                        synchronized (lock) {
                                            row.addFloat(name, Float.parseFloat(rawData));
                                        }
                                        break;
                                    case DOUBLE:
                                        synchronized (lock) {
                                            row.addDouble(name, Double.parseDouble(rawData));
                                        }
                                        break;
                                    case BOOLEAN:
                                        synchronized (lock) {
                                            row.addBoolean(name, Boolean.parseBoolean(rawData));
                                        }
                                        break;
                                    case STRING:
                                    default:
                                        synchronized (lock) {
                                            row.addString(name, rawData);
                                        }
                                }
                            }
                        } finally {
                            countDownLatch.countDown();
                        }
                    });
                    futures.add(future);
                }
                countDownLatch.await();
                for (Future<?> future : futures) {
                    future.get();
                }
                try {
                    RetryUtil.executeWithRetry(() -> {
                        if (isUpsert) {
                            //覆盖更新
                            session.apply(upsert);
                        } else {
                            //增量更新
                            session.apply(insert);
                        }
                        //flush
                        if (counter.longValue() > (batchSize * 0.8)) {
                            session.flush();
                            counter.reset();
                        }
                        counter.increment();
                        return true;
                    }, 5, 500L, true);

                } catch (Exception e) {
                    LOG.error("Record Write Failure!", e);
                    if (isSkipFail) {
                        LOG.warn("Since you have configured \"skipFail\" to be true, this record will be skipped !");
                        taskPluginCollector.collectDirtyRecord(record, e.getMessage());
                    } else {
                        throw DataXException.asDataXException(Kudu11xWriterErrorcode.PUT_KUDU_ERROR, e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("write failure! the task will exit!");
            throw DataXException.asDataXException(Kudu11xWriterErrorcode.PUT_KUDU_ERROR, e.getMessage());
        }
        AtomicInteger i = new AtomicInteger(10);
        try {
            while (i.get() > 0) {
                if (session.hasPendingOperations()) {
                    session.flush();
                    break;
                }
                Thread.sleep(20L);
                i.decrementAndGet();
            }
        } catch (Exception e) {
            LOG.info("Waiting for data to be written to kudu...... " + i + "s");

        } finally {
            try {
                pool.shutdown();
                //强制刷写
                session.flush();
            } catch (KuduException e) {
                LOG.error("kuduwriter flush error! The results may be incomplete！");
                throw DataXException.asDataXException(Kudu11xWriterErrorcode.PUT_KUDU_ERROR, e.getMessage());
            }
        }

    }


}
