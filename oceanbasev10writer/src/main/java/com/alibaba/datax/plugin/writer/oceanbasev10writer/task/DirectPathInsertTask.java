package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.common.Table;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.common.TableCache;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath.DirectPathConnection;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.directPath.DirectPathPreparedStatement;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.DirectPathConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectPathInsertTask extends AbstractInsertTask {
    private static final Logger LOG = LoggerFactory.getLogger(DirectPathInsertTask.class);

    public DirectPathInsertTask(long taskId, Queue<List<Record>> recordsQueue, Configuration config, ServerConnectInfo connectInfo, ConcurrentTableWriterTask task, ConcurrentTableWriterTask.ConcurrentTableWriter writer) {
        super(taskId, recordsQueue, config, connectInfo, task, writer);
    }

    @Override
    protected void initConnHolder() {
        this.connHolder = new DirectPathConnHolder(config, connInfo, writerTask.getTable(), writer.getThreadCount());
        this.connHolder.initConnection();
    }

    @Override
    protected void write(List<Record> records) {
        Table table = TableCache.getInstance().getTable(connInfo.databaseName, writerTask.getTable());
        if (Table.Status.FAILURE.equals(table.getStatus())) {
            return;
        }
        DirectPathConnection conn = (DirectPathConnection) connHolder.getConn();
        if (records != null && !records.isEmpty()) {
            long startTime = System.currentTimeMillis();
            try (DirectPathPreparedStatement stmt = conn.createStatement()) {
                final int columnNumber = records.get(0).getColumnNumber();
                Object[] values = new Object[columnNumber];
                for (Record record : records) {
                    for (int i = 0; i < columnNumber; i++) {
                        Column column = record.getColumn(i);
                        //处理一下时间类型
                        if (column.getType().equals(Type.DATE)) {
                            values[i] = record.getColumn(i).asString();
                        } else {
                            values[i] = record.getColumn(i).getRawData();
                        }
                    }
                    stmt.addBatch(values);
                }

                int[] result = stmt.executeBatch();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Insert {} rows success", Thread.currentThread().getName(), Arrays.stream(result).sum());
                }
                calStatistic(System.currentTimeMillis() - startTime);
                stmt.clearBatch();
            } catch (Throwable ex) {
                String msg = MessageFormat.format("Insert data into table \"{0}\" failed. Error: {1}", writerTask.getTable(), ex.getMessage());
                LOG.error(msg, ex);
                table.setError(ex);
                table.setStatus(Table.Status.FAILURE);
                throw new RuntimeException(msg);
            }
        }
    }
}