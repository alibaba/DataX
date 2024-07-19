package com.alibaba.datax.plugin.writer.obhbasewriter.task;

import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.plugin.writer.obhbasewriter.ColumnType;
import com.alibaba.datax.plugin.writer.obhbasewriter.Config;
import com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey;
import com.alibaba.datax.plugin.writer.obhbasewriter.Hbase094xWriterErrorCode;
import com.alibaba.datax.plugin.writer.obhbasewriter.ObHTableInfo;
import com.alibaba.datax.plugin.writer.obhbasewriter.ext.ObHbaseTableHolder;
import com.alibaba.datax.plugin.writer.obhbasewriter.ext.ServerConnectInfo;
import com.alipay.oceanbase.hbase.constants.OHConstants;
import com.alipay.oceanbase.rpc.property.Property;

import com.google.common.base.Stopwatch;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey.OBHBASE_HTABLE_CLIENT_WRITE_BUFFER;
import static com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey.OBHBASE_HTABLE_PUT_WRITE_BUFFER_CHECK;
import static com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey.TABLE_CLIENT_RPC_EXECUTE_TIMEOUT;
import static com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey.WRITE_BUFFER_HIGH_MARK;
import static com.alibaba.datax.plugin.writer.obhbasewriter.ConfigKey.WRITE_BUFFER_LOW_MARK;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_HBASE_HTABLE_CLIENT_WRITE_BUFFER;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_NETTY_BUFFER_HIGH_WATERMARK;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_NETTY_BUFFER_LOW_WATERMARK;
import static com.alibaba.datax.plugin.writer.obhbasewriter.Constant.DEFAULT_RPC_EXECUTE_TIMEOUT;
import static com.alibaba.datax.plugin.writer.obhbasewriter.util.ObHbaseWriterUtils.getColumnByte;
import static com.alibaba.datax.plugin.writer.obhbasewriter.util.ObHbaseWriterUtils.getRowkey;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_CLIENT_WRITE_BUFFER;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_DATABASE;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_FULL_USER_NAME;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_PARAM_URL;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_PASSWORD;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_SYS_USER_NAME;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_SYS_PASSWORD;

public class PutTask implements Runnable {

    private static final MessageSource MESSAGE_SOURCE = MessageSource.loadResourceBundle(PutTask.class);

    private static final Logger LOG = LoggerFactory.getLogger(PutTask.class);

    private ObHBaseWriteTask writerTask;
    private ObHBaseWriteTask.ConcurrentTableWriter writer;

    private long totalCost = 0;
    private long putCount = 0;
    private boolean isStop;

    private ObHTableInfo obHTableInfo;
    private final Configuration versionColumn;
    // 失败重试次数
    private final int failTryCount;

    private String parentThreadName;
    private Queue<List<Record>> queue;
    private Configuration config;
    private ServerConnectInfo connInfo;

    private ObHbaseTableHolder tableHolder;

    private final SimpleDateFormat df_second = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final SimpleDateFormat df_ms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");

    public PutTask(String parentThreadName, Queue<List<Record>> recordsQueue, Configuration config, ServerConnectInfo connectInfo, ObHTableInfo obHTableInfo, ObHBaseWriteTask writerTask) {
        this.parentThreadName = parentThreadName;
        this.queue = recordsQueue;
        this.config = config;
        this.connInfo = connectInfo;
        this.obHTableInfo = obHTableInfo;
        this.writerTask = writerTask;
        this.versionColumn = config.getConfiguration(ConfigKey.VERSION_COLUMN);
        this.failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);
        this.isStop = false;
        initTableHolder();
    }

    private void initTableHolder() {
        try {
            org.apache.hadoop.conf.Configuration c = new org.apache.hadoop.conf.Configuration();
            c.set(HBASE_OCEANBASE_FULL_USER_NAME, writerTask.fullUserName);
            c.set(HBASE_OCEANBASE_PASSWORD, this.connInfo.password);
            c.set(HBASE_OCEANBASE_DATABASE, writerTask.dbName);
            // obkv-table-client is needed the code below
            if (writerTask.usdOdpMode) {
                c.setBoolean(OHConstants.HBASE_OCEANBASE_ODP_MODE, true);
                c.set(OHConstants.HBASE_OCEANBASE_ODP_ADDR, connInfo.host);
                c.set(OHConstants.HBASE_OCEANBASE_ODP_PORT, connInfo.port);
                LOG.info("sysUser and sysPassword is empty, build HTABLE in odp mode.");
            } else {
                c.set(HBASE_OCEANBASE_PARAM_URL, writerTask.configUrl);
                c.set(HBASE_OCEANBASE_SYS_USER_NAME, this.connInfo.sysUser);
                c.set(HBASE_OCEANBASE_SYS_PASSWORD, this.connInfo.sysPass);
                LOG.info("sysUser and sysPassword is not empty, build HTABLE in sys mode.");
            }
            c.set(HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK, config.getString(OBHBASE_HTABLE_PUT_WRITE_BUFFER_CHECK, DEFAULT_HBASE_HTABLE_PUT_WRITE_BUFFER_CHECK));
            c.set(HBASE_HTABLE_CLIENT_WRITE_BUFFER, config.getString(OBHBASE_HTABLE_CLIENT_WRITE_BUFFER, DEFAULT_HBASE_HTABLE_CLIENT_WRITE_BUFFER));

            c.set(Property.RS_LIST_ACQUIRE_CONNECT_TIMEOUT.getKey(), "500");
            c.set(Property.RS_LIST_ACQUIRE_READ_TIMEOUT.getKey(), "5000");
            c.set(Property.RPC_EXECUTE_TIMEOUT.getKey(), config.getString(TABLE_CLIENT_RPC_EXECUTE_TIMEOUT, DEFAULT_RPC_EXECUTE_TIMEOUT));
            c.set(Property.NETTY_BUFFER_LOW_WATERMARK.getKey(), config.getString(WRITE_BUFFER_LOW_MARK, DEFAULT_NETTY_BUFFER_LOW_WATERMARK));
            c.set(Property.NETTY_BUFFER_HIGH_WATERMARK.getKey(), config.getString(WRITE_BUFFER_HIGH_MARK, DEFAULT_NETTY_BUFFER_HIGH_WATERMARK));
            this.tableHolder = new ObHbaseTableHolder(c, obHTableInfo.getTableName());
        } catch (Exception e) {
            LOG.error("init table holder failed, reason: {}", e.getMessage());
            throw new IllegalStateException(e);
        }

    }

    private void batchWrite(final List<Record> buffer) {
        HTableInterface ohTable = null;
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            ohTable = this.tableHolder.getOhTable();
            List<Put> puts = buildBatchPutList(buffer);
            ohTable.put(puts);
        } catch (Exception e) {
            if (Objects.isNull(ohTable)) {
                LOG.error("build obHTable: {} failed. reason: {}", obHTableInfo.getTableName(), e.getMessage());
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.GET_HBASE_TABLE_ERROR, Hbase094xWriterErrorCode.GET_HBASE_TABLE_ERROR.getDescription());
            }
            //
            LOG.error("hbase batch error: " + e);
            // 出错了之后对该出错的batch逐条重试
            for (Record record : buffer) {
                writeOneRecord(ohTable, record);
            }
        } finally {
            this.writer.increFinishCount();
            putCount++;
            totalCost += stopwatch.elapsed(TimeUnit.MILLISECONDS);
            try {
                if (!Objects.isNull(ohTable)) {
                    ohTable.close();
                }
            } catch (Exception e) {
                LOG.warn("error in closing htable: {}. Reason: {}", obHTableInfo.getFullHbaseTableName(), e.getMessage());
            }
        }
    }

    private void writeOneRecord(HTableInterface ohTable, Record record) {
        int retryCount = 0;
        while (retryCount < this.failTryCount) {
            try {
                byte[] rowkey = getRowkey(record, obHTableInfo);
                Put put = new Put(rowkey); // row key
                boolean hasValidValue = buildPut(put, record);

                if (hasValidValue) {
                    ohTable.put(put);
                }
                break;
            } catch (Exception e) {
                retryCount++;
                LOG.error("error in writing: " + e.getMessage() + ", retry count: " + retryCount);
                if (retryCount == this.failTryCount) {
                    LOG.warn("ERROR : record {}", record);
                    this.writerTask.collectDirtyRecord(record, e);
                }
            }
        }
    }

    private List<Put> buildBatchPutList(List<Record> buffer) {
        List<Put> puts = new ArrayList<>();
        for (Record record : buffer) {
            byte[] rowkey = getRowkey(record, obHTableInfo);
            Put put = new org.apache.hadoop.hbase.client.Put(rowkey); // row key
            boolean hasValidValue = buildPut(put, record);
            if (hasValidValue) {
                puts.add(put);
            }
        }
        return puts;
    }

    private boolean buildPut(Put put, Record record) {
        boolean hasValidValue = false;
        long timestamp = buildTimestamp(record);
        for (Map.Entry<Integer, Triple<String, String, ColumnType>> columnInfo : obHTableInfo.getIndexColumnInfoMap().entrySet()) {
            Integer index = columnInfo.getKey();
            if (index >= record.getColumnNumber()) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.ILLEGAL_VALUE,
                        MESSAGE_SOURCE.message("normaltask.2", record.getColumnNumber(), index));
            }
            ColumnType columnType = columnInfo.getValue().getRight();
            String familyName = columnInfo.getValue().getLeft();
            String columnName = columnInfo.getValue().getMiddle();

            byte[] value = getColumnByte(columnType, record.getColumn(index), obHTableInfo);
            if (value != null) {
                hasValidValue = true;
                if (timestamp == -1) {
                    put.add(familyName.getBytes(), // family
                            columnName.getBytes(),     // Q
                            value);                           // V
                } else {
                    put.add(familyName.getBytes(), // family
                            columnName.getBytes(),     // Q
                            timestamp,                        // timestamp/version
                            value);                           // V
                }
            }
        }

        return hasValidValue;
    }

    private long buildTimestamp(Record record) {
        if (versionColumn == null) {
            return -1;
        }

        int index = versionColumn.getInt(ConfigKey.INDEX);
        long timestamp;
        if (index == -1) {
            // user specified the constant as timestamp
            timestamp = versionColumn.getLong(ConfigKey.VALUE);
            if (timestamp < 0) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR,
                        MESSAGE_SOURCE.message("normaltask.4"));
            }
        } else {
            // 指定列作为版本,long/doubleColumn直接record.aslong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,
            // yyyy-MM-dd HH:mm:ss SSS去format
            if (index >= record.getColumnNumber()) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR,
                        MESSAGE_SOURCE.message("normaltask.5", record.getColumnNumber(), index));
            }

            if (record.getColumn(index).getRawData() == null) {
                throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR,
                        MESSAGE_SOURCE.message("normaltask.6"));
            }

            if (record.getColumn(index) instanceof LongColumn || record.getColumn(index) instanceof DoubleColumn) {
                timestamp = record.getColumn(index).asLong();
            } else {
                Date date;
                try {
                    date = df_ms.parse(record.getColumn(index).asString());
                } catch (ParseException e) {
                    try {
                        date = df_second.parse(record.getColumn(index).asString());
                    } catch (ParseException e1) {
                        LOG.info(MESSAGE_SOURCE.message("normaltask.7", index));
                        throw DataXException.asDataXException(Hbase094xWriterErrorCode.CONSTRUCT_VERSION_ERROR, e1);
                    }
                }
                timestamp = date.getTime();
            }
        }

        return timestamp;
    }

    public void setStop() {isStop = true;}

    public long getTotalCost() {return totalCost;}

    public long getPutCount() {return putCount;}

    public void destroy() {
        tableHolder.destroy();
    }

    void setWriterTask(ObHBaseWriteTask writerTask) {
        this.writerTask = writerTask;
    }

    void setWriter(ObHBaseWriteTask.ConcurrentTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public void run() {
        String currentThreadName = String.format("%s-putTask-%d", parentThreadName, Thread.currentThread().getId());
        Thread.currentThread().setName(currentThreadName);
        LOG.debug("Task {} start to execute...", currentThreadName);
        int sleepTimes = 0;
        while (!isStop) {
            try {
                List<Record> records = queue.poll();
                if (null != records) {
                    batchWrite(records);
                } else if (writerTask.isFinished()) {
                    writerTask.singalTaskFinish();
                    LOG.debug("not more task, thread exist ...");
                    break;
                } else {
                    TimeUnit.MILLISECONDS.sleep(5);
                    sleepTimes++;
                }
            } catch (InterruptedException e) {
                LOG.debug("TableWriter is interrupt");
            } catch (Exception e) {
                LOG.warn("ERROR UNEXPECTED {}", e);
            }
        }
        LOG.debug("Thread exist...");
        LOG.debug("sleep {} times, total sleep time: {}", sleepTimes, sleepTimes * 5);
    }
}
