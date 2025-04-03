package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.AbstractConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ObClientConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.task.ConcurrentTableWriterTask.ConcurrentTableWriter;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.util.ObWriterUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class InsertTask extends AbstractInsertTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(InsertTask.class);

    private ConcurrentTableWriterTask writerTask;
    private ConcurrentTableWriter writer;

    private String writeRecordSql;
    private long totalCost = 0;
    private long insertCount = 0;

    private BlockingQueue<List<Record>> queue;
    private boolean isStop;
    private AbstractConnHolder connHolder;

    private final long taskId;
    private ServerConnectInfo connInfo;

    // 失败重试次数
    private int failTryCount = Config.DEFAULT_FAIL_TRY_COUNT;
    private boolean printCost = Config.DEFAULT_PRINT_COST;
    private long costBound = Config.DEFAULT_COST_BOUND;

    public InsertTask(
            final long taskId,
            BlockingQueue<List<Record>> recordsQueue,
            Configuration config,
            ServerConnectInfo connectInfo,
            String writeRecordSql) {
        super(taskId, recordsQueue, config, connectInfo);
        this.taskId = taskId;
        this.queue = recordsQueue;
        this.connInfo = connectInfo;
        failTryCount = config.getInt(Config.FAIL_TRY_COUNT, Config.DEFAULT_FAIL_TRY_COUNT);
        printCost = config.getBool(Config.PRINT_COST, Config.DEFAULT_PRINT_COST);
        costBound = config.getLong(Config.COST_BOUND, Config.DEFAULT_COST_BOUND);
        this.connHolder = new ObClientConnHolder(config, connInfo.jdbcUrl,
                connInfo.getFullUserName(), connInfo.password);
        this.writeRecordSql = writeRecordSql;
        this.isStop = false;
        connHolder.initConnection();
    }

    protected void initConnHolder() {

    }

    public void setWriterTask(ConcurrentTableWriterTask writerTask) {
        this.writerTask = writerTask;
    }

    public void setWriter(ConcurrentTableWriter writer) {
        this.writer = writer;
    }

    private boolean isStop() {
        return isStop;
    }

    public void setStop() {
        isStop = true;
    }

    public long getTotalCost() {
        return totalCost;
    }

    public long getInsertCount() {
        return insertCount;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("%d-insertTask-%d", taskId, Thread.currentThread().getId()));
        LOG.debug("Task {} start to execute...", taskId);
        while (!isStop()) {
            try {
                List<Record> records = queue.poll(5, TimeUnit.MILLISECONDS);
                if (null != records) {
                    doMultiInsert(records, this.printCost, this.costBound);
                } else if (writerTask.isFinished()) {
                    writerTask.singalTaskFinish();
                    LOG.debug("not more task, thread exist ...");
                    break;
                }
            } catch (InterruptedException e) {
                LOG.debug("TableWriter is interrupt");
            } catch (Exception e) {
                LOG.warn("ERROR UNEXPECTED ", e);
            }
        }
        LOG.debug("Thread exist...");
    }

    protected void write(List<Record> records) {

    }

    public void destroy() {
        connHolder.destroy();
    }

    public void calStatistic(final long cost) {
        writer.increFinishCount();
        ++insertCount;
        totalCost += cost;
        if (this.printCost && cost > this.costBound) {
            LOG.info("slow multi insert cost {}ms", cost);
        }
    }

    public void doMultiInsert(final List<Record> buffer, final boolean printCost, final long restrict) {
        checkMemstore();
        Connection conn = connHolder.getConn();
        boolean success = false;
        long cost = 0;
        long startTime = 0;
        try {
            for (int i = 0; i < failTryCount; ++i) {
                if (i > 0) {
                    conn = connHolder.getConn();
                    LOG.info("retry {}, start do batch insert, size={}", i, buffer.size());
                    checkMemstore();
                }
                startTime = System.currentTimeMillis();
                PreparedStatement ps = null;
                try {
                    conn.setAutoCommit(false);
                    ps = conn.prepareStatement(writeRecordSql);
                    for (Record record : buffer) {
                        ps = writerTask.fillStatement(ps, record);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    conn.commit();
                    success = true;
                    cost = System.currentTimeMillis() - startTime;
                    calStatistic(cost);
                    break;
                } catch (SQLException e) {
                    LOG.warn("Insert fatal error SqlState ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
                    if (LOG.isDebugEnabled() && (i == 0 || i > 10)) {
                        for (Record record : buffer) {
                            LOG.warn("ERROR : record {}", record);
                        }
                    }
                    // 按照错误码分类，分情况处理
                    // 如果是OB系统级异常,则需要重建连接
                    boolean fatalFail = ObWriterUtils.isFatalError(e);
                    if (fatalFail) {
                        ObWriterUtils.sleep(300000);
                        connHolder.reconnect();
                        // 如果是可恢复的异常,则重试
                    } else if (ObWriterUtils.isRecoverableError(e)) {
                        conn.rollback();
                        ObWriterUtils.sleep(60000);
                    } else {// 其它异常直接退出,采用逐条写入方式
                        conn.rollback();
                        ObWriterUtils.sleep(1000);
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.warn("Insert error unexpected {}", e);
                } finally {
                    DBUtil.closeDBResources(ps, null);
                }
            }
        } catch (SQLException e) {
            LOG.warn("ERROR:retry failSql State ={}, errorCode = {}, {}", e.getSQLState(), e.getErrorCode(), e);
        }

        if (!success) {
            LOG.info("do one insert");
            conn = connHolder.reconnect();
            writerTask.doOneInsert(conn, buffer);
            cost = System.currentTimeMillis() - startTime;
            calStatistic(cost);
        }
    }

    private void checkMemstore() {
        if (writerTask.isShouldSlow()) {
            ObWriterUtils.sleep(100);
        } else {
            while (writerTask.isShouldPause()) {
                ObWriterUtils.sleep(100);
            }
        }
    }
}
