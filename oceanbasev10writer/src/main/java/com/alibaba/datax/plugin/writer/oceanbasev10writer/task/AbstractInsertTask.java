package com.alibaba.datax.plugin.writer.oceanbasev10writer.task;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.Config;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.AbstractConnHolder;
import com.alibaba.datax.plugin.writer.oceanbasev10writer.ext.ServerConnectInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractInsertTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractInsertTask.class);
    protected final long taskId;
    protected ConcurrentTableWriterTask writerTask;
    protected ConcurrentTableWriterTask.ConcurrentTableWriter writer;
    protected Queue<List<Record>> queue;
    protected boolean isStop;
    protected Configuration config;
    protected ServerConnectInfo connInfo;
    protected AbstractConnHolder connHolder;
    protected long totalCost = 0;
    protected long insertCount = 0;
    private boolean printCost = Config.DEFAULT_PRINT_COST;
    private long costBound = Config.DEFAULT_COST_BOUND;

    public AbstractInsertTask(final long taskId, Queue<List<Record>> recordsQueue, Configuration config, ServerConnectInfo connectInfo, ConcurrentTableWriterTask task, ConcurrentTableWriterTask.ConcurrentTableWriter writer) {
        this.taskId = taskId;
        this.queue = recordsQueue;
        this.config = config;
        this.connInfo = connectInfo;
        this.isStop = false;
        this.printCost = config.getBool(Config.PRINT_COST, Config.DEFAULT_PRINT_COST);
        this.costBound = config.getLong(Config.COST_BOUND, Config.DEFAULT_COST_BOUND);
        this.writer = writer;
        this.writerTask = task;
        initConnHolder();
    }

    public AbstractInsertTask(final long taskId, Queue<List<Record>> recordsQueue, Configuration config, ServerConnectInfo connectInfo) {
        this.taskId = taskId;
        this.queue = recordsQueue;
        this.config = config;
        this.connInfo = connectInfo;
        this.isStop = false;
        this.printCost = config.getBool(Config.PRINT_COST, Config.DEFAULT_PRINT_COST);
        this.costBound = config.getLong(Config.COST_BOUND, Config.DEFAULT_COST_BOUND);
        initConnHolder();
    }

    protected abstract void initConnHolder();

    public void setWriterTask(ConcurrentTableWriterTask writerTask) {
        this.writerTask = writerTask;
    }

    public void setWriter(ConcurrentTableWriterTask.ConcurrentTableWriter writer) {
        this.writer = writer;
    }

    private boolean isStop() {
        return isStop;
    }

    public void setStop() {
        isStop = true;
    }

    public AbstractConnHolder getConnHolder() {
        return connHolder;
    }

    public void calStatistic(final long cost) {
        writer.increFinishCount();
        insertCount++;
        totalCost += cost;
        if (this.printCost && cost > this.costBound) {
            LOG.info("slow multi insert cost {}ms", cost);
        }
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("%d-insertTask-%d", taskId, Thread.currentThread().getId()));
        LOG.debug("Task {} start to execute...", taskId);
        while (!isStop()) {
            try {
                List<Record> records = queue.poll();
                if (null != records) {
                    write(records);
                } else if (writerTask.isFinished()) {
                    writerTask.singalTaskFinish();
                    LOG.debug("not more task, thread exist ...");
                    break;
                } else {
                    TimeUnit.MILLISECONDS.sleep(5);
                }
            } catch (InterruptedException e) {
                LOG.debug("TableWriter is interrupt");
            } catch (Exception e) {
                LOG.warn("ERROR UNEXPECTED ", e);
                break;
            }
        }
        LOG.debug("Thread exist...");
    }

    protected abstract void write(List<Record> records);

    public long getTotalCost() {
        return totalCost;
    }

    public long getInsertCount() {
        return insertCount;
    }

    public void destroy() {
        if (connHolder != null) {
            connHolder.destroy();
        }
    }
}
