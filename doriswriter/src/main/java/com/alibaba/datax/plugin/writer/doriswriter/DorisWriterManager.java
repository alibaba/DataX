package com.alibaba.datax.plugin.writer.doriswriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

public class DorisWriterManager {

    private static final Logger LOG=LoggerFactory.getLogger(DorisWriterManager.class);
    public static final String EOF = "EOF";
    private final DorisWriterEmitter dorisWriterEmitter;
    private final Key keys;
    private final List<String> buffer;
    private int batchCount;
    private long batchSize;
    private volatile boolean closed;
    private volatile Exception flushException;
    private final LinkedBlockingDeque<DorisFlushBatch> flushQueue;

    public DorisWriterManager(final Key keys) {
        this.buffer = new ArrayList<>();
        this.batchCount = 0;
        this.batchSize = 0L;
        this.closed = false;
        this.keys = keys;
        this.dorisWriterEmitter = new DorisWriterEmitter(keys);
        this.flushQueue = new LinkedBlockingDeque<>(keys.getFlushQueueLength());
        this.startAsyncFlushing();
    }

    public final synchronized void writeRecord(final String record) throws IOException {
        this.checkFlushException();
        try {
            this.buffer.add(record);
            ++this.batchCount;
            this.batchSize += record.getBytes().length;
            if (this.batchCount >= this.keys.getBatchRows() || this.batchSize >= this.keys.getBatchSize()) {
                final String label = this.createBatchLabel();
                LOG.debug(String.format("Doris buffer Sinking triggered: rows[%d] label[%s].", this.batchCount, label));
                this.flush(label, false);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to Doris failed.", e);
        }
    }

    public synchronized void flush(final String label, final boolean isDone) throws Exception {
        this.checkFlushException();
        if (this.batchCount == 0) {
            if (isDone) {
                this.waitAsyncFlushingDone();
            }
            return;
        }
        this.flushQueue.put(new DorisFlushBatch(label, this.batchSize, new ArrayList<>(this.buffer)));
        if (isDone) {
            this.waitAsyncFlushingDone();
        }
        this.buffer.clear();
        this.batchCount = 0;
        this.batchSize = 0L;
    }

    public synchronized void close() {
        if (!this.closed) {
            this.closed = true;
            try {
                final String label = this.createBatchLabel();
                if (this.batchCount > 0) {
                    LOG.debug(String.format("Doris Sink is about to close: label[%s].", label));
                }
                this.flush(label, true);
            } catch (Exception e) {
                throw new RuntimeException("Writing records to Doris failed.", e);
            }
        }
        this.checkFlushException();
    }

    public String createBatchLabel() {
        return UUID.randomUUID().toString();
    }

    private void startAsyncFlushing() {
        final Thread flushThread = new Thread(() -> {
            while (true) {
                try {
                    while (true) {
                        DorisWriterManager.this.asyncFlush();
                    }
                } catch (Exception e) {
                    DorisWriterManager.this.flushException = e;
                }
            }
        },"DataSubmitDoris");
        flushThread.setDaemon(true);
        flushThread.start();
    }

    private void waitAsyncFlushingDone() throws InterruptedException {
        for (int i = 0; i <= this.keys.getFlushQueueLength(); ++i) {
            this.flushQueue.put(new DorisFlushBatch(EOF, 0L, null));
        }
    }

    private void asyncFlush() throws Exception {
        final DorisFlushBatch flushData = this.flushQueue.take();
        if (flushData.getLabel().equals(EOF)) {
            return;
        }
        LOG.debug(String.format("Async stream load: rows[%d] bytes[%d] label[%s].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel()));
        int i = 0;
        while (i <= this.keys.getMaxRetries()) {
            try {
                this.dorisWriterEmitter.doStreamLoad(flushData);
                LOG.info(String.format("Async stream load finished: label[%s].", flushData.getLabel()));
            } catch (Exception e) {
                LOG.warn("Failed to flush batch data to doris, retry times = {} ,Exception ={}", i, e);
                if (i >= this.keys.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000L * (i + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Unable to flush, interrupted while doing another attempt", e);
                }
                ++i;
                continue;
            }
            break;
        }
    }

    private void checkFlushException() {
        if (this.flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", this.flushException);
        }
    }


}
