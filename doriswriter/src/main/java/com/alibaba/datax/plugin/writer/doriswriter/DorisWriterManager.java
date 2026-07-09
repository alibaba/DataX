package com.alibaba.datax.plugin.writer.doriswriter;

import com.google.common.base.Strings;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DorisWriterManager {

    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterManager.class);

    private final DorisStreamLoadObserver visitor;
    private final Keys options;
    private final LinkedBlockingDeque<WriterTuple> flushQueue;
    private final List<Thread> flushWorkers = new ArrayList<Thread>();
    private final ScheduledExecutorService scheduler;
    private final FastByteArrayOutputStream batchBuffer;
    private final byte[] lineDelimiter;
    private final byte[] jsonDelimiter = ",".getBytes(StandardCharsets.UTF_8);
    private final byte[] jsonArrayPrefix = "[".getBytes(StandardCharsets.UTF_8);
    private final byte[] jsonArraySuffix = "]".getBytes(StandardCharsets.UTF_8);

    private volatile boolean closed = false;
    private volatile Exception flushException;
    private int batchCount = 0;
    private long batchSize = 0;

    public DorisWriterManager(Keys options) {
        this.options = options;
        this.visitor = new DorisStreamLoadObserver(options);
        this.flushQueue = new LinkedBlockingDeque<WriterTuple>(options.getFlushQueueLength());
        this.batchBuffer = new FastByteArrayOutputStream((int) Math.min(Integer.MAX_VALUE - 8L, Math.max(options.getInitialBufferSize(), options.getBatchSize())));
        this.lineDelimiter = DelimiterParser.parse(options.getLineDelimiter(), "\n").getBytes(StandardCharsets.UTF_8);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new BasicThreadFactory.Builder().namingPattern("doris-interval-flush-%d").daemon(true).build());
        startAsyncFlushing();
        startScheduler();
    }

    private void startScheduler() {
        this.scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                synchronized (DorisWriterManager.this) {
                    if (closed || batchCount == 0) {
                        return;
                    }
                    try {
                        String label = createBatchLabel();
                        LOG.debug("Doris interval flush triggered: rows[{}] bytes[{}] label[{}].", batchCount, batchSize, label);
                        flushInternal(label);
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }, options.getFlushInterval(), options.getFlushInterval(), TimeUnit.MILLISECONDS);
    }

    public synchronized void writeRecord(String record) throws IOException {
        checkFlushException();
        if (closed) {
            throw new IOException("DorisWriterManager is already closed.");
        }
        try {
            byte[] bts = record.getBytes(StandardCharsets.UTF_8);
            if (Keys.StreamLoadFormat.JSON.equals(options.getStreamLoadFormat())) {
                if (batchCount > 0) {
                    batchBuffer.write(jsonDelimiter);
                }
                batchBuffer.write(bts);
            } else {
                batchBuffer.write(bts);
                batchBuffer.write(lineDelimiter);
            }
            batchCount++;
            batchSize += bts.length;
            if (batchCount >= options.getBatchRows() || batchBuffer.size() >= options.getBatchSize()) {
                String label = createBatchLabel();
                LOG.debug("Doris buffer flush triggered: rows[{}] bytes[{}] label[{}].", batchCount, batchBuffer.size(), label);
                flushInternal(label);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to Doris failed.", e);
        }
    }

    public synchronized void close() {
        if (closed) {
            checkFlushException();
            return;
        }
        closed = true;
        scheduler.shutdown();
        try {
            if (batchCount > 0) {
                flushInternal(createBatchLabel());
            }
            for (int i = 0; i < options.getFlushWorkerCount(); i++) {
                flushQueue.put(WriterTuple.poisonPill());
            }
            for (Thread worker : flushWorkers) {
                worker.join();
            }
            visitor.close();
        } catch (Exception e) {
            throw new RuntimeException("Writing records to Doris failed.", e);
        }
        checkFlushException();
    }

    public String createBatchLabel() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(options.getLabelPrefix())) {
            sb.append(options.getLabelPrefix());
        }
        return sb.append(UUID.randomUUID().toString()).toString();
    }

    private void startAsyncFlushing() {
        for (int i = 0; i < options.getFlushWorkerCount(); i++) {
            Thread flushThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            WriterTuple flushData = flushQueue.take();
                            if (flushData.isPoison()) {
                                return;
                            }
                            asyncFlush(flushData);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        } catch (Exception e) {
                            flushException = e;
                            return;
                        }
                    }
                }
            }, "doris-streamload-worker-" + i);
            flushThread.setDaemon(true);
            flushThread.start();
            flushWorkers.add(flushThread);
        }
    }

    private void asyncFlush(WriterTuple flushData) throws Exception {
        LOG.debug("Async stream load start: rows[{}] bytes[{}] label[{}].", flushData.getRows(), flushData.getBytes(), flushData.getLabel());
        for (int i = 0; i <= options.getMaxRetries(); i++) {
            try {
                visitor.streamLoad(flushData);
                LOG.info("Async stream load finished: rows[{}] bytes[{}] label[{}].", flushData.getRows(), flushData.getBytes(), flushData.getLabel());
                return;
            } catch (Exception e) {
                LOG.warn("Failed to flush batch data to Doris, retry times = {}, label = {}", i, flushData.getLabel(), e);
                if (i >= options.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000L * Math.min(i + 1, 10));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Unable to flush, interrupted while doing another attempt", e);
                }
            }
        }
    }

    private void flushInternal(String label) throws InterruptedException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        byte[] payload = buildPayload();
        flushQueue.put(new WriterTuple(label, payload.length, batchCount, payload));
        batchBuffer.reset();
        batchCount = 0;
        batchSize = 0;
    }

    private byte[] buildPayload() {
        if (Keys.StreamLoadFormat.JSON.equals(options.getStreamLoadFormat())) {
            FastByteArrayOutputStream out = new FastByteArrayOutputStream(batchBuffer.size() + 2);
            out.write(jsonArrayPrefix);
            if (batchBuffer.size() > 0) {
                out.write(batchBuffer.toByteArray());
            }
            out.write(jsonArraySuffix);
            return out.toByteArray();
        }
        return batchBuffer.toByteArray();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", flushException);
        }
    }
}
