package com.dorisdb.connector.datax.plugin.writer.doriswriter.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

import com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriterOptions;
import com.google.common.base.Strings;

public class DorisWriterManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterManager.class);

    private final DorisStreamLoadVisitor dorisStreamLoadVisitor;
    private final DorisWriterOptions writerOptions;

    private final List<String> buffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Exception flushException;
    private final LinkedBlockingDeque<DorisFlushTuple> flushQueue;

    public DorisWriterManager(DorisWriterOptions writerOptions) {
        this.writerOptions = writerOptions;
        this.dorisStreamLoadVisitor = new DorisStreamLoadVisitor(writerOptions);
        flushQueue = new LinkedBlockingDeque<>(writerOptions.getFlushQueueLength()); 
        this.startAsyncFlushing();
    }

    public final synchronized void writeRecord(String record) throws IOException {
        checkFlushException();
        try {
            buffer.add(record);
            batchCount++;
            batchSize += record.getBytes().length;
            if (batchCount >= writerOptions.getBatchRows() || batchSize >= writerOptions.getBatchSize()) {
                String label = createBatchLabel();
                LOG.debug(String.format("Doris buffer Sinking triggered: rows[%d] label[%s].", batchCount, label));
                flush(label, false);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to Doris failed.", e);
        }
    }

    public synchronized void flush(String label, boolean waitUtilDone) throws Exception {
        checkFlushException();
        if (batchCount == 0) {
            if (waitUtilDone) {
                waitAsyncFlushingDone();
            }
            return;
        }
        flushQueue.put(new DorisFlushTuple(label, batchSize,  new ArrayList<>(buffer)));
        if (waitUtilDone) {
            // wait the last flush
            waitAsyncFlushingDone();
        }
        buffer.clear();
        batchCount = 0;
        batchSize = 0;
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;            
            try {
                String label = createBatchLabel();
                if (batchCount > 0) LOG.debug(String.format("Doris Sink is about to close: label[%s].", label));
                flush(label, true);
            } catch (Exception e) {
                throw new RuntimeException("Writing records to Doris failed.", e);
            }
        }
        checkFlushException();
    }

    public String createBatchLabel() {
        return UUID.randomUUID().toString();
    }

    private void startAsyncFlushing() {
        // start flush thread
        Thread flushThread = new Thread(new Runnable(){
            public void run() {
                while(true) {
                    try {
                        asyncFlush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }   
        });
        flushThread.setDaemon(true);
        flushThread.start();
    }

    private void waitAsyncFlushingDone() throws InterruptedException {
        // wait previous flushings
        for (int i = 0; i <= writerOptions.getFlushQueueLength(); i++) {
            flushQueue.put(new DorisFlushTuple("", 0l, null));
        }
    }

    private void asyncFlush() throws Exception {
        DorisFlushTuple flushData = flushQueue.take();
        if (Strings.isNullOrEmpty(flushData.getLabel())) {
            return;
        }
        LOG.debug(String.format("Async stream load: rows[%d] bytes[%d] label[%s].", flushData.getRows().size(), flushData.getBytes(), flushData.getLabel()));
        for (int i = 0; i <= writerOptions.getMaxRetries(); i++) {
            try {
                // flush to Doris with stream load
                dorisStreamLoadVisitor.doStreamLoad(flushData);
                LOG.info(String.format("Async stream load finished: label[%s].", flushData.getLabel()));
                break;
            } catch (Exception e) {
                LOG.warn("Failed to flush batch data to doris, retry times = {}", i, e);
                if (i >= writerOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000l * (i + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Unable to flush, interrupted while doing another attempt", e);
                }
            }
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", flushException);
        }
    }
}
