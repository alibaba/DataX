package com.dorisdb.connector.datax.plugin.writer.doriswriter.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriterOptions;

public class DorisWriterManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(DorisWriterManager.class);

    private final DorisStreamLoadVisitor dorisStreamLoadVisitor;
    private final DorisWriterOptions writerOptions;

    private final List<String> buffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Exception flushException;

    public DorisWriterManager(DorisWriterOptions writerOptions) {
        this.writerOptions = writerOptions;
        this.dorisStreamLoadVisitor = new DorisStreamLoadVisitor(writerOptions);
    }

    public final synchronized void writeRecord(String record) throws IOException {
        checkFlushException();
        try {
            buffer.add(record);
            batchCount++;
            batchSize += record.getBytes().length;
            if (batchCount >= writerOptions.getBatchRows() || batchSize >= writerOptions.getBatchSize()) {
                flush(createBatchLabel());
            }
        } catch (Exception e) {
            throw new IOException("Writing records to Doris failed.", e);
        }
    }

    public synchronized void flush(String label) throws IOException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        for (int i = 0; i <= writerOptions.getMaxRetries(); i++) {
            try {
                tryToFlush(label);
                buffer.clear();
                batchCount = 0;
                batchSize = 0;
                break;
            } catch (IOException e) {
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
    
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (batchCount > 0) {
                try {
                    flush(createBatchLabel());
                } catch (Exception e) {
                    throw new RuntimeException("Writing records to Doris failed.", e);
                }
            }
        }
        checkFlushException();
    }

    public String createBatchLabel() {
        return UUID.randomUUID().toString();
    }

    public List<String> getBufferedBatchList() {
        return buffer;
    }

    public void setBufferedBatchList(List<String> buffer) {
        this.buffer.clear();
        this.buffer.addAll(buffer);
    }

    private void tryToFlush(String label) throws IOException {
        // flush to Doris with stream load
        dorisStreamLoadVisitor.doStreamLoad(label, buffer);
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", flushException);
        }
    }
}
