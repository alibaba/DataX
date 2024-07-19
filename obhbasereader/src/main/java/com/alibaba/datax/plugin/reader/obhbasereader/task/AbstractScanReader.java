package com.alibaba.datax.plugin.reader.obhbasereader.task;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obhbasereader.Constant;
import com.alibaba.datax.plugin.reader.obhbasereader.HTableManager;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.obhbasereader.Key;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;

import com.alipay.oceanbase.hbase.OHTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractScanReader extends AbstractHbaseTask {
    private static Logger LOG = LoggerFactory.getLogger(AbstractScanReader.class);
    protected OHTable ohtable;
    protected Result lastResult = null;
    protected Scan scan;
    protected ResultScanner resultScanner;
    protected int maxVersion;
    private int scanCache;
    private byte[] startKey = null;
    private byte[] endKey = null;

    public AbstractScanReader(Configuration configuration) {
        super(configuration);
        this.maxVersion = configuration.getInt(Key.MAX_VERSION, 1);
        this.scanCache = configuration.getInt(Key.SCAN_CACHE, Constant.DEFAULT_SCAN_CACHE);
        this.ohtable = ObHbaseReaderUtil.initOHtable(configuration);
        this.startKey = ObHbaseReaderUtil.convertInnerStartRowkey(configuration);
        this.endKey = ObHbaseReaderUtil.convertInnerEndRowkey(configuration);
        LOG.info("The task set startRowkey=[{}], endRowkey=[{}].", Bytes.toStringBinary(this.startKey), Bytes.toStringBinary(this.endKey));
    }

    @Override
    public void prepare() throws Exception {
        this.scan = new Scan();
        this.scan.setSmall(false);
        this.scan.setCacheBlocks(false);
        this.scan.setStartRow(startKey);
        this.scan.setStopRow(endKey);
        LOG.info("The task set startRowkey=[{}], endRowkey=[{}].", Bytes.toStringBinary(this.startKey), Bytes.toStringBinary(this.endKey));
        this.scan.setCaching(this.scanCache);
        if (this.maxVersion == -1 || this.maxVersion == Integer.MAX_VALUE) {
            this.scan.setMaxVersions();
        } else {
            this.scan.setMaxVersions(this.maxVersion);
        }
        initScanColumns();
        this.resultScanner = this.ohtable.getScanner(this.scan);
    }

    @Override
    public void close() throws IOException {
        if (this.resultScanner != null) {
            this.resultScanner.close();
        }
        HTableManager.closeHTable(this.ohtable);
    }

    protected void initScanColumns() {
        boolean isConstant;
        boolean isRowkeyColumn;
        for (HbaseColumnCell cell : this.hbaseColumnCellMap.values()) {
            isConstant = cell.isConstant();
            isRowkeyColumn = ObHbaseReaderUtil.isRowkeyColumn(cell.getColumnName());
            if (!isConstant && !isRowkeyColumn) {
                LOG.info("columnFamily: " + new String(cell.getCf()) + ", qualifier: " + new String(cell.getQualifier()));
                this.scan.addColumn(cell.getCf(), cell.getQualifier());
            }
        }
    }

    protected Result getNextHbaseRow() throws Exception {
        Result result = null;
        try {
            result = resultScanner.next();
        } catch (Exception e) {
            LOG.error("failed to get result", e);
            if (lastResult != null) {
                scan.setStartRow(lastResult.getRow());
            }
            resultScanner = this.ohtable.getScanner(scan);
            result = resultScanner.next();
            if (lastResult != null && Bytes.equals(lastResult.getRow(), result.getRow())) {
                result = resultScanner.next();
            }
        }
        lastResult = result;
        // may be null
        return result;
    }
}
