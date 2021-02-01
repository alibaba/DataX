package com.alibaba.datax.plugin.reader.hbase094xreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class HbaseAbstractTask {
    private final static Logger LOG = LoggerFactory.getLogger(HbaseAbstractTask.class);

    private byte[] startKey = null;
    private byte[] endKey = null;

    protected HTable htable;
    protected String encoding;
    protected int scanCacheSize;
    protected int  scanBatchSize;

    protected Result lastResult = null;
    protected Scan scan;
    protected ResultScanner resultScanner;

    public HbaseAbstractTask(com.alibaba.datax.common.util.Configuration configuration) {

        this.htable = Hbase094xHelper.getTable(configuration);

        this.encoding = configuration.getString(Key.ENCODING,Constant.DEFAULT_ENCODING);
        this.startKey = Hbase094xHelper.convertInnerStartRowkey(configuration);
        this.endKey =  Hbase094xHelper.convertInnerEndRowkey(configuration);
        this.scanCacheSize = configuration.getInt(Key.SCAN_CACHE_SIZE,Constant.DEFAULT_SCAN_CACHE_SIZE);
        this.scanBatchSize = configuration.getInt(Key.SCAN_BATCH_SIZE,Constant.DEFAULT_SCAN_BATCH_SIZE);
    }

    public abstract boolean fetchLine(Record record) throws Exception;

    //不同模式设置不同,如多版本模式需要设置版本
    public abstract void initScan(Scan scan);


    public void prepare() throws Exception {
        this.scan = new Scan();
        this.scan.setSmall(false);
        this.scan.setStartRow(startKey);
        this.scan.setStopRow(endKey);
        LOG.info("The task set startRowkey=[{}], endRowkey=[{}].", Bytes.toStringBinary(this.startKey), Bytes.toStringBinary(this.endKey));
        //scan的Caching Batch全部留在hconfig中每次从服务器端读取的行数，设置默认值未256
        this.scan.setCaching(this.scanCacheSize);
        //设置获取记录的列个数，hbase默认无限制，也就是返回所有的列,这里默认是100
        this.scan.setBatch(this.scanBatchSize);
        //为是否缓存块，hbase默认缓存,同步全部数据时非热点数据，因此不需要缓存
        this.scan.setCacheBlocks(false);
        initScan(this.scan);

        this.resultScanner = this.htable.getScanner(this.scan);
    }

    public void close()  {
        Hbase094xHelper.closeResultScanner(this.resultScanner);
        Hbase094xHelper.closeTable(this.htable);
    }

    protected Result getNextHbaseRow() throws IOException {
        Result result;
        try {
            result = resultScanner.next();
        } catch (IOException e) {
            if (lastResult != null) {
                this.scan.setStartRow(lastResult.getRow());
            }
            resultScanner = this.htable.getScanner(scan);
            result = resultScanner.next();
            if (lastResult != null && Bytes.equals(lastResult.getRow(), result.getRow())) {
                result = resultScanner.next();
            }
        }
        lastResult = result;
        // may be null
        return result;
    }

    public Column convertBytesToAssignType(ColumnType columnType, byte[] byteArray,String dateformat) throws Exception {
        Column column;
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toBoolean(byteArray));
                break;
            case SHORT:
                column = new LongColumn(ArrayUtils.isEmpty(byteArray) ? null : String.valueOf(Bytes.toShort(byteArray)));
                break;
            case INT:
                column = new LongColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toInt(byteArray));
                break;
            case LONG:
                column = new LongColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toLong(byteArray));
                break;
            case FLOAT:
                column = new DoubleColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toFloat(byteArray));
                break;
            case DOUBLE:
                column = new DoubleColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toDouble(byteArray));
                break;
            case STRING:
                column = new StringColumn(ArrayUtils.isEmpty(byteArray) ? null : new String(byteArray, encoding));
                break;
            case BINARY_STRING:
                column = new StringColumn(ArrayUtils.isEmpty(byteArray) ? null : Bytes.toStringBinary(byteArray));
                break;
            case DATE:
                String dateValue = Bytes.toStringBinary(byteArray);
                column = new DateColumn(ArrayUtils.isEmpty(byteArray) ? null : DateUtils.parseDate(dateValue, new String[]{dateformat}));
                break;
            default:
                throw DataXException.asDataXException(Hbase094xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持您配置的列类型:" + columnType);
        }
        return column;
    }

    public Column convertValueToAssignType(ColumnType columnType, String constantValue,String dateformat) throws Exception {
        Column column;
        switch (columnType) {
            case BOOLEAN:
                column = new BoolColumn(constantValue);
                break;
            case SHORT:
            case INT:
            case LONG:
                column = new LongColumn(constantValue);
                break;
            case FLOAT:
            case DOUBLE:
                column = new DoubleColumn(constantValue);
                break;
            case STRING:
                column = new StringColumn(constantValue);
                break;
            case DATE:
                column = new DateColumn(DateUtils.parseDate(constantValue, new String[]{dateformat}));
                break;
            default:
                throw DataXException.asDataXException(Hbase094xReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 常量列不支持您配置的列类型:" + columnType);
        }
        return column;
    }
}
