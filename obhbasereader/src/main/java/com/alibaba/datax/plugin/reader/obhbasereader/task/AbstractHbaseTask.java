package com.alibaba.datax.plugin.reader.obhbasereader.task;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.obhbasereader.Constant;
import com.alibaba.datax.plugin.reader.obhbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.obhbasereader.Key;
import com.alibaba.datax.plugin.reader.obhbasereader.enums.ModeType;
import com.alibaba.datax.plugin.reader.obhbasereader.util.ObHbaseReaderUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractHbaseTask {
    protected String encoding;
    protected String timezone = null;
    protected Map<String, HbaseColumnCell> hbaseColumnCellMap;
    // 常量字段
    protected Map<String, Column> constantMap;
    protected ModeType modeType;

    public AbstractHbaseTask() {
    }

    public AbstractHbaseTask(Configuration configuration) {
        this.timezone = configuration.getString(Key.TIMEZONE, Constant.DEFAULT_TIMEZONE);
        this.encoding = configuration.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        String mode = configuration.getString(Key.MODE, "Normal");
        this.modeType = ModeType.getByTypeName(mode);
        this.constantMap = new HashMap<>();
        this.hbaseColumnCellMap = ObHbaseReaderUtil.parseColumn(configuration.getList(Key.COLUMN, Map.class), constantMap, encoding, timezone);
    }

    public abstract void prepare() throws Exception;

    public abstract boolean fetchLine(Record record) throws Exception;

    public abstract void close() throws IOException;
}
