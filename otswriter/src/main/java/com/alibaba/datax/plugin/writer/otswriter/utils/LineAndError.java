package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;

public class LineAndError {
    private OTSLine line;
    private com.alicloud.openservices.tablestore.model.Error error;

    public LineAndError(OTSLine record, com.alicloud.openservices.tablestore.model.Error error) {
        this.line = record;
        this.error = error;
    }

    public OTSLine getLine() {
        return line;
    }

    public com.alicloud.openservices.tablestore.model.Error getError() {
        return error;
    }
}
