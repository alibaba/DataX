package com.dorisdb.connector.datax.plugin.writer.doriswriter.row;

import java.io.StringWriter;

import com.alibaba.datax.common.element.Record;

import com.google.common.base.Strings;

public class DorisCsvSerializer extends DorisBaseSerializer implements DorisISerializer {
    
    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    public DorisCsvSerializer(String sp) {
        this.columnSeparator = DorisDelimiterParser.parse(sp, "\t");
    }

    @Override
    public String serialize(Record row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getColumnNumber(); i++) {
            String value = fieldConvertion(row.getColumn(i));
            sb.append(null == value ? "\\N" : value);
            if (i < row.getColumnNumber() - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }

}
