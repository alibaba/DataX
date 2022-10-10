package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Record;

public class DorisCsvCodec extends DorisBaseCodec implements DorisCodec {

    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    public DorisCsvCodec ( String sp) {
        this.columnSeparator = DelimiterParser.parse(sp, "\t");
    }

    @Override
    public String codec( Record row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getColumnNumber(); i++) {
            String value = convertionField(row.getColumn(i));
            sb.append(null == value ? "\\N" : value);
            if (i < row.getColumnNumber() - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
}
