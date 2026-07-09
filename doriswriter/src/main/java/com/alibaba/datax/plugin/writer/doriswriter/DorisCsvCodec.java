package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Record;

public class DorisCsvCodec extends DorisBaseCodec implements DorisCodec {

    private static final long serialVersionUID = 1L;
    private final String columnSeparator;

    public DorisCsvCodec(String sp) {
        this.columnSeparator = DelimiterParser.parse(sp, "\t");
    }

    @Override
    public String codec(Record row) {
        StringBuilder sb = new StringBuilder(Math.max(64, row.getColumnNumber() * 16));
        for (int i = 0; i < row.getColumnNumber(); i++) {
            String value = convertionField(row.getColumn(i));
            if (value == null) {
                sb.append("\\N");
            } else {
                appendEscaped(sb, value);
            }
            if (i < row.getColumnNumber() - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }

    private void appendEscaped(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\\\n");
                    break;
                case '\r':
                    sb.append("\\\\r");
                    break;
                case '\t':
                    sb.append("\\\\t");
                    break;
                default:
                    sb.append(ch);
            }
        }
    }
}
