package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDataHandler implements DataHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDataHandler.class);

    @Override
    public long handle(RecordReceiver lineReceiver, Configuration configuration) {
        long count = 0;
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {

            int recordLength = record.getColumnNumber();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < recordLength; i++) {
                Column column = record.getColumn(i);
                sb.append(column.asString()).append("\t");
            }
            sb.setLength(sb.length() - 1);
            LOG.debug(sb.toString());

            count++;
        }
        return count;
    }

}