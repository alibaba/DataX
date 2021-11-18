package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.plugin.RecordReceiver;

import java.util.Properties;

public interface DataHandler {

    long handle(RecordReceiver lineReceiver, Properties properties);
}
