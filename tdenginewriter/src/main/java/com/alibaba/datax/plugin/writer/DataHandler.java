package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.plugin.RecordReceiver;

import java.util.Properties;

public interface DataHandler {

    long handle(RecordReceiver lineReceiver, Properties properties);
}
