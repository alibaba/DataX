package com.alibaba.datax.plugin.writer;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;

public interface DataHandler {

    long handle(RecordReceiver lineReceiver, Configuration properties);
}
