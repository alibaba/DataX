package com.alibaba.datax.plugin.writer.tdengine20writer;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;

public interface DataHandler {
    int handle(RecordReceiver lineReceiver, TaskPluginCollector collector);
}
