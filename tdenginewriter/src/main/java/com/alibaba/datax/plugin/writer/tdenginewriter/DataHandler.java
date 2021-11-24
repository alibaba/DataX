package com.alibaba.datax.plugin.writer.tdenginewriter;

import com.alibaba.datax.common.plugin.RecordReceiver;

import com.alibaba.datax.common.plugin.TaskPluginCollector;

import java.util.Properties;

public interface DataHandler {

    long handle(RecordReceiver lineReceiver, Properties properties, TaskPluginCollector collector);
}
